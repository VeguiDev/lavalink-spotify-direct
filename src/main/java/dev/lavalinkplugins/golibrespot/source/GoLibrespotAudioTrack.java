package dev.lavalinkplugins.golibrespot.source;

import com.sedmelluq.discord.lavaplayer.filter.PcmFormat;
import com.sedmelluq.discord.lavaplayer.source.AudioSourceManager;
import com.sedmelluq.discord.lavaplayer.tools.FriendlyException;
import com.sedmelluq.discord.lavaplayer.tools.FriendlyException.Severity;
import com.sedmelluq.discord.lavaplayer.track.AudioTrack;
import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;
import com.sedmelluq.discord.lavaplayer.track.BaseAudioTrack;
import com.sedmelluq.discord.lavaplayer.track.playback.AudioProcessingContext;
import com.sedmelluq.discord.lavaplayer.track.playback.LocalAudioTrackExecutor;
import dev.lavalinkplugins.golibrespot.lifecycle.ActivationException;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

/**
 * The spdirect track (T18): plays decoded daemon PCM through the Lavaplayer
 * pipeline, coordinated by a {@link PlaybackCoordinator}.
 *
 * <p><b>process()/barrier contract.</b> {@link #process} runs
 * {@code executor.executeProcessingLoop(readExecutor, seekExecutor)}. The read
 * executor first blocks bounded on {@code coordinator.awaitActivated} — throwing
 * the typed {@link ActivationException} to FAIL the track (never returning while
 * the barrier is pending, which would prematurely end the track) — then loops
 * {@code coordinator.nextFrame(50ms)}: a non-empty array feeds the pipeline, an
 * empty array is a transient no-data call (loop on), {@code null} is
 * end-of-stream (stop feeding; Lavaplayer fires TrackEnd FINISHED once the frame
 * buffer drains). An {@link InterruptedException} (seek interrupt / executor
 * stop) is re-raised and handled as a natural stop. The pipeline is always
 * flushed + closed in {@code finally}.</p>
 *
 * <p><b>Seek.</b> The seek executor maps to {@code coordinator.seek(positionMs)}
 * — the T16 handshake — and calls {@code pipeline.seekPerformed()} after it
 * returns OK (the pipeline clears its filter state at the next frame; the JVM
 * frame buffer is cleared by Lavaplayer's seek ghosting). A non-OK result
 * throws a {@link FriendlyException} so the track FAILS — never a silent
 * mis-seek. Position is daemon-authoritative: this track maintains no timecode
 * of its own (the executor's default {@code getPosition()} is acceptable).</p>
 *
 * <p><b>Identity.</b> {@link #makeShallowClone()} returns a fresh track over the
 * same id/info/source manager; {@link #getSourceManager()} returns the manager
 * (the fork's {@code BaseAudioTrack} does NOT store one — each source's track
 * owns it, see {@code YoutubeAudioTrack} for the reference pattern).</p>
 */
public final class GoLibrespotAudioTrack extends BaseAudioTrack {

  /** Activation barrier budget (DECISIONS.md). */
  private static final Duration ACTIVATION_TIMEOUT =
      Duration.ofMillis(BackendStateMachine.Timing.defaults().activationTimeoutMs());
  /** nextFrame poll budget — return promptly so seeks/stops stay responsive. */
  private static final Duration FRAME_TIMEOUT = Duration.ofMillis(50);
  private static final int CHANNEL_COUNT = 2;
  private static final int SAMPLE_RATE = 44100;

  private final String trackId;
  private final BiFunction<AudioProcessingContext, PcmFormat, PcmPipeline> pipelineFactory;
  private volatile GoLibrespotAudioSourceManager sourceManager;
  private final AtomicReference<PlaybackCoordinator> playbackCoordinator = new AtomicReference<>();

  /**
   * @param trackId       the bare Spotify track id (22-char base62)
   * @param trackInfo     metadata mapped by {@code AudioTrackInfoMapper}
   * @param sourceManager the owning source manager (carries pool + metadata + factory)
   */
  public GoLibrespotAudioTrack(
      String trackId, AudioTrackInfo trackInfo, GoLibrespotAudioSourceManager sourceManager) {
    this(trackId, trackInfo, sourceManager, PcmPipeline.defaultFactory());
  }

  /** Test seam: inject a recording pipeline factory. */
  GoLibrespotAudioTrack(
      String trackId,
      AudioTrackInfo trackInfo,
      GoLibrespotAudioSourceManager sourceManager,
      BiFunction<AudioProcessingContext, PcmFormat, PcmPipeline> pipelineFactory) {
    super(trackInfo);
    this.trackId = Objects.requireNonNull(trackId, "trackId");
    this.sourceManager = Objects.requireNonNull(sourceManager, "sourceManager");
    this.pipelineFactory = Objects.requireNonNull(pipelineFactory, "pipelineFactory");
  }

  /** The bare Spotify track id. */
  public String trackId() {
    return trackId;
  }

  /** The daemon-side URI for this track. */
  public String daemonUri() {
    return GoLibrespotAudioSourceManager.daemonUriFor(trackId);
  }

  @Override
  public void process(LocalAudioTrackExecutor executor) throws Exception {
    PlaybackCoordinator coordinator = playbackCoordinator();
    PcmPipeline pipeline = pipelineFactory.apply(executor.getProcessingContext(),
        new PcmFormat(CHANNEL_COUNT, SAMPLE_RATE));
    try {
      executor.executeProcessingLoop(
          () -> performRead(coordinator, pipeline),
          (positionMs) -> performSeek(coordinator, positionMs, pipeline));
    } finally {
      try {
        pipeline.flush();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      pipeline.close();
    }
  }

  /** Read path: bounded activation await, then feed decoded frames until EOS. */
  private void performRead(PlaybackCoordinator coordinator, PcmPipeline pipeline) throws Exception {
    try {
      coordinator.awaitActivated(ACTIVATION_TIMEOUT);
    } catch (InterruptedException e) {
      // seek interrupt / executor stop — let the loop treat it as a natural stop
      Thread.currentThread().interrupt();
      return;
    }
    // executeProcessingLoop invokes its read executor once. The source is
    // therefore responsible for keeping that invocation alive until a real
    // daemon completion is observed; returning after one FIFO chunk makes
    // Lavaplayer treat a few milliseconds of PCM as the complete track.
    while (!Thread.currentThread().isInterrupted()) {
      short[] frames;
      try {
        frames = coordinator.nextFrame(FRAME_TIMEOUT);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
      if (frames == null) {
        return; // confirmed end-of-stream (current-generation not_playing)
      }
      if (frames.length > 0) {
        pipeline.process(frames, 0, frames.length);
      }
    }
  }

  /** Seek path: the T16 handshake; fails the track on a non-OK result. */
  private void performSeek(PlaybackCoordinator coordinator, long positionMs, PcmPipeline pipeline)
      throws Exception {
    BackendStateMachine.Result result = coordinator.seek(positionMs);
    if (!result.isOk()) {
      throw new FriendlyException(
          "spdirect seek to " + positionMs + "ms failed: " + result.reason(),
          Severity.COMMON, null);
    }
    pipeline.seekPerformed(positionMs, positionMs);
  }

  @Override
  public boolean isSeekable() {
    return true;
  }

  @Override
  public AudioSourceManager getSourceManager() {
    return sourceManager;
  }

  /** Re-binds the owning source manager (mirrors the fork's track API). */
  public void setSourceManager(GoLibrespotAudioSourceManager manager) {
    this.sourceManager = Objects.requireNonNull(manager, "manager");
  }

  @Override
  protected AudioTrack makeShallowClone() {
    return new GoLibrespotAudioTrack(trackId, trackInfo, sourceManager);
  }

  // ------------------------------------------------------------ coordinator wiring

  /**
   * The {@link PlaybackCoordinator} driving this track — resolved lazily (once,
   * thread-safe) from the source manager, or set by the player bridge to REUSE
   * the coordinator of the replaced track (play-over-play on the held lease).
   */
  PlaybackCoordinator playbackCoordinator() {
    PlaybackCoordinator existing = playbackCoordinator.get();
    if (existing != null) {
      return existing;
    }
    PlaybackCoordinator resolved = sourceManager.resolvePlaybackCoordinator();
    if (playbackCoordinator.compareAndSet(null, resolved)) {
      return resolved;
    }
    return playbackCoordinator.get();
  }

  /** The player bridge pins the replacing track to the replaced track's coordinator. */
  void setPlaybackCoordinator(PlaybackCoordinator coordinator) {
    playbackCoordinator.set(Objects.requireNonNull(coordinator, "coordinator"));
  }
}
