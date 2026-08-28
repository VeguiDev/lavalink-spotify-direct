package dev.lavalinkplugins.golibrespot.source;

import com.sedmelluq.discord.lavaplayer.player.AudioPlayer;
import com.sedmelluq.discord.lavaplayer.player.event.AudioEventAdapter;
import com.sedmelluq.discord.lavaplayer.tools.FriendlyException;
import com.sedmelluq.discord.lavaplayer.track.AudioTrack;
import com.sedmelluq.discord.lavaplayer.track.AudioTrackEndReason;
import dev.arbjerg.lavalink.api.IPlayer;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Result;
import dev.lavalinkplugins.golibrespot.logging.LogSanitizer;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The T18 player lifecycle bridge: one {@link AudioEventAdapter} per Lavalink
 * player, attached by the T19 plugin wiring in
 * {@code PluginEventHandler.onNewPlayer} (via {@link #attach(IPlayer)}) and
 * removed in {@code onDestroyPlayer} (via {@link #detach(IPlayer)}).
 *
 * <p>It routes ONLY the events whose {@link AudioTrack} is a
 * {@link GoLibrespotAudioTrack} — and, for pause/resume/end/exception, only
 * while that track's coordinator owns an ACTIVE session — so LavaSrc, YouTube
 * and every other source's tracks are completely untouched.</p>
 *
 * <p>Mappings (DECISIONS.md):</p>
 * <ul>
 *   <li>{@code onTrackStart} → {@code coordinator.start(uri, position)} async;</li>
 *   <li>{@code onPlayerPause} / {@code onPlayerResume} → remote pause / resume
 *       via the machine (bounded futures, never awaited on the player thread);</li>
 *   <li>{@code onTrackEnd} {@code FINISHED} → nothing (natural completion already
 *       released via {@code not_playing}); {@code STOPPED} →
 *       {@code StopSequence.logicalStop()}; {@code REPLACED} → play-over-play
 *       {@code coordinator.replace(newUri, newPosition)} on the held lease and
 *       pin the new spdirect track to the same coordinator; {@code CLEANUP} →
 *       {@code StopSequence.destroy()};</li>
 *   <li>{@code onTrackException} / {@code onTrackStuck} → quarantine the backend
 *       via the machine (bounded, async — the machine owns the lease release).</li>
 * </ul>
 *
 * <p>Every callback runs on Lavaplayer's dispatch thread and MUST NOT block on
 * a daemon round-trip — all routing submits to the coordinator / stop-sequence
 * lanes (single-threaded, bounded) and logs the outcome on the future. All log
 * lines pass through {@link LogSanitizer}. The daemon stop endpoint is never
 * issued (v0.9.0 stop-race).</p>
 */
public final class PlayerLifecycleBridge extends AudioEventAdapter {

  private final Logger log = LoggerFactory.getLogger(PlayerLifecycleBridge.class);
  private final LogSanitizer sanitizer = LogSanitizer.defaults();

  /** Attaches this bridge to a Lavalink player (T19 wiring: {@code onNewPlayer}). */
  public void attach(IPlayer player) {
    Objects.requireNonNull(player, "player");
    player.getAudioPlayer().addListener(this);
  }

  /** Detaches this bridge from a Lavalink player (T19 wiring: {@code onDestroyPlayer}). */
  public void detach(IPlayer player) {
    Objects.requireNonNull(player, "player");
    player.getAudioPlayer().removeListener(this);
  }

  @Override
  public void onTrackStart(AudioPlayer player, AudioTrack track) {
    GoLibrespotAudioTrack spdirect = spdirect(track);
    if (spdirect == null) {
      return;
    }
    PlaybackCoordinator coordinator = coordinatorOf(spdirect);
    if (coordinator == null) {
      return;
    }
    long position = Math.max(0, track.getPosition());
    log.debug("spdirect track start '{}' at {}ms", spdirect.trackId(), position);
    coordinator.start(spdirect.daemonUri(), position)
        .whenComplete((result, error) -> logCompletion("start", spdirect.trackId(), result, error));
  }

  @Override
  public void onPlayerPause(AudioPlayer player) {
    GoLibrespotAudioTrack spdirect = spdirect(player.getPlayingTrack());
    if (spdirect == null) {
      return;
    }
    PlaybackCoordinator coordinator = activeCoordinator(spdirect);
    if (coordinator == null) {
      return;
    }
    coordinator.pauseRemote()
        .whenComplete((result, error) -> logCompletion("pause", spdirect.trackId(), result, error));
  }

  @Override
  public void onPlayerResume(AudioPlayer player) {
    GoLibrespotAudioTrack spdirect = spdirect(player.getPlayingTrack());
    if (spdirect == null) {
      return;
    }
    PlaybackCoordinator coordinator = activeCoordinator(spdirect);
    if (coordinator == null) {
      return;
    }
    coordinator.resumeRemote()
        .whenComplete((result, error) -> logCompletion("resume", spdirect.trackId(), result, error));
  }

  @Override
  public void onTrackEnd(AudioPlayer player, AudioTrack track, AudioTrackEndReason endReason) {
    GoLibrespotAudioTrack spdirect = spdirect(track);
    if (spdirect == null) {
      return;
    }
    PlaybackCoordinator coordinator = coordinatorOf(spdirect);
    if (coordinator == null) {
      return;
    }
    switch (endReason) {
      case FINISHED -> {
        // Natural completion: the machine already released the lease via a
        // matching not_playing and the coordinator cleared itself — nothing to do.
      }
      case STOPPED -> {
        if (!coordinator.isActive()) {
          return;
        }
        coordinator.logicalStop()
            .whenComplete((result, error) -> logCompletion("logicalStop", spdirect.trackId(), result, error));
      }
      case REPLACED -> {
        if (!coordinator.isActive()) {
          return;
        }
        AudioTrack next = player.getPlayingTrack();
        GoLibrespotAudioTrack nextSpdirect = spdirect(next);
        if (nextSpdirect != null) {
          // Play-over-play on the HELD lease: reuse the same coordinator (and pin
          // the new track to it so process() reads the replaced session), no stop.
          nextSpdirect.setPlaybackCoordinator(coordinator);
          long nextPosition = Math.max(0, next.getPosition());
          log.debug("spdirect replace '{}' with '{}' at {}ms",
              spdirect.trackId(), nextSpdirect.trackId(), nextPosition);
          coordinator.replace(nextSpdirect.daemonUri(), nextPosition)
              .whenComplete((result, error) ->
                  logCompletion("replace", nextSpdirect.trackId(), result, error));
        } else {
          // Replaced by a foreign track — retire the session gracefully.
          coordinator.logicalStop()
              .whenComplete((result, error) -> logCompletion("logicalStop", spdirect.trackId(), result, error));
        }
      }
      case CLEANUP -> coordinator.destroy()
          .whenComplete((result, error) -> logCompletion("destroy", spdirect.trackId(), result, error));
      default -> {
        // LOAD_FAILED etc. — the session ends through the paths above
      }
    }
  }

  @Override
  public void onTrackException(AudioPlayer player, AudioTrack track, FriendlyException exception) {
    quarantine(track, "exception: " + (exception == null ? "null" : exception.getMessage()));
  }

  @Override
  public void onTrackStuck(AudioPlayer player, AudioTrack track, long thresholdMs) {
    quarantine(track, "stuck for " + thresholdMs + "ms");
  }

  private void quarantine(AudioTrack track, String reason) {
    GoLibrespotAudioTrack spdirect = spdirect(track);
    if (spdirect == null) {
      return;
    }
    PlaybackCoordinator coordinator = activeCoordinator(spdirect);
    if (coordinator == null) {
      return;
    }
    log.warn("spdirect track '{}' quarantining backend: {}", spdirect.trackId(),
        sanitizer.sanitize(reason));
    coordinator.quarantine(reason)
        .whenComplete((result, error) -> logCompletion("quarantine", spdirect.trackId(), result, error));
  }

  // ------------------------------------------------------------ helpers

  private static GoLibrespotAudioTrack spdirect(AudioTrack track) {
    return track instanceof GoLibrespotAudioTrack spdirect ? spdirect : null;
  }

  /** Resolves the track's coordinator defensively (no backends → null, never throws). */
  private PlaybackCoordinator coordinatorOf(GoLibrespotAudioTrack track) {
    try {
      return track.playbackCoordinator();
    } catch (RuntimeException e) {
      log.warn("spdirect coordinator resolution failed: {}",
          sanitizer.sanitize(String.valueOf(e.getMessage())));
      return null;
    }
  }

  /** The coordinator only when it owns an active session for this plugin instance. */
  private PlaybackCoordinator activeCoordinator(GoLibrespotAudioTrack track) {
    PlaybackCoordinator coordinator = coordinatorOf(track);
    return coordinator != null && coordinator.isActive() ? coordinator : null;
  }

  private void logCompletion(String op, String trackId, Result result, Throwable error) {
    if (error != null) {
      log.warn("spdirect {} for track '{}' failed: {}", op, trackId,
          sanitizer.sanitize(String.valueOf(error)));
    } else if (result != null && !result.isOk()) {
      log.warn("spdirect {} for track '{}' returned {}: {}", op, trackId, result.outcome(),
          sanitizer.sanitize(String.valueOf(result.reason())));
    } else {
      log.debug("spdirect {} for track '{}' completed", op, trackId);
    }
  }
}
