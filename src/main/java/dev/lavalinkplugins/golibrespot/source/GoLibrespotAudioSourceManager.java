package dev.lavalinkplugins.golibrespot.source;

import com.sedmelluq.discord.lavaplayer.player.AudioPlayerManager;
import com.sedmelluq.discord.lavaplayer.source.AudioSourceManager;
import com.sedmelluq.discord.lavaplayer.track.AudioItem;
import com.sedmelluq.discord.lavaplayer.track.AudioReference;
import com.sedmelluq.discord.lavaplayer.track.AudioTrack;
import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;
import dev.lavalinkplugins.golibrespot.identifier.TrackIdParser;
import dev.lavalinkplugins.golibrespot.logging.LogSanitizer;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver;
import dev.lavalinkplugins.golibrespot.pool.BackendHandle;
import dev.lavalinkplugins.golibrespot.pool.BackendState;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * The {@code spdirect} source manager — the ONLY source that claims the
 * {@code spdirect:<id>} / {@code spdirect:spotify:track:<id>} identifiers
 * (DECISIONS.md §3). Source name is exactly {@code "spdirect"}.
 *
 * <p><b>Lease at play, never at load.</b> {@link #loadItem} resolves metadata
 * via the T10 {@link MetadataResolver} (READY backends, no lease) and returns
 * a {@link GoLibrespotAudioTrack}. Ordinary Spotify URIs/URLs, albums,
 * playlists and structurally malformed spdirect identifiers are NEVER claimed
 * ({@code null} lets Lavalink fall through to other sources); a metadata
 * failure is likewise a clear {@code null} load — duration/metadata are never
 * fabricated. The pool is only ever consulted at play time
 * ({@link #resolvePlaybackCoordinator}), never during load.</p>
 *
 * <p><b>Shared wiring.</b> The manager holds the {@link ExclusivePool} (backend
 * selection + readiness for play-time coordinator resolution), the
 * {@link MetadataResolver} (load-time metadata) and a {@link CoordinatorFactory}
 * seam producing the per-backend {@link PlaybackCoordinator}. A coordinator is
 * cached per backend id; {@link #shutdown()} closes the factory (the production
 * factory tears down its coordinator + stop-sequence chain).</p>
 *
 * <p><b>No encoded-track format.</b> {@link #isTrackEncodable} is {@code false}
 * and {@link #encodeTrack}/{@link #decodeTrack} throw
 * {@link UnsupportedOperationException} — spdirect tracks are not persistable
 * (the daemon session is not replayable from a byte stream).</p>
 *
 * <p>{@code @Service}: Spring auto-registers this bean; Lavalink registers
 * {@code AudioSourceManager} beans before
 * {@code AudioPlayerManagerConfiguration} beans, which is exactly why this
 * source claims ONLY {@code spdirect:} identifiers and nothing else.</p>
 */
@Service
public class GoLibrespotAudioSourceManager implements AudioSourceManager {

  private static final String SOURCE_NAME = "spdirect";
  private static final String SPOTIFY_TRACK_URI_PREFIX = "spotify:track:";

  private final ExclusivePool pool;
  private final MetadataResolver metadataResolver;
  private final CoordinatorFactory coordinatorFactory;
  private final Map<String, PlaybackCoordinator> coordinators = new ConcurrentHashMap<>();
  private final AtomicInteger roundRobin = new AtomicInteger();
  private final AtomicBoolean shutDown = new AtomicBoolean();
  private final Logger log = LoggerFactory.getLogger(GoLibrespotAudioSourceManager.class);
  private final LogSanitizer sanitizer = LogSanitizer.defaults();

  /**
   * @param pool              the T13 backend pool (never leased during load)
   * @param metadataResolver  the T10 lease-free metadata resolver
   * @param coordinatorFactory per-backend playback coordinator seam
   */
  public GoLibrespotAudioSourceManager(
      ExclusivePool pool, MetadataResolver metadataResolver, CoordinatorFactory coordinatorFactory) {
    this.pool = java.util.Objects.requireNonNull(pool, "pool");
    this.metadataResolver = java.util.Objects.requireNonNull(metadataResolver, "metadataResolver");
    this.coordinatorFactory = java.util.Objects.requireNonNull(coordinatorFactory, "coordinatorFactory");
  }

  @Override
  public String getSourceName() {
    return SOURCE_NAME;
  }

  @Override
  public AudioItem loadItem(AudioPlayerManager manager, AudioReference reference) {
    if (shutDown.get()) {
      log.warn("spdirect load rejected: source manager is shut down");
      return null;
    }
    String identifier = reference == null ? null : reference.identifier;
    if (identifier == null || identifier.isBlank()) {
      return null;
    }
    TrackIdParser.TrackIdParseResult parsed = TrackIdParser.parse(identifier);
    if (!(parsed instanceof TrackIdParser.TrackIdParseResult.TrackId trackId)) {
      // NotClaimed (ordinary Spotify URIs/URLs, albums, playlists) or Malformed —
      // never claimed, never an error: null lets Lavalink fall through.
      return null;
    }
    Optional<AudioTrackInfo> resolved = metadataResolver.resolve(trackId.id());
    if (resolved.isEmpty()) {
      log.warn("spdirect load failed for track '{}': metadata unavailable (no lease taken, "
          + "no duration fabricated)", sanitizer.sanitize(trackId.id()));
      return null;
    }
    return new GoLibrespotAudioTrack(trackId.id(), resolved.get(), this);
  }

  @Override
  public boolean isTrackEncodable(AudioTrack track) {
    return false;
  }

  @Override
  public void encodeTrack(AudioTrack track, DataOutput output) throws IOException {
    throw new UnsupportedOperationException("spdirect tracks are not encodable");
  }

  @Override
  public AudioTrack decodeTrack(AudioTrackInfo trackInfo, DataInput input) throws IOException {
    throw new UnsupportedOperationException("spdirect tracks are not encodable");
  }

  /**
   * Shuts the source down: closes the coordinator factory (the production
   * factory tears down its coordinator + stop-sequence chain). Idempotent.
   */
  @Override
  public void shutdown() {
    if (shutDown.compareAndSet(false, true)) {
      log.info("spdirect source manager shutting down: closing coordinator factory");
      try {
        coordinatorFactory.close();
      } catch (RuntimeException e) {
        log.warn("coordinator factory close failed: {}",
            sanitizer.sanitize(String.valueOf(e.getMessage())));
      }
    }
  }

  // ------------------------------------------------------------ playback wiring

  /**
   * Resolves the {@link PlaybackCoordinator} a track (and the bridge) drive for
   * playback: round-robins over the pool's READY backends (falling back to the
   * first backend when none is ready — the coordinator's {@code start()} then
   * waits bounded on its own exclusive lease). One coordinator is cached per
   * backend id via the factory. Never leases here — leasing happens inside the
   * coordinator's {@code start()}.
   */
  public PlaybackCoordinator resolvePlaybackCoordinator() {
    List<BackendHandle> handles = pool.handles();
    if (handles.isEmpty()) {
      throw new IllegalStateException("no go-librespot backends configured; cannot play spdirect tracks");
    }
    BackendHandle selected = null;
    int n = handles.size();
    int start = Math.floorMod(roundRobin.getAndIncrement(), n);
    for (int i = 0; i < n; i++) {
      BackendHandle candidate = handles.get((start + i) % n);
      if (pool.stateOf(candidate.getBackendId()) == BackendState.READY) {
        selected = candidate;
        break;
      }
    }
    if (selected == null) {
      selected = handles.get(0);
    }
    final BackendHandle chosen = selected;
    return coordinators.computeIfAbsent(
        chosen.getBackendId(), id -> {
          CoordinatorFactory.requireArgs(chosen, chosen.getConfig());
          return coordinatorFactory.create(chosen, chosen.getConfig());
        });
  }

  /** The URI sent to the daemon for a bare spdirect track id. */
  static String daemonUriFor(String trackId) {
    return SPOTIFY_TRACK_URI_PREFIX + trackId;
  }
}
