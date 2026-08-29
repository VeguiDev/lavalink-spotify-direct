package dev.lavalinkplugins.golibrespot.source;

import com.sedmelluq.discord.lavaplayer.player.AudioPlayerManager;
import com.sedmelluq.discord.lavaplayer.source.AudioSourceManager;
import com.sedmelluq.discord.lavaplayer.track.AudioItem;
import com.sedmelluq.discord.lavaplayer.track.AudioReference;
import com.sedmelluq.discord.lavaplayer.track.AudioTrack;
import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;
import com.sedmelluq.discord.lavaplayer.track.BasicAudioPlaylist;
import dev.lavalinkplugins.golibrespot.identifier.TrackIdParser;
import dev.lavalinkplugins.golibrespot.logging.LogSanitizer;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver;
import dev.lavalinkplugins.golibrespot.pool.BackendHandle;
import dev.lavalinkplugins.golibrespot.pool.BackendState;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
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
 * The {@code spotify} source manager. It claims supported Spotify track and
 * collection identifiers while preserving {@code spdirect:} as the encoded
 * track identifier namespace. Its source name is exactly {@code "spotify"}.
 *
 * <p><b>Lease at play, never at load.</b> {@link #loadItem} resolves metadata
 * via the T10 {@link MetadataResolver} (READY backends, no lease) and returns
 * a {@link GoLibrespotAudioTrack} or collection playlist. Supported albums and
 * playlists are claimed and surfaced as {@link BasicAudioPlaylist} instances;
 * unrecognized or structurally malformed identifiers return {@code null} so
 * Lavalink can fall through. A metadata failure is likewise a clear
 * {@code null} load — duration/metadata are never fabricated. The pool is only
 * ever consulted at play time
 * ({@link #resolvePlaybackCoordinator}), never during load.</p>
 *
 * <p><b>Shared wiring.</b> The manager holds the {@link ExclusivePool} (backend
 * selection + readiness for play-time coordinator resolution), the
 * {@link MetadataResolver} (load-time metadata) and a {@link CoordinatorFactory}
 * seam producing the per-backend {@link PlaybackCoordinator}. A coordinator is
 * cached per backend id; {@link #shutdown()} closes the factory (the production
 * factory tears down its coordinator + stop-sequence chain).</p>
 *
 * <p><b>Encoded-track format.</b> {@link #isTrackEncodable} accepts
 * {@link GoLibrespotAudioTrack} instances. {@link #encodeTrack} persists the
 * bare Spotify track id and {@link #decodeTrack} restores the wrapping track;
 * playback still obtains a fresh daemon session at play time.</p>
 *
 * <p>{@code @Service}: Spring auto-registers this bean; Lavalink registers
 * {@code AudioSourceManager} beans before
 * {@code AudioPlayerManagerConfiguration} beans, which is exactly why this
 * source participates in identifier claiming under the {@code spotify} source
 * name.</p>
 */
@Service
public class GoLibrespotAudioSourceManager implements AudioSourceManager {

  private static final String SOURCE_NAME = "spotify";
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
    if (parsed instanceof TrackIdParser.TrackIdParseResult.CollectionId collection) {
      return metadataResolver.resolveCollection(collection.kind(), collection.id())
          .map(this::buildCollectionPlaylist)
          .orElse(null);
    }
    if (!(parsed instanceof TrackIdParser.TrackIdParseResult.TrackId trackId)) {
      return null;
    }
    Optional<AudioTrackInfo> info = metadataResolver.resolve(trackId.id());
    if (info.isEmpty()) {
      log.warn("Spotify metadata unavailable for '{}'; refusing an invalid-duration track",
          sanitizer.sanitize(trackId.id()));
      return null;
    }
    return new GoLibrespotAudioTrack(trackId.id(), info.get(), this);
  }

  private BasicAudioPlaylist buildCollectionPlaylist(MetadataResolver.CollectionMetadata metadata) {
    List<AudioTrack> tracks = new ArrayList<>(metadata.tracks().size());
    for (int i = 0; i < metadata.tracks().size(); i++) {
      AudioTrackInfo info = metadata.tracks().get(i);
      if (i == 0 && metadata.artworkUrl() != null && !metadata.artworkUrl().isBlank()) {
        info = new AudioTrackInfo(
            info.title,
            info.author,
            info.length,
            info.identifier,
            info.isStream,
            info.uri,
            metadata.artworkUrl(),
            info.isrc);
      }
      tracks.add(buildCollectionTrack(info));
    }
    String name = metadata.author() != null && !metadata.author().isBlank()
        ? metadata.name() + " · " + metadata.author()
        : metadata.name();
    return new BasicAudioPlaylist(name, List.copyOf(tracks), null, false);
  }

  private AudioTrack buildCollectionTrack(AudioTrackInfo info) {
    return new GoLibrespotAudioTrack(
        info.identifier.substring("spdirect:".length()), info, this);
  }

  @Override
  public boolean isTrackEncodable(AudioTrack track) {
    return track instanceof GoLibrespotAudioTrack;
  }

  @Override
  public void encodeTrack(AudioTrack track, DataOutput output) throws IOException {
    if (!(track instanceof GoLibrespotAudioTrack direct)) {
      throw new IllegalArgumentException("track is not a spdirect track");
    }
    output.writeUTF(direct.trackId());
  }

  @Override
  public AudioTrack decodeTrack(AudioTrackInfo trackInfo, DataInput input) throws IOException {
    return new GoLibrespotAudioTrack(input.readUTF(), trackInfo, this);
  }

  /**
   * Shuts the source down: closes the coordinator factory (the production
   * factory tears down its coordinator + stop-sequence chain). Idempotent.
   */
  @Override
  public void shutdown() {
    if (shutDown.compareAndSet(false, true)) {
      log.info("spotify source manager shutting down: closing coordinator factory");
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
