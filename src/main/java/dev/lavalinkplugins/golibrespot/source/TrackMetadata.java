package dev.lavalinkplugins.golibrespot.source;

import java.util.List;

/**
 * Immutable go-librespot track metadata — the seam between the daemon's
 * {@code /web-api} response (and/or WS {@code metadata} event fields: name,
 * artist_names[], album_name, duration, album_cover_url, external ISRC) and a
 * Lavaplayer {@code AudioTrackInfo} (see {@link AudioTrackInfoMapper}).
 *
 * <p>Plain record: no behaviour. All components are non-null except
 * {@link #artworkUrl()} and {@link #isrc()} which are optional (may be
 * {@code null}). {@link #durationMs()} must be positive for a successful
 * mapping.</p>
 *
 * @param id         Spotify 22-char base62 track id (used for the
 *                   {@code spdirect:&lt;id&gt;} identifier)
 * @param title      track name
 * @param artists    artist names, in order (may be empty)
 * @param album      album name
 * @param durationMs track duration in milliseconds (&gt; 0 required)
 * @param artworkUrl optional cover-art URL, may be {@code null}
 * @param isrc       optional external ISRC code, may be {@code null}
 */
public record TrackMetadata(
        String id,
        String title,
        List<String> artists,
        String album,
        long durationMs,
        String artworkUrl,
        String isrc) {
}
