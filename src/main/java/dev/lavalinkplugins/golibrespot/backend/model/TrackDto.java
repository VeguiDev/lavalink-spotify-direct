package dev.lavalinkplugins.golibrespot.backend.model;

import java.util.List;
import java.util.Map;

/**
 * The go-librespot v0.9.0 {@code track} schema (docs/API_CONTRACT.md §2.2) —
 * also the {@code metadata} WS event data shape.
 *
 * <p>All fields nullable-safe: missing fields default to {@code ""}, {@code 0}
 * or {@code null} so unknown/absent JSON fields never throw (forward-compat
 * rule). Nullable fields per contract: {@code album_cover_url}, {@code bitrate},
 * {@code sample_rate}, {@code bit_depth}.</p>
 */
public record TrackDto(
        String uri,
        String name,
        List<String> artistNames,
        String albumName,
        String albumCoverUrl,
        long position,
        long duration,
        String releaseDate,
        int trackNumber,
        int discNumber,
        String format,
        String codec,
        Integer bitrate,
        Integer sampleRate,
        Integer bitDepth) {

    public TrackDto {
        artistNames = artistNames == null ? List.of() : List.copyOf(artistNames);
    }

    /** Parses from a raw JSON object; {@code null} in, {@code null} out. */
    public static TrackDto fromJson(Map<String, Object> o) {
        if (o == null) {
            return null;
        }
        return new TrackDto(
                Jsons.str(o, "uri"),
                Jsons.str(o, "name"),
                Jsons.strings(o, "artist_names"),
                Jsons.str(o, "album_name"),
                Jsons.nullableStr(o, "album_cover_url"),
                Jsons.lng(o, "position"),
                Jsons.lng(o, "duration"),
                Jsons.str(o, "release_date"),
                Jsons.integer(o, "track_number"),
                Jsons.integer(o, "disc_number"),
                Jsons.str(o, "format"),
                Jsons.str(o, "codec"),
                Jsons.nullableInt(o, "bitrate"),
                Jsons.nullableInt(o, "sample_rate"),
                Jsons.nullableInt(o, "bit_depth"));
    }
}
