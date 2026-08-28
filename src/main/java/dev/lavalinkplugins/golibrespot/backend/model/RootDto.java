package dev.lavalinkplugins.golibrespot.backend.model;

import java.util.Map;

/**
 * The go-librespot v0.9.0 root body ({@code GET /}, docs/API_CONTRACT.md §2.1):
 * {@code playback_ready} — the daemon is fully bootstrapped (Spotify connection
 * id + initial connect state + country code) and ready to accept
 * {@code /player/play}.
 */
public record RootDto(boolean playbackReady) {

    /** Parses from a raw JSON object; {@code false} for null/missing/unknown input. */
    public static RootDto fromJson(Map<String, Object> o) {
        return new RootDto(o != null && Jsons.bool(o, "playback_ready"));
    }
}
