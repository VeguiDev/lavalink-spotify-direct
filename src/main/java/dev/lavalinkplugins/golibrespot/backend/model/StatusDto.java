package dev.lavalinkplugins.golibrespot.backend.model;

import java.util.Map;

/**
 * The go-librespot v0.9.0 full player status ({@code GET /status}, schema
 * {@code status}; docs/API_CONTRACT.md §2.2). {@code track} is {@code null}
 * when no stream is loaded (idle after stop) and {@code stopped} is derived
 * ({@code !IsPlaying}).
 *
 * <p>All fields nullable-safe: missing fields default to {@code ""}, {@code 0}
 * or {@code false} so unknown/absent JSON fields never throw.</p>
 */
public record StatusDto(
        String username,
        String deviceId,
        String deviceType,
        String deviceName,
        String playOrigin,
        boolean stopped,
        boolean paused,
        boolean buffering,
        long volume,
        long volumeSteps,
        boolean repeatContext,
        boolean repeatTrack,
        boolean shuffleContext,
        TrackDto track) {

    /** Parses from a raw JSON object; {@code null} in, {@code null} out. */
    public static StatusDto fromJson(Map<String, Object> o) {
        if (o == null) {
            return null;
        }
        return new StatusDto(
                Jsons.str(o, "username"),
                Jsons.str(o, "device_id"),
                Jsons.str(o, "device_type"),
                Jsons.str(o, "device_name"),
                Jsons.str(o, "play_origin"),
                Jsons.bool(o, "stopped"),
                Jsons.bool(o, "paused"),
                Jsons.bool(o, "buffering"),
                Jsons.lng(o, "volume"),
                Jsons.lng(o, "volume_steps"),
                Jsons.bool(o, "repeat_context"),
                Jsons.bool(o, "repeat_track"),
                Jsons.bool(o, "shuffle_context"),
                TrackDto.fromJson(Jsons.asObject(o.get("track"))));
    }
}
