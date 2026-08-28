package dev.lavalinkplugins.golibrespot.backend.ws;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One parsed frame from the daemon's {@code /events} WebSocket:
 * {@code {"type": "<event_type>", "data": { ... }}}.
 *
 * <p>Parsing is tolerant by design ({@code docs/API_CONTRACT.md} §3): the
 * {@code type} is mapped through {@link EventType#fromWire(String)} (unknown
 * values become {@link EventType#UNKNOWN}) and the {@code data} object keeps
 * <b>every</b> field raw — including fields this build does not know about —
 * as plain {@link String}/{@link Long}/{@link Double}/{@link Boolean}/{@code null}/
 * {@link java.util.List}/{@link java.util.Map} values. Events without data
 * (e.g. {@code playback_ready}, {@code active}, {@code inactive}) carry a
 * {@code null} data map.</p>
 *
 * @param type the event type ({@link EventType#UNKNOWN} for unrecognized types)
 * @param data raw field map, or {@code null} when the frame had no data object
 */
public record PlayerEvent(EventType type, Map<String, Object> data) {

    public PlayerEvent {
        Objects.requireNonNull(type, "type must not be null");
        if (data != null) {
            // defensive copy: callers must not mutate dispatched events, and the
            // parse result must not be affected by caller mutation of the map
            data = Collections.unmodifiableMap(new HashMap<>(data));
        }
    }
}
