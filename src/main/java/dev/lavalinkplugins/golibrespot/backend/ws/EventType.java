package dev.lavalinkplugins.golibrespot.backend.ws;

/**
 * The event types emitted by the go-librespot v0.9.0 daemon's {@code /events}
 * WebSocket (see {@code docs/API_CONTRACT.md} §3).
 *
 * <p>{@link #UNKNOWN} is the tolerant fallback for any {@code type} value the
 * daemon may send that this build does not know about (forward compatibility):
 * such frames are parsed normally but dispatched to the client's debug-only
 * unknown-event sink instead of the typed path.</p>
 */
public enum EventType {
    PLAYBACK_READY("playback_ready"),
    ACTIVE("active"),
    INACTIVE("inactive"),
    METADATA("metadata"),
    WILL_PLAY("will_play"),
    PLAYING("playing"),
    NOT_PLAYING("not_playing"),
    PAUSED("paused"),
    STOPPED("stopped"),
    SEEK("seek"),
    VOLUME("volume"),
    REPEAT_TRACK("repeat_track"),
    REPEAT_CONTEXT("repeat_context"),
    SHUFFLE_CONTEXT("shuffle_context"),
    UNKNOWN("unknown");

    private final String wireName;

    EventType(String wireName) {
        this.wireName = wireName;
    }

    /** The exact {@code type} string used on the wire. */
    public String wireName() {
        return wireName;
    }

    /**
     * Maps a wire {@code type} string to an {@link EventType}. Case-sensitive
     * (the daemon always emits lowercase); unknown values map to
     * {@link #UNKNOWN} — never to {@code null} and never an exception.
     */
    public static EventType fromWire(String wireName) {
        for (EventType type : values()) {
            if (type.wireName.equals(wireName)) {
                return type;
            }
        }
        return UNKNOWN;
    }
}
