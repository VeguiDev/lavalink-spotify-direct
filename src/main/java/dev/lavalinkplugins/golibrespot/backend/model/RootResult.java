package dev.lavalinkplugins.golibrespot.backend.model;

/**
 * Result of {@code GET /} (liveness + readiness probe): raw status + body plus
 * the parsed {@code playback_ready} flag.
 *
 * <p>Conservative semantics: readiness is {@code false} unless the daemon
 * answered 200 with a parseable {@code {"playback_ready": true}} — a failure
 * or unparseable body is never treated as ready.</p>
 */
public record RootResult(int status, String body, boolean playbackReady) {

    /** Best-effort parse of {@code playback_ready} from a 200 body; unparseable → not ready. */
    public static RootResult of(int status, String body) {
        boolean ready = false;
        if (status == 200 && body != null && !body.isBlank()) {
            try {
                ready = RootDto.fromJson(Jsons.object(body)).playbackReady();
            } catch (RuntimeException ignored) {
                ready = false; // never infer readiness from an unparseable body
            }
        }
        return new RootResult(status, body == null ? "" : body, ready);
    }

    /** {@code true} only when the daemon reported {@code playback_ready: true}. */
    public boolean isReady() {
        return playbackReady;
    }

    /** {@code 200..299}. */
    public boolean is2xx() {
        return status >= 200 && status < 300;
    }
}
