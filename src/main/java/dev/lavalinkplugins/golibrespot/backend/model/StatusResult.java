package dev.lavalinkplugins.golibrespot.backend.model;

import java.util.Optional;

/**
 * Result of {@code GET /status}: raw status + body plus a best-effort parsed
 * {@link StatusDto}.
 *
 * <p>{@code parsed()} is empty when the daemon answered 204 (no session) or
 * when a 200 body could not be parsed (forward-compat: the daemon may add
 * fields, but a malformed body is never guessed at). The raw status + body are
 * always exposed — callers reconcile, never assume.</p>
 */
public record StatusResult(int status, String body, StatusDto statusDto) {

    /**
     * Builds a result from the raw response; parses {@code status == 200} with
     * a non-blank body, swallowing parse failures (unparseable → empty parsed,
     * the caller decides how to treat it).
     */
    public static StatusResult of(int status, String body) {
        StatusDto dto = null;
        if (status == 200 && body != null && !body.isBlank()) {
            try {
                dto = StatusDto.fromJson(Jsons.object(body));
            } catch (RuntimeException ignored) {
                dto = null; // never infer from an unparseable body
            }
        }
        return new StatusResult(status, body == null ? "" : body, dto);
    }

    /** {@code 204} — no active session. */
    public boolean isNoSession() {
        return status == 204;
    }

    /** {@code 200..299}. */
    public boolean is2xx() {
        return status >= 200 && status < 300;
    }

    /** The parsed status when a 200 body parsed successfully. */
    public Optional<StatusDto> parsed() {
        return Optional.ofNullable(statusDto);
    }
}
