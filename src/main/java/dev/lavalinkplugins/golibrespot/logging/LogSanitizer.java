package dev.lavalinkplugins.golibrespot.logging;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Redacts secrets from strings destined for log output.
 *
 * <p>Pure sanitization utility — it never logs anything itself. By default it
 * redacts HTTP {@code Authorization: Bearer <token>} headers (quoted or not,
 * any casing), Spotify credentials ({@code client_id}, {@code client_secret},
 * {@code username}, {@code password}), and query-string secrets
 * ({@code code}, {@code refresh_token}, {@code access_token}, {@code token})
 * in both {@code ?key=value} and {@code &key=value} positions. Additional
 * literal secrets or {@link Pattern}s can be supplied via the constructor and
 * are redacted wherever they appear. Redacted values are replaced with
 * {@code ***} so the original secret never survives into sanitized output.
 * All methods are null-safe.</p>
 *
 * <p>Zero runtime dependencies: {@code java.util.regex} only.</p>
 */
public final class LogSanitizer {

    /** Replacement marker for every redacted secret. */
    private static final String REDACTED = "***";

    /**
     * {@code Authorization: Bearer <token>} and the quoted JSON/header form
     * {@code "Authorization": "Bearer <token>"}. The token is any run of
     * non-space, non-quote characters so base64/URL-safe tokens containing
     * {@code =}, {@code /}, {@code +} are fully covered; trailing quotes stay.
     */
    private static final Pattern BEARER_HEADER = Pattern.compile(
            "(?i)(authorization\\s*[\"']?\\s*[:=]\\s*[\"']?\\s*bearer\\s+)([^\\s\"']+)");

    /**
     * Query-string secrets in both {@code ?key=value} and {@code &key=value}
     * positions. Longest keys first so {@code refresh_token}/{@code access_token}
     * win over {@code token}. Values run to the next {@code &} or {@code #}.
     */
    private static final Pattern QUERY_SECRET = Pattern.compile(
            "(?i)([?&](?:code|refresh_token|access_token|token)=)([^&#]*)");

    /**
     * Spotify credentials in form ({@code key=value}) and JSON
     * ({@code "key": "value"}) styles. Values are single tokens (no
     * whitespace), stopping at quotes/whitespace/separators.
     */
    private static final Pattern CREDENTIAL = Pattern.compile(
            "(?i)((?:client_id|client_secret|username|password)\\s*[\"']?\\s*[:=]\\s*[\"']?)([^\"'\\s&#,;]+)");

    private final List<Rule> rules;

    /**
     * Creates a sanitizer with the built-in bearer/query/credential rules plus
     * the given literal secrets, redacted verbatim (exact match, case
     * sensitive) wherever they appear.
     *
     * @param extraSecrets literal secret strings to redact
     */
    public LogSanitizer(Collection<String> extraSecrets) {
        this(extraSecrets, List.of());
    }

    /**
     * Creates a sanitizer with the built-in rules plus literal secrets and
     * caller-supplied patterns.
     *
     * @param extraSecrets  literal secret strings to redact verbatim
     * @param extraPatterns compiled patterns whose matches are redacted
     */
    public LogSanitizer(Collection<String> extraSecrets, Collection<Pattern> extraPatterns) {
        List<Rule> rules = new ArrayList<>();
        rules.add(new Rule(BEARER_HEADER, "$1" + REDACTED));
        rules.add(new Rule(QUERY_SECRET, "$1" + REDACTED));
        rules.add(new Rule(CREDENTIAL, "$1" + REDACTED));
        for (String secret : extraSecrets) {
            rules.add(new Rule(Pattern.compile(Pattern.quote(secret)), REDACTED));
        }
        for (Pattern pattern : extraPatterns) {
            rules.add(new Rule(pattern, REDACTED));
        }
        this.rules = List.copyOf(rules);
    }

    /**
     * Returns a sanitizer pre-configured with the built-in rules (bearer
     * headers, query-string secrets, Spotify credentials).
     */
    public static LogSanitizer defaults() {
        return new LogSanitizer(List.of(), List.of());
    }

    /**
     * Redacts every recognized secret in {@code input}. {@code null} in,
     * {@code null} out; empty in, empty out.
     */
    public String sanitize(String input) {
        if (input == null) {
            return null;
        }
        String result = input;
        for (Rule rule : rules) {
            result = rule.pattern.matcher(result).replaceAll(rule.replacement);
        }
        return result;
    }

    /**
     * Reduces a URL to its scheme+host+path for path-only logging: the query
     * (and fragment) is stripped entirely, then the result is defensively
     * passed through {@link #sanitize} so a secret that somehow survived in
     * the path is still redacted. {@code null} in, {@code null} out.
     */
    public String sanitizeUrl(String url) {
        if (url == null) {
            return null;
        }
        int end = url.length();
        int query = url.indexOf('?');
        if (query >= 0) {
            end = query;
        }
        int fragment = url.indexOf('#');
        if (fragment >= 0 && fragment < end) {
            end = fragment;
        }
        return sanitize(url.substring(0, end));
    }

    /** One redaction rule: a pattern plus its replacement. */
    private record Rule(Pattern pattern, String replacement) {
    }
}
