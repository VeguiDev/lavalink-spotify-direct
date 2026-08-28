package dev.lavalinkplugins.golibrespot.logging;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * TDD spec for {@link LogSanitizer}.
 *
 * <p>Redaction invariants under test: bearer tokens in Authorization headers
 * (quoted/unquoted, any casing), query-string secrets, Spotify credentials,
 * constructor-injected extra secrets/patterns, and the guarantee that
 * sanitized output never contains the original secret. All methods are
 * null-safe.</p>
 */
class LogSanitizerTest {

    private static final LogSanitizer SANITIZER = LogSanitizer.defaults();

    // ------------------------------------------------------------------
    // Bearer Authorization header redaction
    // ------------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("bearerHeaderCases")
    void redactsBearerTokensInAuthorizationHeaders(String input, String expected) {
        assertThat(SANITIZER.sanitize(input)).isEqualTo(expected);
    }

    static Stream<Arguments> bearerHeaderCases() {
        return Stream.of(
                // Unquoted header, mixed-case key + value.
                Arguments.of("Authorization: Bearer AbCdEf123", "Authorization: Bearer ***"),
                // Lower-case scheme words must be redacted too.
                Arguments.of("authorization: bearer xyz", "authorization: bearer ***"),
                // No whitespace around the colon.
                Arguments.of("Authorization:Bearer noSpaces", "Authorization:Bearer ***"),
                // Token chars that appear in base64/URL-safe tokens: = / +
                Arguments.of("Authorization: Bearer abc==/+/xyz", "Authorization: Bearer ***"),
                // JSON-style double-quoted header.
                Arguments.of("\"Authorization\": \"Bearer AbCdEf123\"", "\"Authorization\": \"Bearer ***\""),
                // Lower-case scheme inside quotes; trailing quote must survive.
                Arguments.of("\"authorization\": \"bearer token123\"", "\"authorization\": \"bearer ***\""),
                // Single-quoted variant.
                Arguments.of("'Authorization': 'Bearer singleQ'", "'Authorization': 'Bearer ***'"),
                // Mid-string occurrence.
                Arguments.of("http 401: Authorization: Bearer midStr", "http 401: Authorization: Bearer ***"));
    }

    // ------------------------------------------------------------------
    // Query-string secret redaction
    // ------------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("querySecretCases")
    void redactsQueryStringSecrets(String input, String expected) {
        assertThat(SANITIZER.sanitize(input)).isEqualTo(expected);
    }

    static Stream<Arguments> querySecretCases() {
        return Stream.of(
                // ?key=value position.
                Arguments.of("http://host/status?code=abc123", "http://host/status?code=***"),
                // &key=value position.
                Arguments.of("http://host/path?keep=1&code=abc123", "http://host/path?keep=1&code=***"),
                Arguments.of("?refresh_token=abc", "?refresh_token=***"),
                Arguments.of("?access_token=abc", "?access_token=***"),
                Arguments.of("?token=abc", "?token=***"),
                // Percent-encoded secret values (/, +, =) are redacted wholesale.
                Arguments.of("?code=a%2Fb%2Bc%3Dd", "?code=***"),
                // Several secrets in one query string.
                Arguments.of("?code=abc&refresh_token=def&token=ghi",
                        "?code=***&refresh_token=***&token=***"),
                // Non-secret neighbours survive.
                Arguments.of("?code=abc&keep=this", "?code=***&keep=this"),
                // Fragment boundary stops the value.
                Arguments.of("?code=abc#frag", "?code=***#frag"),
                // Keys are matched case-insensitively.
                Arguments.of("?CODE=abc", "?CODE=***"),
                // No secrets -> unchanged.
                Arguments.of("http://host/status?volume=0.5", "http://host/status?volume=0.5"));
    }

    // ------------------------------------------------------------------
    // Spotify credential redaction
    // ------------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("credentialCases")
    void redactsSpotifyCredentials(String input, String expected) {
        assertThat(SANITIZER.sanitize(input)).isEqualTo(expected);
    }

    static Stream<Arguments> credentialCases() {
        return Stream.of(
                // Form/query style.
                Arguments.of("client_id=abc", "client_id=***"),
                Arguments.of("client_secret=def", "client_secret=***"),
                Arguments.of("username=user1", "username=***"),
                Arguments.of("password=pass1", "password=***"),
                // JSON-style quoted values; trailing quotes survive.
                Arguments.of("{\"client_id\": \"abc\", \"client_secret\": \"def\"}",
                        "{\"client_id\": \"***\", \"client_secret\": \"***\"}"),
                Arguments.of("{\"username\": \"u\", \"password\": \"p\"}",
                        "{\"username\": \"***\", \"password\": \"***\"}"),
                // Value containing characters that appear in real credentials.
                Arguments.of("client_secret=Ab/3+Xz_9==f", "client_secret=***"));
    }

    // ------------------------------------------------------------------
    // Constructor-injected extra secrets
    // ------------------------------------------------------------------

    @Test
    void redactsConstructorInjectedLiteralSecretsEverywhere() {
        LogSanitizer sanitizer = new LogSanitizer(Set.of("superSecret123"));
        assertThat(sanitizer.sanitize("prefix superSecret123 suffix"))
                .isEqualTo("prefix *** suffix")
                .doesNotContain("superSecret123");
        assertThat(sanitizer.sanitize("superSecret123")).isEqualTo("***");
        // Exact-match only: a different casing is not this literal.
        assertThat(sanitizer.sanitize("SUPERSECRET123")).isEqualTo("SUPERSECRET123");
    }

    @Test
    void redactsConstructorInjectedPatterns() {
        LogSanitizer sanitizer = new LogSanitizer(
                Set.of(),
                Set.of(Pattern.compile("\\bAPI-KEY-[A-Z0-9]+\\b")));
        assertThat(sanitizer.sanitize("key API-KEY-ABC123 used")).isEqualTo("key *** used");
        assertThat(sanitizer.sanitize("API-KEY-ABC123")).isEqualTo("***");
    }

    @Test
    void defaultsFactoryRedacts() {
        LogSanitizer defaults = LogSanitizer.defaults();
        assertThat(defaults.sanitize("Authorization: Bearer tok123"))
                .isEqualTo("Authorization: Bearer ***");
        assertThat(defaults.sanitize("?code=abc")).isEqualTo("?code=***");
    }

    // ------------------------------------------------------------------
    // Invariant: sanitized output never contains the original secret
    // ------------------------------------------------------------------

    @ParameterizedTest
    @ValueSource(strings = {
            "Authorization: Bearer secret-token-123",
            "\"Authorization\": \"Bearer secret-token-123\"",
            "?code=secret-token-123&refresh_token=secret-token-123",
            "?access_token=secret-token-123&token=secret-token-123",
            "client_secret=secret-token-123",
            "password=secret-token-123",
            "username=secret-token-123"
    })
    void sanitizedOutputNeverContainsTheSecret(String input) {
        assertThat(SANITIZER.sanitize(input)).doesNotContain("secret-token-123");
    }

    @Test
    void sanitizedOutputNeverContainsConfiguredSecret() {
        LogSanitizer sanitizer = new LogSanitizer(Set.of("spotify-credential-value-999"));
        String input = "Authorization: Bearer spotify-credential-value-999 "
                + "?code=spotify-credential-value-999&client_secret=spotify-credential-value-999";
        assertThat(sanitizer.sanitize(input)).doesNotContain("spotify-credential-value-999");
    }

    // ------------------------------------------------------------------
    // sanitizeUrl: scheme+host+path only
    // ------------------------------------------------------------------

    @ParameterizedTest
    @MethodSource("urlCases")
    void sanitizeUrlKeepsOnlySchemeHostPath(String url, String expected) {
        assertThat(SANITIZER.sanitizeUrl(url)).isEqualTo(expected);
    }

    static Stream<Arguments> urlCases() {
        return Stream.of(
                Arguments.of("https://daemon:23456/api/v1/status?token=abc",
                        "https://daemon:23456/api/v1/status"),
                Arguments.of("http://localhost:5432/web-api/v1/tracks/abc?code=x&refresh_token=y",
                        "http://localhost:5432/web-api/v1/tracks/abc"),
                Arguments.of("https://host/status", "https://host/status"),
                Arguments.of("https://host/", "https://host/"),
                Arguments.of("https://host/status?token=abc#frag", "https://host/status"),
                Arguments.of("no-scheme-path-only", "no-scheme-path-only"));
    }

    // ------------------------------------------------------------------
    // Null / empty safety + non-secret text is untouched
    // ------------------------------------------------------------------

    @Test
    void nullAndEmptyInputsAreSafe() {
        assertThat(SANITIZER.sanitize(null)).isNull();
        assertThat(SANITIZER.sanitize("")).isEmpty();
        assertThat(SANITIZER.sanitizeUrl(null)).isNull();
        assertThat(SANITIZER.sanitizeUrl("")).isEmpty();
    }

    @Test
    void plainTextWithoutSecretsIsUnchanged() {
        assertThat(SANITIZER.sanitize("GET /status HTTP/1.1")).isEqualTo("GET /status HTTP/1.1");
        assertThat(SANITIZER.sanitize("playback started for track xyz")).isEqualTo("playback started for track xyz");
        assertThat(SANITIZER.sanitize("https://example.com/status?volume=0.5"))
                .isEqualTo("https://example.com/status?volume=0.5");
    }
}
