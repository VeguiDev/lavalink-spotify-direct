package dev.lavalinkplugins.golibrespot.identifier;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses identifiers in the "spdirect" namespace (source name "spdirect").
 *
 * <p>Claims exactly two forms:
 * <ul>
 *   <li>{@code spdirect:<22-char-base62>}</li>
 *   <li>{@code spdirect:spotify:track:<22-char-base62>}</li>
 * </ul>
 * where base62 is {@code [0-9A-Za-z]{22}} (case-sensitive, upper and lower both
 * valid). Ordinary Spotify URIs/URLs are never claimed, but track-shaped ones
 * are recognized for a {@link NotClaimed#diagnosticHint()} that suggests the
 * equivalent {@code spdirect:<id>} form.
 */
public final class TrackIdParser {

  private static final String PREFIX = "spdirect:";
  private static final String SPOTIFY_PREFIX = "spotify:";

  private static final Pattern BASE62_22 = Pattern.compile("[0-9A-Za-z]{22}");
  private static final Pattern SPOTIFY_TRACK_URI =
      Pattern.compile("^spotify:track:([0-9A-Za-z]+)$");
  private static final Pattern OPEN_SPOTIFY_TRACK_URL =
      Pattern.compile("^https://open\\.spotify\\.com/(?:[^/]+/)?track/([0-9A-Za-z]+)$");

  private TrackIdParser() {
    // utility class
  }

  /**
   * Parses {@code identifier} without ever throwing.
   *
   * @return a {@link TrackId} when the identifier is claimed by this source,
   *     {@link NotClaimed} for anything outside the spdirect namespace (with a
   *     conversion hint for ordinary Spotify track URIs/URLs), or
   *     {@link Malformed} for spdirect-prefixed input with a wrong-length or
   *     invalid-charset track id, or for null/empty input.
   */
  public static TrackIdParseResult parse(String identifier) {
    if (identifier == null) {
      return new TrackIdParseResult.Malformed("identifier is null");
    }
    if (identifier.isEmpty()) {
      return new TrackIdParseResult.Malformed("identifier is empty");
    }
    if (!identifier.startsWith(PREFIX)) {
      return parseForeign(identifier);
    }
    String rest = identifier.substring(PREFIX.length());
    if (rest.startsWith(SPOTIFY_PREFIX)) {
      String body = rest.substring(SPOTIFY_PREFIX.length());
      if (body.startsWith("track:")) {
        String id = body.substring("track:".length());
        if (isBase62Id(id)) {
          return new TrackIdParseResult.TrackId(id);
        }
        return new TrackIdParseResult.Malformed(
            "invalid track id after " + PREFIX + SPOTIFY_PREFIX + "track: '" + id + "'");
      }
      // spdirect:spotify:album|playlist|episode|search|... -> not claimed
      return new TrackIdParseResult.NotClaimed(Optional.empty());
    }
    if (isBase62Id(rest)) {
      return new TrackIdParseResult.TrackId(rest);
    }
    return new TrackIdParseResult.Malformed(
        "invalid " + PREFIX + "identifier: '" + rest + "'");
  }

  private static TrackIdParseResult parseForeign(String identifier) {
    Matcher uri = SPOTIFY_TRACK_URI.matcher(identifier);
    if (uri.matches()) {
      return new TrackIdParseResult.NotClaimed(Optional.of(PREFIX + uri.group(1)));
    }
    Matcher url = OPEN_SPOTIFY_TRACK_URL.matcher(identifier);
    if (url.matches()) {
      return new TrackIdParseResult.NotClaimed(Optional.of(PREFIX + url.group(1)));
    }
    return new TrackIdParseResult.NotClaimed(Optional.empty());
  }

  private static boolean isBase62Id(String candidate) {
    return BASE62_22.matcher(candidate).matches();
  }

  /** Result of {@link TrackIdParser#parse(String)}. */
  public sealed interface TrackIdParseResult {

    /** A claimed identifier: the bare 22-char Spotify track id. */
    record TrackId(String id) implements TrackIdParseResult {}

    /** Not claimed by this source; optionally carries a conversion hint. */
    record NotClaimed(Optional<String> diagnosticHint) implements TrackIdParseResult {}

    /** spdirect-prefixed input that is structurally invalid. */
    record Malformed(String reason) implements TrackIdParseResult {}
  }
}
