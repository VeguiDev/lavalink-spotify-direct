package dev.lavalinkplugins.golibrespot.identifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import dev.lavalinkplugins.golibrespot.identifier.TrackIdParser.TrackIdParseResult;
import dev.lavalinkplugins.golibrespot.identifier.TrackIdParser.TrackIdParseResult.Malformed;
import dev.lavalinkplugins.golibrespot.identifier.TrackIdParser.TrackIdParseResult.NotClaimed;
import dev.lavalinkplugins.golibrespot.identifier.TrackIdParser.TrackIdParseResult.TrackId;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class TrackIdParserTest {

  private static final String TRACK_ID = "4iV5W9uYEdYUVa79Axb7Rh";
  private static final String ALBUM_ID = "6akEvsycLGftJxYudPjmhK";

  @Test
  void acceptsBareSpdirectId() {
    assertThat(TrackIdParser.parse("spdirect:" + TRACK_ID))
        .isEqualTo(new TrackId(TRACK_ID));
  }

  @Test
  void acceptsSpdirectSpotifyTrackUri() {
    assertThat(TrackIdParser.parse("spdirect:spotify:track:" + TRACK_ID))
        .isEqualTo(new TrackId(TRACK_ID));
  }

  @Test
  void acceptsUppercaseBase62() {
    String upper = TRACK_ID.toUpperCase();
    assertThat(TrackIdParser.parse("spdirect:" + upper))
        .isEqualTo(new TrackId(upper));
  }

  @Test
  void acceptsLowercaseBase62() {
    String lower = TRACK_ID.toLowerCase();
    assertThat(TrackIdParser.parse("spdirect:" + lower))
        .isEqualTo(new TrackId(lower));
  }

  @Test
  void rejectsTwentyOneCharId() {
    assertThat(TrackIdParser.parse("spdirect:" + TRACK_ID.substring(0, 21)))
        .isInstanceOf(Malformed.class);
  }

  @Test
  void rejectsTwentyThreeCharId() {
    assertThat(TrackIdParser.parse("spdirect:" + TRACK_ID + "A"))
        .isInstanceOf(Malformed.class);
  }

  @Test
  void rejectsInvalidCharset() {
    String bad = TRACK_ID.substring(0, 21) + "-";
    assertThat(TrackIdParser.parse("spdirect:" + bad))
        .isInstanceOf(Malformed.class);
  }

  @Test
  void rejectsSpdirectTrackUriWithWrongLengthId() {
    assertThat(TrackIdParser.parse("spdirect:spotify:track:abc"))
        .isInstanceOf(Malformed.class);
  }

  @Test
  void rejectsBareSpdirectWithWrongLength() {
    assertThat(TrackIdParser.parse("spdirect:abc"))
        .isInstanceOf(Malformed.class);
  }

  @Test
  void ordinarySpotifyTrackUriIsClaimed() {
    TrackIdParseResult result = TrackIdParser.parse("spotify:track:" + TRACK_ID);
    assertThat(result).isEqualTo(new TrackId(TRACK_ID));
  }

  @Test
  void openSpotifyTrackUrlIsClaimed() {
    TrackIdParseResult result =
        TrackIdParser.parse("https://open.spotify.com/track/" + TRACK_ID);
    assertThat(result).isEqualTo(new TrackId(TRACK_ID));
  }

  @Test
  void openSpotifyIntlTrackUrlIsClaimed() {
    TrackIdParseResult result =
        TrackIdParser.parse("https://open.spotify.com/intl-xx/track/" + TRACK_ID);
    assertThat(result).isEqualTo(new TrackId(TRACK_ID));
  }

  @Test
  void openSpotifyTrackUrlAcceptsQueryAndFragment() {
    assertThat(TrackIdParser.parse(
        "https://open.spotify.com/intl-es/track/" + TRACK_ID + "?si=abc#fragment"))
        .isEqualTo(new TrackId(TRACK_ID));
  }

  @Test
  void spdirectSpotifyNonTrackKindsAreNotClaimed() {
    assertThat(TrackIdParser.parse("spdirect:spotify:album:" + ALBUM_ID))
        .isInstanceOf(NotClaimed.class);
    assertThat(TrackIdParser.parse("spdirect:spotify:playlist:37i9dQZF1DXcBWIGoYBM5M"))
        .isInstanceOf(NotClaimed.class);
    assertThat(TrackIdParser.parse("spdirect:spotify:episode:512ojhOuo1ktJprKbVcKyQ"))
        .isInstanceOf(NotClaimed.class);
    assertThat(TrackIdParser.parse("spdirect:spotify:search:hello"))
        .isInstanceOf(NotClaimed.class);
  }

  @Test
  void ordinaryAlbumAndPlaylistUrisAreClaimed() {
    assertThat(TrackIdParser.parse("spotify:album:" + ALBUM_ID))
        .isEqualTo(new TrackIdParseResult.CollectionId("album", ALBUM_ID));
    assertThat(TrackIdParser.parse("spotify:playlist:37i9dQZF1DXcBWIGoYBM5M"))
        .isEqualTo(new TrackIdParseResult.CollectionId("playlist", "37i9dQZF1DXcBWIGoYBM5M"));
    assertThat(TrackIdParser.parse("spotify:episode:512ojhOuo1ktJprKbVcKyQ"))
        .isInstanceOf(NotClaimed.class);
    assertThat(TrackIdParser.parse("spotify:search:hello"))
        .isInstanceOf(NotClaimed.class);
  }

  @Test
  void openSpotifyAlbumsAndPlaylistsAreClaimed() {
    assertThat(TrackIdParser.parse("https://open.spotify.com/album/" + ALBUM_ID))
        .isEqualTo(new TrackIdParseResult.CollectionId("album", ALBUM_ID));
    assertThat(TrackIdParser.parse("https://open.spotify.com/playlist/37i9dQZF1DXcBWIGoYBM5M"))
        .isEqualTo(new TrackIdParseResult.CollectionId("playlist", "37i9dQZF1DXcBWIGoYBM5M"));
    assertThat(TrackIdParser.parse("https://open.spotify.com/episode/512ojhOuo1ktJprKbVcKyQ"))
        .isInstanceOf(NotClaimed.class);
    assertThat(TrackIdParser.parse("https://open.spotify.com/search/hello"))
        .isInstanceOf(NotClaimed.class);
  }

  @Test
  void nullAndEmptyAreMalformed() {
    assertThat(TrackIdParser.parse(null)).isInstanceOf(Malformed.class);
    assertThat(TrackIdParser.parse("")).isInstanceOf(Malformed.class);
  }

  @Test
  void parseNeverThrows() {
    for (String input : Arrays.asList(
        null,
        "",
        "spdirect:",
        "spdirect:abc",
        "spdirect:spotify:track:abc",
        "spdirect:spotify:album:" + ALBUM_ID,
        "spotify:track:x",
        "https://open.spotify.com/track/x",
        "https://open.spotify.com/album/x",
        "garbage",
        "SPDIRECT:" + TRACK_ID)) {
      assertThatCode(() -> TrackIdParser.parse(input))
          .doesNotThrowAnyException();
    }
  }
}
