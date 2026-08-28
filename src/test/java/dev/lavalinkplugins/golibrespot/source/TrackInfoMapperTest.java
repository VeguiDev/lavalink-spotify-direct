package dev.lavalinkplugins.golibrespot.source;

import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AudioTrackInfoMapper} — the mapping from
 * go-librespot track metadata to a Lavaplayer {@link AudioTrackInfo}.
 *
 * <p>Covers the exact contract: title passthrough, artists joined with
 * {@code ", "}, duration required &gt; 0 (no fabrication), identifier always
 * {@code spdirect:&lt;id&gt;}, isStream always {@code true}, uri always
 * {@code null}, and optional artwork URL / ISRC pass through as-is (may be
 * {@code null}).</p>
 */
class TrackInfoMapperTest {

    private final AudioTrackInfoMapper mapper = new AudioTrackInfoMapper();

    private TrackMetadata metadata(long durationMs) {
        return new TrackMetadata(
                "4iV5W9uYEdYUVa79Axb7Rh",
                "夜に駆ける",
                List.of("YOASOBI"),
                "THE BOOK",
                durationMs,
                "https://example.com/artwork.jpg",
                "JPU901100390"
        );
    }

    @Test
    void mapsFullMetadataIncludingUnicodeTitleAndAuthor() {
        Optional<AudioTrackInfo> result = mapper.map(metadata(3_040_000L));

        assertThat(result).isPresent();
        AudioTrackInfo info = result.orElseThrow();
        assertThat(info.title).isEqualTo("夜に駆ける");
        assertThat(info.author).isEqualTo("YOASOBI");
        assertThat(info.length).isEqualTo(3_040_000L);
        assertThat(info.identifier).isEqualTo("spdirect:4iV5W9uYEdYUVa79Axb7Rh");
        assertThat(info.isStream).isTrue();
        assertThat(info.uri).isNull();
        assertThat(info.artworkUrl).isEqualTo("https://example.com/artwork.jpg");
        assertThat(info.isrc).isEqualTo("JPU901100390");
    }

    @Test
    void joinsMultipleArtistsWithCommaSpace() {
        TrackMetadata metadata = new TrackMetadata(
                "0jv2SxQk2Tz29TlCVODMW7",
                "「ハルカ」",
                List.of("サカナクション", "アン・サリー", "Dave the Band"),
                "some album",
                252_000L,
                null,
                null
        );

        AudioTrackInfo info = mapper.map(metadata).orElseThrow();

        assertThat(info.author).isEqualTo("サカナクション, アン・サリー, Dave the Band");
    }

    @Test
    void singleArtistHasNoJoinArtifacts() {
        TrackMetadata metadata = new TrackMetadata(
                "abc123", "title", List.of("Solo Artist"), "album", 60_000L, null, null);

        AudioTrackInfo info = mapper.map(metadata).orElseThrow();

        assertThat(info.author).isEqualTo("Solo Artist");
    }

    @Test
    void emptyArtistListMapsToEmptyAuthor() {
        TrackMetadata metadata = new TrackMetadata(
                "abc123", "title", List.of(), "album", 60_000L, null, null);

        AudioTrackInfo info = mapper.map(metadata).orElseThrow();

        assertThat(info.author).isEmpty();
    }

    @Test
    void failsWhenDurationIsZero() {
        assertThat(mapper.map(metadata(0L))).isEmpty();
    }

    @Test
    void failsWhenDurationIsNegative() {
        assertThat(mapper.map(metadata(-1L))).isEmpty();
    }

    @Test
    void isrcAbsentMapsToNull() {
        TrackMetadata metadata = new TrackMetadata(
                "abc123", "title", List.of("artist"), "album", 60_000L,
                "https://example.com/artwork.jpg", null);

        AudioTrackInfo info = mapper.map(metadata).orElseThrow();

        assertThat(info.isrc).isNull();
    }

    @Test
    void artworkUrlAbsentMapsToNull() {
        TrackMetadata metadata = new TrackMetadata(
                "abc123", "title", List.of("artist"), "album", 60_000L, null, null);

        AudioTrackInfo info = mapper.map(metadata).orElseThrow();

        assertThat(info.artworkUrl).isNull();
        assertThat(info.title).isEqualTo("title");
        assertThat(info.length).isEqualTo(60_000L);
        assertThat(info.identifier).isEqualTo("spdirect:abc123");
    }

    @Test
    void identifierAlwaysCarriesSpdirectPrefix() {
        AudioTrackInfo info = mapper.map(metadata(60_000L)).orElseThrow();

        assertThat(info.identifier).startsWith("spdirect:");
        assertThat(info.identifier).isEqualTo("spdirect:4iV5W9uYEdYUVa79Axb7Rh");
    }

    @Test
    void isStreamAlwaysTrueAndUriAlwaysNull() {
        AudioTrackInfo info = mapper.map(metadata(60_000L)).orElseThrow();

        assertThat(info.isStream).isTrue();
        assertThat(info.uri).isNull();
    }
}
