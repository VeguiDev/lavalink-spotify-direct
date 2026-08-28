package dev.lavalinkplugins.golibrespot.source;

import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;

import java.util.Optional;

/**
 * Maps {@link TrackMetadata} (go-librespot track metadata) to a Lavaplayer
 * {@link AudioTrackInfo}.
 *
 * <p>Contract (exact): title = {@code m.title()}; author = artists joined with
 * {@code ", "} (empty artist list &rarr; empty string); length =
 * {@code m.durationMs()}; identifier = {@code spdirect:&lt;id&gt;}; isStream =
 * {@code true}; uri = {@code null}; artwork URL and ISRC pass through as-is
 * (may be {@code null}). No fields are invented: a non-positive duration makes
 * the mapping fail ({@link Optional#empty()}) rather than fabricate a
 * duration.</p>
 */
public final class AudioTrackInfoMapper {

    private static final String IDENTIFIER_PREFIX = "spdirect:";

    /**
     * Maps the metadata to an {@link AudioTrackInfo}, or {@link Optional#empty()}
     * when {@code m.durationMs() <= 0} (a track without a real duration cannot be
     * represented).
     */
    public Optional<AudioTrackInfo> map(TrackMetadata m) {
        if (m.durationMs() <= 0) {
            return Optional.empty();
        }
        return Optional.of(new AudioTrackInfo(
                m.title(),
                String.join(", ", m.artists()),
                m.durationMs(),
                IDENTIFIER_PREFIX + m.id(),
                true,
                null,
                m.artworkUrl(),
                m.isrc()));
    }
}
