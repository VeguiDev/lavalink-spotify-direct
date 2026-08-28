package dev.lavalinkplugins.golibrespot.source;

import com.sedmelluq.discord.lavaplayer.player.AudioPlayerManager;
import com.sedmelluq.discord.lavaplayer.source.AudioSourceManager;
import com.sedmelluq.discord.lavaplayer.track.AudioItem;
import com.sedmelluq.discord.lavaplayer.track.AudioReference;
import com.sedmelluq.discord.lavaplayer.track.AudioTrack;
import com.sedmelluq.discord.lavaplayer.track.AudioTrackInfo;
import org.springframework.stereotype.Service;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Placeholder {@link AudioSourceManager} for the {@code spdirect} source.
 *
 * <p>Compile-only skeleton for the build scaffold (T2). No real loading logic
 * exists yet — {@link #loadItem} deliberately returns {@code null} for every
 * identifier until the source-manager todo (T18) implements the spdirect
 * claiming + track pipeline.</p>
 */
@Service
public class GoLibrespotAudioSourceManager implements AudioSourceManager {

    @Override
    public String getSourceName() {
        return "spdirect";
    }

    @Override
    public AudioItem loadItem(AudioPlayerManager manager, AudioReference reference) {
        return null;
    }

    @Override
    public boolean isTrackEncodable(AudioTrack track) {
        return false;
    }

    @Override
    public void encodeTrack(AudioTrack track, DataOutput output) throws IOException {
    }

    @Override
    public AudioTrack decodeTrack(AudioTrackInfo trackInfo, DataInput input) throws IOException {
        return null;
    }

    @Override
    public void shutdown() {
    }
}
