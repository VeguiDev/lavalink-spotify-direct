package dev.lavalinkplugins.golibrespot.source;

import com.sedmelluq.discord.lavaplayer.filter.AudioPipeline;
import com.sedmelluq.discord.lavaplayer.filter.AudioPipelineFactory;
import com.sedmelluq.discord.lavaplayer.filter.PcmFormat;
import com.sedmelluq.discord.lavaplayer.track.playback.AudioProcessingContext;
import java.util.Objects;
import java.util.function.BiFunction;

/**
 * The PCM sink the T18 track feeds — a narrow interface over Lavaplayer's
 * {@link AudioPipeline} so tests can observe {@code process}/{@code seekPerformed}/
 * {@code close} without reaching into the pipeline internals. The production
 * factory is {@link #defaultFactory()}; the {@link GoLibrespotAudioTrack} test
 * seam injects a recording wrapper.
 */
public interface PcmPipeline {

  /** Feeds interleaved stereo shorts (complete frames; {@code length} = number of shorts). */
  void process(short[] input, int offset, int length) throws InterruptedException;

  /** Marks the filter chain to clear its state at the next input frame (post-seek). */
  void seekPerformed(long requestedPosition, long accuratePosition);

  /** Flushes buffered filter state (best-effort, before close). */
  void flush() throws InterruptedException;

  /** Closes the pipeline (idempotent; called in {@code finally}). */
  void close();

  /** Production factory: the real Lavaplayer pipeline over the daemon's PCM format. */
  static BiFunction<AudioProcessingContext, PcmFormat, PcmPipeline> defaultFactory() {
    return (context, format) -> {
      Objects.requireNonNull(context, "context");
      Objects.requireNonNull(format, "format");
      return new AudioPipelineAdapter(AudioPipelineFactory.create(context, format));
    };
  }

  /** Adapts Lavaplayer's {@link AudioPipeline} (nominal typing: not a PcmPipeline). */
  final class AudioPipelineAdapter implements PcmPipeline {

    private final AudioPipeline delegate;

    AudioPipelineAdapter(AudioPipeline delegate) {
      this.delegate = delegate;
    }

    @Override
    public void process(short[] input, int offset, int length) throws InterruptedException {
      delegate.process(input, offset, length);
    }

    @Override
    public void seekPerformed(long requestedPosition, long accuratePosition) {
      delegate.seekPerformed(requestedPosition, accuratePosition);
    }

    @Override
    public void flush() throws InterruptedException {
      delegate.flush();
    }

    @Override
    public void close() {
      delegate.close();
    }
  }
}
