package dev.lavalinkplugins.golibrespot.fifo;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Pure s16le PCM decoder (T12). No FIFO, no platform dependency — runs on
 * every OS.
 *
 * <p>Contract under test (docs/API_CONTRACT.md §6): interleaved little-endian
 * signed 16-bit stereo at fixed 44100 Hz; a stereo frame is 4 bytes = 2 shorts
 * (L then R). The decoder never emits partial frames and preserves the 1–3
 * trailing partial bytes across reads.
 */
class PcmDecoderTest {

  @Test
  void goldenSineDecodesToExactShorts() {
    int sampleRate = 44100;
    int samplesPerChannel = sampleRate; // 1 second
    byte[] pcm = new byte[samplesPerChannel * PcmDecoder.FRAME_BYTES];
    short[] expected = new short[samplesPerChannel * PcmDecoder.FRAME_SHORTS];
    int p = 0;
    for (int i = 0; i < samplesPerChannel; i++) {
      double t = (double) i / sampleRate;
      short left = (short) Math.round(Math.sin(2 * Math.PI * 440.0 * t) * 32767);
      short right = (short) Math.round(Math.sin(2 * Math.PI * 220.0 * t) * 32767);
      expected[2 * i] = left;
      expected[2 * i + 1] = right;
      pcm[p++] = (byte) (left & 0xFF);
      pcm[p++] = (byte) ((left >> 8) & 0xFF);
      pcm[p++] = (byte) (right & 0xFF);
      pcm[p++] = (byte) ((right >> 8) & 0xFF);
    }

    short[] decoded = new PcmDecoder().decode(pcm);

    assertThat(decoded).containsExactly(expected);
  }

  @Test
  void goldenSineDecodedInOddChunksMatchesSingleShot() {
    byte[] pcm = sineChunk(44100, 0.5);
    short[] singleShot = new PcmDecoder().decode(pcm);

    PcmDecoder streaming = new PcmDecoder();
    short[] all = new short[pcm.length / 2];
    int[] sizes = {1, 2, 3, 5, 7, 11, 13, 17};
    int pos = 0;
    int w = 0;
    int si = 0;
    while (pos < pcm.length) {
      int n = Math.min(sizes[si++ % sizes.length], pcm.length - pos);
      short[] part = streaming.decode(pcm, pos, n);
      System.arraycopy(part, 0, all, w, part.length);
      w += part.length;
      pos += n;
    }

    assertThat(streaming.pendingBytes()).isZero();
    assertThat(w).isEqualTo(all.length);
    assertThat(all).containsExactly(singleShot);
  }

  @Test
  void decodesLittleEndianSigned16Explicitly() {
    // bytes 0x34 0x12 0x78 0x56 -> shorts 0x1234, 0x5678
    // bytes 0x00 0x80 0xFF 0x7F -> shorts -32768 (MIN), 32767 (MAX)
    short[] out =
        new PcmDecoder().decode(new byte[] {0x34, 0x12, 0x78, 0x56, 0x00, (byte) 0x80, (byte) 0xFF, 0x7F});
    assertThat(out).containsExactly(0x1234, 0x5678, Short.MIN_VALUE, Short.MAX_VALUE);
  }

  @Test
  void singleFrameChunkEmitsTwoShortsLeftThenRight() {
    assertThat(new PcmDecoder().decode(new byte[] {0x01, 0x00, 0x02, 0x00}))
        .containsExactly(1, 2);
  }

  @Test
  void neverEmitsPartialFramesAndPreservesRemainder() {
    PcmDecoder decoder = new PcmDecoder();
    // 10 bytes = 2.5 frames (shorts 1..5): only whole frames may be emitted
    short[] out = decoder.decode(new byte[] {1, 0, 2, 0, 3, 0, 4, 0, 5, 0});

    assertThat(out).containsExactly(1, 2, 3, 4);
    assertThat(decoder.pendingBytes()).isEqualTo(2);

    // the two trailing bytes complete the straddling frame on the next read
    assertThat(decoder.decode(new byte[] {6, 0})).containsExactly(5, 6);
    assertThat(decoder.pendingBytes()).isZero();
  }

  @Test
  void oddLengthChunkTorturePreservesPartialFrames() {
    short[] expected = new short[100];
    byte[] pcm = new byte[200];
    for (int i = 0; i < expected.length; i++) {
      expected[i] = (short) (i - 50);
      pcm[2 * i] = (byte) (expected[i] & 0xFF);
      pcm[2 * i + 1] = (byte) ((expected[i] >> 8) & 0xFF);
    }

    PcmDecoder decoder = new PcmDecoder();
    short[] all = new short[expected.length];
    int[] sizes = {1, 3, 3, 1, 3, 1, 7, 2, 1, 1, 5};
    int pos = 0;
    int w = 0;
    int si = 0;
    while (pos < pcm.length) {
      int n = Math.min(sizes[si++ % sizes.length], pcm.length - pos);
      short[] part = decoder.decode(pcm, pos, n);
      System.arraycopy(part, 0, all, w, part.length);
      w += part.length;
      pos += n;
    }

    assertThat(decoder.pendingBytes()).isZero();
    assertThat(all).containsExactly(expected);
  }

  @Test
  void discardModeDrainsWithoutEmittingAndPreservesRemainder() {
    PcmDecoder decoder = new PcmDecoder();
    decoder.setDiscardMode(true);

    // 10 bytes = 2.5 frames — dropped, but the trailing 2 bytes survive
    assertThat(decoder.decode(new byte[] {1, 0, 2, 0, 3, 0, 4, 0, 5, 0})).isEmpty();
    assertThat(decoder.pendingBytes()).isEqualTo(2);
    assertThat(decoder.discardMode()).isTrue();

    // 2 more bytes complete the straddling frame — dropped too, remainder empty
    assertThat(decoder.decode(new byte[] {6, 0})).isEmpty();
    assertThat(decoder.pendingBytes()).isZero();

    decoder.setDiscardMode(false);
    assertThat(decoder.decode(new byte[] {7, 0, 8, 0, 9, 0, 10, 0})).containsExactly(7, 8, 9, 10);
    assertThat(decoder.pendingBytes()).isZero();
  }

  @Test
  void discardKeepsRemainderForTheStraddlingFrame() {
    PcmDecoder decoder = new PcmDecoder();
    decoder.setDiscardMode(true);
    decoder.decode(new byte[] {1, 0, 2, 0, 3, 0, 4, 0, 5, 0}); // drops (1,2),(3,4); keeps {5,0}
    decoder.setDiscardMode(false);

    assertThat(decoder.decode(new byte[] {6, 0})).containsExactly(5, 6);
    assertThat(decoder.pendingBytes()).isZero();
  }

  @Test
  void resetClearsPartialFrameRemainder() {
    PcmDecoder decoder = new PcmDecoder();
    decoder.decode(new byte[] {1, 0, 2, 0, 3, 0}); // 1.5 frames -> (1,2) emitted, {3,0} pending
    assertThat(decoder.pendingBytes()).isEqualTo(2);

    decoder.reset();
    assertThat(decoder.pendingBytes()).isZero();

    // decode starts fresh: pre-reset bytes must not leak into frames
    assertThat(decoder.decode(new byte[] {4, 0, 5, 0})).containsExactly(4, 5);
  }

  @Test
  void emptyInputEmitsNothingAndKeepsRemainder() {
    PcmDecoder decoder = new PcmDecoder();
    decoder.decode(new byte[] {1, 0});

    assertThat(decoder.decode(new byte[0])).isEmpty();
    assertThat(decoder.decode(new byte[0], 0, 0)).isEmpty();
    assertThat(decoder.pendingBytes()).isEqualTo(2);
  }

  @Test
  void largeBufferSpanningManyFramesDecodesExactly() {
    Random rnd = new Random(42);
    int frames = 100_000;
    short[] expected = new short[frames * PcmDecoder.FRAME_SHORTS];
    byte[] pcm = new byte[frames * PcmDecoder.FRAME_BYTES];
    for (int i = 0; i < frames; i++) {
      short left = (short) rnd.nextInt();
      short right = (short) rnd.nextInt();
      expected[2 * i] = left;
      expected[2 * i + 1] = right;
      int b = 4 * i;
      pcm[b] = (byte) (left & 0xFF);
      pcm[b + 1] = (byte) ((left >> 8) & 0xFF);
      pcm[b + 2] = (byte) (right & 0xFF);
      pcm[b + 3] = (byte) ((right >> 8) & 0xFF);
    }

    short[] decoded = new PcmDecoder().decode(pcm);

    assertThat(decoded).containsExactly(expected);
  }

  /** s16le stereo sine chunk: 440 Hz on L, 220 Hz on R (interleave proof). */
  private static byte[] sineChunk(int sampleRate, double seconds) {
    int samplesPerChannel = (int) Math.round(sampleRate * seconds);
    byte[] pcm = new byte[samplesPerChannel * PcmDecoder.FRAME_BYTES];
    int p = 0;
    for (int i = 0; i < samplesPerChannel; i++) {
      double t = (double) i / sampleRate;
      short left = (short) Math.round(Math.sin(2 * Math.PI * 440.0 * t) * 32767);
      short right = (short) Math.round(Math.sin(2 * Math.PI * 220.0 * t) * 32767);
      pcm[p++] = (byte) (left & 0xFF);
      pcm[p++] = (byte) ((left >> 8) & 0xFF);
      pcm[p++] = (byte) (right & 0xFF);
      pcm[p++] = (byte) ((right >> 8) & 0xFF);
    }
    return pcm;
  }
}
