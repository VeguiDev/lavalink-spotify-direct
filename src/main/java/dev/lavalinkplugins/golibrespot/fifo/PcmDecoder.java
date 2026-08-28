package dev.lavalinkplugins.golibrespot.fifo;

import java.util.Arrays;

/**
 * Pure s16le decoder for the daemon's PCM plane (docs/API_CONTRACT.md §6):
 * interleaved, little-endian, signed 16-bit, fixed 44100 Hz stereo. A stereo
 * frame is 4 bytes = 2 shorts (L then R).
 *
 * <p>Consumes arbitrary byte chunks (FIFO reads are not frame-aligned),
 * retains the trailing 1–3 partial bytes across calls and only ever emits
 * complete frames as a {@code short[]} of even length (2 shorts per frame) —
 * partial frames are never emitted. Feeding the Lavaplayer pipeline happens
 * downstream (T18); this class only performs exact frame assembly.
 *
 * <p>Discard mode ({@link #setDiscardMode(boolean)}) consumes input without
 * emitting anything — the seek drain (T16) drops pre-seek PCM under the
 * 5 s / 4 MiB caps (docs/DECISIONS.md); the caller owns those bounds, this
 * decoder stays pure. The trailing partial-byte remainder is still preserved
 * in discard mode so a later non-discard decode can complete the straddling
 * frame; {@link #reset()} clears it at the seek boundary (DECISIONS.md §3.1
 * step 3).
 *
 * <p>Not thread-safe: exactly one consumer thread must own an instance.
 */
public final class PcmDecoder {

  /** Bytes per stereo frame (2 channels × 16-bit LE). */
  public static final int FRAME_BYTES = 4;

  /** Shorts per stereo frame. */
  public static final int FRAME_SHORTS = 2;

  private static final byte[] EMPTY = new byte[0];
  private static final short[] NO_FRAMES = new short[0];

  private byte[] remainder = EMPTY;
  private boolean discardMode;

  /** Creates a decoder with no pending bytes and discard mode off. */
  public PcmDecoder() {}

  /** Decodes one chunk of PCM bytes. */
  public short[] decode(byte[] data) {
    return decode(data, 0, data.length);
  }

  /**
   * Decodes {@code length} bytes of PCM starting at {@code offset} in {@code
   * data}.
   *
   * @return every complete stereo frame as {@code L,R,L,R,...} shorts (length
   *     is always a multiple of {@link #FRAME_SHORTS}); the 1–3 trailing
   *     partial bytes stay in the decoder for the next call
   */
  public short[] decode(byte[] data, int offset, int length) {
    if (length == 0) {
      return NO_FRAMES;
    }
    int carry = remainder.length;
    int total = carry + length;
    int frames = total / FRAME_BYTES;
    int newRemainder = total % FRAME_BYTES;

    if (discardMode) {
      // Consume everything; keep only the bytes that would form the next frame.
      remainder = tail(remainder, data, offset, length, newRemainder);
      return NO_FRAMES;
    }

    byte[] all = new byte[total];
    if (carry > 0) {
      System.arraycopy(remainder, 0, all, 0, carry);
    }
    System.arraycopy(data, offset, all, carry, length);

    short[] out = new short[frames * FRAME_SHORTS];
    for (int i = 0; i < frames; i++) {
      int b = i * FRAME_BYTES;
      out[i * FRAME_SHORTS] = le16(all[b], all[b + 1]);
      out[i * FRAME_SHORTS + 1] = le16(all[b + 2], all[b + 3]);
    }
    remainder = newRemainder == 0 ? EMPTY : Arrays.copyOfRange(all, all.length - newRemainder, all.length);
    return out;
  }

  private static short le16(byte lo, byte hi) {
    return (short) ((lo & 0xFF) | (hi << 8));
  }

  /**
   * Toggles discard mode: {@link #decode} then consumes input and emits
   * nothing (seek drain — pre-seek PCM is dropped under the caller's
   * 5 s / 4 MiB caps). The trailing partial bytes are still preserved.
   */
  public void setDiscardMode(boolean discard) {
    this.discardMode = discard;
  }

  /** True while discard mode is active. */
  public boolean discardMode() {
    return discardMode;
  }

  /** Trailing partial bytes retained across reads (0..3). */
  public int pendingBytes() {
    return remainder.length;
  }

  /** Clears the partial-frame remainder (seek boundary, DECISIONS.md §3.1). */
  public void reset() {
    remainder = EMPTY;
  }

  /** Keeps the LAST {@code keep} bytes of (head ++ data[offset..offset+length)). */
  private static byte[] tail(byte[] head, byte[] data, int offset, int length, int keep) {
    if (keep == 0) {
      return EMPTY;
    }
    byte[] out = new byte[keep];
    int fromData = Math.min(length, keep);
    int fromHead = keep - fromData;
    if (fromHead > 0) {
      System.arraycopy(head, head.length - fromHead, out, 0, fromHead);
    }
    System.arraycopy(data, offset + length - fromData, out, fromHead, fromData);
    return out;
  }
}
