package dev.lavalinkplugins.golibrespot.fifo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.lavalinkplugins.golibrespot.fifo.FifoReader.Event;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/**
 * Always-draining FIFO reader (T12). Linux-only: tests drive real mkfifo
 * FIFOs, gated by {@code @EnabledOnOs(OS.LINUX)} plus {@link
 * FifoTestUtil#requireMkfifo()} — on Windows the suite is skipped and the
 * build stays green; CI (ubuntu) executes it.
 *
 * <p>Contract under test (docs/API_CONTRACT.md §6): a dedicated thread reads
 * fixed 16 KiB chunks off the FIFO into a bounded consumer queue; when the
 * writer closes (EOF) the reader records an {@link Event.Eof}, KEEPS the read
 * end open, and resumes delivering when the writer reopens (play-over-play
 * replacement). The FIFO read never stalls on a slow consumer (drop-oldest
 * policy) — backpressure exists only at the consumer boundary.
 */
@EnabledOnOs(OS.LINUX)
class FifoReaderTest {

  private static final Duration BOUNDED = Duration.ofSeconds(10);

  private Path fifo;
  private FifoReader reader;
  private Rendezvous pair;

  @BeforeEach
  void setUp() throws Exception {
    FifoTestUtil.requireMkfifo();
    fifo = FifoTestUtil.createTempFifo();
    pair = openRendezvous(fifo);
    reader = new FifoReader(pair.read);
    reader.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (reader != null) {
      reader.close();
    }
    if (pair != null) {
      pair.write.close();
    }
    FifoTestUtil.deleteTempFifo(fifo);
  }

  @Test
  void deliversBytesWrittenToFifo() throws Exception {
    byte[] expected = "s16le pcm".getBytes(StandardCharsets.US_ASCII);
    pair.write.write(expected);
    pair.write.flush();

    byte[] received = collectBytes(reader, expected.length, 5_000);

    assertThat(received).isEqualTo(expected);
    assertThat(reader.eofCount()).isZero();
    assertThat(reader.isRunning()).isTrue();
  }

  @Test
  void recordsEofAndStaysAliveWhenWriterCloses() throws Exception {
    byte[] chunk = {1, 2, 3, 4};
    pair.write.write(chunk);
    pair.write.flush();
    assertThat(collectBytes(reader, chunk.length, 5_000)).isEqualTo(chunk);

    pair.write.close(); // writer EOF

    await().atMost(BOUNDED).until(() -> reader.eofCount() >= 1);
    assertThat(reader.isRunning()).as("reader survives EOF with the read end open").isTrue();
    assertThat(reader.failure()).isNull();
  }

  @Test
  void resumesDeliveringAfterWriterReopens() throws Exception {
    byte[] first = {0, 1, 0, 2};
    pair.write.write(first);
    pair.write.flush();
    assertThat(collectBytes(reader, first.length, 5_000)).isEqualTo(first);

    pair.write.close();
    await().atMost(BOUNDED).until(() -> reader.eofCount() >= 1);

    // a brand-new writer session (play-over-play replacement): the reader must
    // still be draining, so the blocking write-end open completes immediately
    try (OutputStream reopened = new FileOutputStream(fifo.toFile())) {
      byte[] second = {9, 8, 9, 7};
      reopened.write(second);
      reopened.flush();
      assertThat(collectBytes(reader, second.length, 5_000)).isEqualTo(second);
    }
    assertThat(reader.isRunning()).isTrue();
  }

  @Test
  void survivesRepeatedEofDataCycles() throws Exception {
    pair.write.write(new byte[] {1});
    pair.write.flush();
    assertThat(collectBytes(reader, 1, 5_000)).isEqualTo(new byte[] {1});
    pair.write.close();
    await().atMost(BOUNDED).until(() -> reader.eofCount() == 1);

    try (OutputStream w2 = new FileOutputStream(fifo.toFile())) {
      w2.write(new byte[] {2, 3});
      w2.flush();
      assertThat(collectBytes(reader, 2, 5_000)).isEqualTo(new byte[] {2, 3});
    }
    await().atMost(BOUNDED).until(() -> reader.eofCount() == 2);

    try (OutputStream w3 = new FileOutputStream(fifo.toFile())) {
      w3.write(new byte[] {4, 5, 6});
      w3.flush();
      assertThat(collectBytes(reader, 3, 5_000)).isEqualTo(new byte[] {4, 5, 6});
    }
    assertThat(reader.isRunning()).isTrue();
  }

  @Test
  void neverStopsDrainingWhileConsumerIsSlow() throws Exception {
    int chunks = 200;
    int chunkBytes = 16 * 1024;
    int queueCapacity = FifoReader.DEFAULT_QUEUE_CAPACITY;

    CountDownLatch writerDone = new CountDownLatch(1);
    AtomicReference<Throwable> writerFailure = new AtomicReference<>();
    Thread writer = new Thread(() -> {
      try {
        byte[] buf = new byte[chunkBytes];
        for (int i = 0; i < chunks; i++) {
          Arrays.fill(buf, (byte) 0);
          buf[0] = (byte) i;
          buf[1] = (byte) (i >> 8);
          buf[2] = (byte) (i >> 16);
          buf[3] = (byte) (i >> 24);
          pair.write.write(buf); // OutputStream.write writes all bytes (blocking)
        }
        writerDone.countDown();
      } catch (Throwable t) {
        writerFailure.set(t);
        writerDone.countDown();
      }
    }, "test-storm-writer");
    writer.setDaemon(true);
    writer.start();

    // The consumer takes NOTHING during the storm: the reader must keep
    // draining (drop-oldest) so the writer is never blocked by the plugin.
    assertThat(writerDone.await(15, TimeUnit.SECONDS))
        .as("writer completed while the consumer took nothing — reader never stalled")
        .isTrue();
    assertThat(writerFailure.get()).isNull();
    assertThat(reader.isRunning()).isTrue();
    assertThat(reader.pendingChunks()).isLessThanOrEqualTo(queueCapacity);

    // Writer EOF, then reconstruct the index stream from the retained bytes.
    pair.write.close();
    List<Integer> indices = parseAllIndices(reader);

    assertThat(indices).isNotEmpty();
    assertThat(indices.get(indices.size() - 1)).as("newest chunk retained").isEqualTo(chunks - 1);
    assertThat(indices.get(0)).as("oldest chunks were dropped (drop-oldest)").isGreaterThan(0);
    for (int i = 1; i < indices.size(); i++) {
      assertThat(indices.get(i)).as("index stream contiguous").isEqualTo(indices.get(i - 1) + 1);
    }
    assertThat(reader.droppedChunks()).isGreaterThan(0);
  }

  @Test
  void deliversAcrossAnIdlePausedWriterWithoutEof() throws Exception {
    pair.write.write(new byte[] {1, 2});
    pair.write.flush();
    // first data delivered: the reader is alive and draining
    await().atMost(BOUNDED).until(() -> reader.pendingChunks() > 0);

    // "paused" daemon (API_CONTRACT.md §6): writer open but idle — the reader
    // must keep the read end open, emit no Eof, and not die
    assertThat(reader.eofCount()).isZero();

    pair.write.write(new byte[] {3, 4});
    pair.write.flush();
    assertThat(collectBytes(reader, 4, 5_000)).isEqualTo(new byte[] {1, 2, 3, 4});
    assertThat(reader.eofCount()).isZero();
    assertThat(reader.isRunning()).isTrue();
  }

  @Test
  void eofBoundaryLeavesNoPartialFrameWithDecoder() throws Exception {
    PcmDecoder decoder = new PcmDecoder();
    // 6 bytes = 1.5 stereo frames: L=100 R=200, then L=300 with R still pending
    byte[] partial = {(byte) 100, 0, (byte) 200, 0, (byte) 300, 0};
    pair.write.write(partial);
    pair.write.flush();
    pair.write.close(); // EOF mid-frame
    await().atMost(BOUNDED).until(() -> reader.eofCount() >= 1);

    List<Short> shorts = drainDecoded(decoder, reader, 5_000);
    assertThat(shorts).containsExactly((short) 100, (short) 200);
    assertThat(decoder.pendingBytes()).as("1–3 trailing bytes preserved across EOF").isEqualTo(2);

    // writer reopens: the two trailing bytes complete the straddling frame
    try (OutputStream reopened = new FileOutputStream(fifo.toFile())) {
      reopened.write(new byte[] {(byte) 400, 0});
      reopened.flush();
      List<Short> completed = drainDecoded(decoder, reader, 5_000);
      assertThat(completed).containsExactly((short) 300, (short) 400);
      assertThat(decoder.pendingBytes()).isZero();
    }
  }

  @Test
  void closeTerminatesReaderAndIsIdempotent() throws Exception {
    assertThat(reader.isRunning()).isTrue();

    reader.close();
    reader.close(); // idempotent

    assertThat(reader.isRunning()).isFalse();
    assertThat(reader.take(200)).isNull(); // no events produced after close
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  /**
   * Opens both ends of a fresh FIFO with the blocking-open rendezvous: the
   * read end opens on a helper thread, the write end opens on the caller's
   * thread — the two blocking opens complete each other in the kernel.
   */
  private static Rendezvous openRendezvous(Path fifo) throws Exception {
    CountDownLatch readOpened = new CountDownLatch(1);
    AtomicReference<InputStream> readRef = new AtomicReference<>();
    AtomicReference<Throwable> openError = new AtomicReference<>();
    Thread opener = new Thread(() -> {
      try {
        readRef.set(new FileInputStream(fifo.toFile()));
      } catch (Throwable t) {
        openError.set(t);
      } finally {
        readOpened.countDown();
      }
    }, "test-fifo-read-open");
    opener.setDaemon(true);
    opener.start();

    OutputStream write = new FileOutputStream(fifo.toFile());
    if (!readOpened.await(10, TimeUnit.SECONDS)) {
      write.close();
      throw new IOException("read-end open rendezvous timed out for " + fifo);
    }
    Throwable error = openError.get();
    if (error != null) {
      write.close();
      throw new IOException("read-end open failed for " + fifo, error);
    }
    return new Rendezvous(readRef.get(), write);
  }

  /** Read + write ends of one FIFO. */
  private record Rendezvous(InputStream read, OutputStream write) {}

  /**
   * Collects exactly {@code wantBytes} PCM bytes from Data events; stops
   * early on Eof (all Data events precede Eof in the FIFO queue). Returns
   * whatever arrived by the deadline — callers assert on the length.
   */
  private static byte[] collectBytes(FifoReader reader, int wantBytes, long timeoutMs)
      throws InterruptedException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (out.size() < wantBytes) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        break;
      }
      Event event = reader.take(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
      if (event instanceof Event.Data data) {
        out.writeBytes(data.bytes());
      } else if (event instanceof Event.Eof) {
        break;
      }
    }
    return out.toByteArray();
  }

  /**
   * Drains the reader until Eof (or deadline), decoding every Data event with
   * the given decoder. Eof is guaranteed to be enqueued after all Data events.
   */
  private static List<Short> drainDecoded(PcmDecoder decoder, FifoReader reader, long timeoutMs)
      throws InterruptedException {
    List<Short> shorts = new ArrayList<>();
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    while (true) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        break;
      }
      Event event = reader.take(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
      if (event == null) {
        break;
      }
      if (event instanceof Event.Data data) {
        for (short s : decoder.decode(data.bytes())) {
          shorts.add(s);
        }
      } else if (event instanceof Event.Eof) {
        break;
      }
    }
    return shorts;
  }

  /**
   * Drains the reader until Eof (or deadline), parsing 4-byte little-endian
   * index markers from the retained byte stream (chunks were written with the
   * marker in their first 4 bytes; reads may split markers arbitrarily).
   */
  private static List<Integer> parseAllIndices(FifoReader reader) throws InterruptedException {
    List<Integer> indices = new ArrayList<>();
    byte[] acc = new byte[4];
    int accLen = 0;
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (true) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        break;
      }
      Event event = reader.take(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
      if (event == null) {
        break;
      }
      if (event instanceof Event.Eof) {
        break;
      }
      for (byte b : ((Event.Data) event).bytes()) {
        acc[accLen++] = b;
        if (accLen == 4) {
          indices.add(leInt(acc));
          accLen = 0;
        }
      }
    }
    return indices;
  }

  private static int leInt(byte[] b) {
    return (b[0] & 0xFF) | ((b[1] & 0xFF) << 8) | ((b[2] & 0xFF) << 16) | ((b[3] & 0xFF) << 24);
  }
}
