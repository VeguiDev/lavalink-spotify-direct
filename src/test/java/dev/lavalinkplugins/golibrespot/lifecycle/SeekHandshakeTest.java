package dev.lavalinkplugins.golibrespot.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.lavalinkplugins.golibrespot.backend.rest.GoLibrespotRestClient;
import dev.lavalinkplugins.golibrespot.backend.ws.EventsWebSocketClient;
import dev.lavalinkplugins.golibrespot.config.BackendConfig;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
import dev.lavalinkplugins.golibrespot.fifo.FifoReader;
import dev.lavalinkplugins.golibrespot.fifo.FifoTestUtil;
import dev.lavalinkplugins.golibrespot.fifo.PcmDecoder;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.MachineState;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Outcome;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Phase;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Result;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine.Timing;
import dev.lavalinkplugins.golibrespot.lifecycle.SeekHandshake.Drain;
import dev.lavalinkplugins.golibrespot.lifecycle.SeekHandshake.DrainOutcome;
import dev.lavalinkplugins.golibrespot.pool.BackendHandle;
import dev.lavalinkplugins.golibrespot.pool.BackendState;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import dev.lavalinkplugins.golibrespot.pool.Lease;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon.RecordedCommand;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon.Response;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

/**
 * Contract + acceptance tests for {@link SeekHandshake} (T16) against the
 * {@link FakeLibrespotDaemon} fixture (T6), the real REST client (T8), the real
 * reconnecting WS client (T9), the real pool (T13) and the real state machine
 * (T14).
 *
 * <p>The seek handshake must run the strict order <b>remote pause → drain +
 * discard pre-seek PCM → clear partial remainder → absolute seek → signal
 * pipeline → resume (iff desired state is playing)</b> and must NEVER issue a
 * bare {@code /player/seek}: every seek is gated by the pause + drain barrier.
 * Any failure at any step aborts the track and quarantines the backend (the
 * machine owns the quarantine; the handshake never double-quarantines).</p>
 *
 * <p>The audio plane is exercised in two ways: pure-JVM tests drive the real
 * {@link FifoReader} + {@link dev.lavalinkplugins.golibrespot.fifo.PcmDecoder}
 * over a scripted {@link PipedInputStream} (runs everywhere), while a
 * Linux-gated test drives a real mkfifo FIFO (skipped by design on Windows).</p>
 *
 * <p>Scenarios: exact pause→seek→resume call order; pre-seek golden PCM is
 * never delivered after resume (the drain discards it and the decoder remainder
 * is cleared at the seek boundary); drain caps (byte + time) terminate an
 * endless-data reader; seek-ack mismatch → abort + quarantine; seek while
 * paused → no spurious resume; two rapid seeks serialized; pause timeout →
 * abort + quarantine with no bare seek.</p>
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class SeekHandshakeTest {

  private static final String URI = "spotify:track:aaaaaaaaaaaaaaaaaaaaaa";
  private static final long SEEK_POS = 60_000L;

  /** Short timings so every barrier/timeout test stays snappy and deterministic. */
  private static final Timing FAST =
      new Timing(800, 600, 1000, 800, 80, 5);
  /** Fast drain budgets: 1 s time cap, 64 KiB byte cap, 40 ms quiet window. */
  private static final Drain DRAIN_FAST = new Drain(1_000, 64 * 1024, 40);

  private final List<Rig> rigs = new ArrayList<>();

  @AfterEach
  void tearDown() {
    for (Rig rig : rigs) {
      rig.close();
    }
    rigs.clear();
  }

  // ==================================================================
  // Happy path: exact call order
  // ==================================================================

  @Test
  void seekIssuesPauseThenSeekThenResumeInExactOrder() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.scriptHappy(SEEK_POS);
      rig.activate();

      Result result = rig.handshake.seek(SEEK_POS, true);

      assertThat(result.isOk()).as("seek result: " + result).isTrue();
      assertThat(rig.machine.phase()).isEqualTo(Phase.PLAYING);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);
      assertThat(rig.seekPerformed).as("seek-performed hook fired once").hasSize(1);

      List<RecordedCommand> posts = posts(rig.daemon.getReceivedCommands());
      assertThat(postPaths(posts))
          .as("strict order: pause before seek before resume — never a bare seek")
          .containsExactly("/player/play", "/player/pause", "/player/seek", "/player/resume");
      assertThat(posts).filteredOn(c -> c.path().equals("/player/stop")).isEmpty();
      RecordedCommand seek =
          posts.stream().filter(c -> c.path().equals("/player/seek")).findFirst().orElseThrow();
      assertThat(seek.body()).isEqualTo(FakeLibrespotDaemon.seekRequestJson(SEEK_POS, false));
    }
  }

  // ==================================================================
  // Pre-seek PCM is never delivered after resume
  // ==================================================================

  @Test
  void preSeekPcmNeverDeliveredAfterResume() throws Exception {
    try (Rig rig = newRig()) {
      PipedOutputStream out = rig.newPipe();
      rig.scriptHappy(SEEK_POS);
      rig.activate();

      short[] pre = golden(800, 0);
      out.write(pcmBytes(pre));
      out.flush();
      await().atMost(Duration.ofSeconds(2)).until(() -> rig.reader.pendingChunks() > 0);

      Result result = rig.handshake.seek(SEEK_POS, true);
      assertThat(result.isOk()).as("seek over queued pre-seek PCM: " + result).isTrue();
      assertThat(rig.decoder.pendingBytes())
          .as("partial-frame remainder cleared at the seek boundary").isZero();

      short[] post = golden(800, 100_000);
      out.write(pcmBytes(post));
      out.flush();

      short[] decoded = drainDecoded(rig, post.length, Duration.ofSeconds(3));
      assertThat(decoded).as("only post-seek frames are decoded — pre-seek PCM never delivered")
          .isEqualTo(post);
    }
  }

  // ==================================================================
  // Drain caps
  // ==================================================================

  @Test
  void drainTerminatesAtTheByteCapOnEndlessData() throws Exception {
    try (Rig rig = newRig()) {
      rig.attachEndlessReader();
      SeekHandshake handshake = rig.newHandshake(new Drain(2_000, 4 * 1024, 40));

      long start = System.nanoTime();
      DrainOutcome outcome = handshake.drain();
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

      assertThat(outcome).isEqualTo(DrainOutcome.BYTE_CAP);
      assertThat(elapsedMs).as("byte cap terminates the drain promptly").isLessThan(1_500);
    }
  }

  @Test
  void drainTerminatesAtTheTimeCapOnEndlessData() throws Exception {
    try (Rig rig = newRig()) {
      rig.attachEndlessReader();
      // byte cap far beyond what the in-memory endless stream can reach within the
      // time cap (~3 GB/s) — the TIME_CAP must terminate the drain
      SeekHandshake handshake = rig.newHandshake(new Drain(300, 1_000_000_000_000L, 40));

      long start = System.nanoTime();
      DrainOutcome outcome = handshake.drain();
      long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

      assertThat(outcome).isEqualTo(DrainOutcome.TIME_CAP);
      assertThat(elapsedMs).as("time cap terminates the drain within its bound")
          .isGreaterThanOrEqualTo(200).isLessThan(1_500);
    }
  }

  // ==================================================================
  // Seek-ack mismatch → abort + quarantine
  // ==================================================================

  @Test
  void seekAckMismatchAbortsTrackAndQuarantinesWithoutResume() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.seekAckMismatch(true); // every seek also emits a seek event 5 s ahead
      rig.scriptHappy(SEEK_POS);
      rig.daemon.seek(Response.ok()); // drop the matching seek event — only the mismatch remains
      rig.activate();

      Result result = rig.handshake.seek(SEEK_POS, true);

      assertThat(result.outcome()).as("mismatch surfaces as quarantine: " + result)
          .isEqualTo(Outcome.QUARANTINED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.QUARANTINING);
      assertThat(rig.seekPerformed).as("no seek-performed signal on an aborted seek").isEmpty();

      List<RecordedCommand> posts = posts(rig.daemon.getReceivedCommands());
      assertThat(postPaths(posts)).containsExactly("/player/play", "/player/pause", "/player/seek");
      assertThat(posts).filteredOn(c -> c.path().equals("/player/resume")).isEmpty();
    }
  }

  // ==================================================================
  // Seek while paused → no spurious resume
  // ==================================================================

  @Test
  void seekWhilePausedDoesNotResume() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.scriptHappy(SEEK_POS);
      rig.activate();

      // the user pauses first (remote pause + paused event) → PAUSE_CONFIRMED
      assertThat(rig.machine.pause().get(5, TimeUnit.SECONDS).isOk()).isTrue();
      assertThat(rig.machine.phase()).isEqualTo(Phase.PAUSE_CONFIRMED);

      // seek with the desired player state paused → the handshake must NOT resume
      Result result = rig.handshake.seek(SEEK_POS, false);

      assertThat(result.isOk()).as("seek while paused: " + result).isTrue();
      assertThat(rig.machine.phase()).isEqualTo(Phase.PAUSE_CONFIRMED);
      assertThat(rig.seekPerformed).as("seek-performed fired even without a resume").hasSize(1);

      List<RecordedCommand> posts = posts(rig.daemon.getReceivedCommands());
      assertThat(postPaths(posts)).containsExactly("/player/play", "/player/pause", "/player/seek");
      assertThat(posts).filteredOn(c -> c.path().equals("/player/resume")).isEmpty();
    }
  }

  // ==================================================================
  // Two rapid seeks are serialized
  // ==================================================================

  @Test
  void twoRapidSeeksAreSerialized() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.scriptHappy(SEEK_POS);
      rig.activate();

      AtomicReference<Result> first = new AtomicReference<>();
      AtomicReference<Result> second = new AtomicReference<>();
      Thread t1 = new Thread(() -> {
        try {
          first.set(rig.handshake.seek(SEEK_POS, true));
        } catch (Throwable t) {
          first.set(Result.failed("thread 1: " + t));
        }
      }, "seek-handshake-1");
      Thread t2 = new Thread(() -> {
        try {
          second.set(rig.handshake.seek(SEEK_POS, true));
        } catch (Throwable t) {
          second.set(Result.failed("thread 2: " + t));
        }
      }, "seek-handshake-2");
      t1.start();
      t2.start();
      t1.join(15_000);
      t2.join(15_000);

      assertThat(first.get().isOk()).as("first seek: " + first.get()).isTrue();
      assertThat(second.get().isOk()).as("second seek waits for the first: " + second.get()).isTrue();
      assertThat(rig.machine.phase()).isEqualTo(Phase.PLAYING);
      assertThat(rig.seekPerformed).hasSize(2);

      // serialized: each seek runs its full handshake (pause → seek → resume) with
      // no interleaving and no "command in flight" failures — the second seek only
      // starts once the first fully completed (machine PLAYING again)
      List<RecordedCommand> posts = posts(rig.daemon.getReceivedCommands());
      assertThat(postPaths(posts)).containsExactly(
          "/player/play", "/player/pause", "/player/seek", "/player/resume",
          "/player/pause", "/player/seek", "/player/resume");
    }
  }

  // ==================================================================
  // Pause failure → abort + quarantine, never a bare seek
  // ==================================================================

  @Test
  void pauseTimeoutAbortsAndQuarantinesWithoutIssuingSeek() throws Exception {
    try (Rig rig = newRig()) {
      rig.newPipe();
      rig.daemon.play(Response.ok().emit("playing", playingData(URI)));
      rig.daemon.status(Response.ok(playingStatus(URI)));
      rig.daemon.pause(Response.ok()); // 200 but never paused, and /status never reports paused
      rig.activate();

      Result result = rig.handshake.seek(SEEK_POS, true);

      assertThat(result.outcome()).as("pause timeout surfaces as quarantine: " + result)
          .isEqualTo(Outcome.QUARANTINED);
      assertThat(rig.machine.state()).isEqualTo(MachineState.QUARANTINING);
      assertThat(rig.pool.stateOf("alpha")).isEqualTo(BackendState.QUARANTINING);

      List<RecordedCommand> posts = posts(rig.daemon.getReceivedCommands());
      assertThat(postPaths(posts)).as("aborted at the pause barrier — no bare seek")
          .containsExactly("/player/play", "/player/pause");
      assertThat(posts).filteredOn(c -> c.path().equals("/player/seek")).isEmpty();
    }
  }

  // ==================================================================
  // Real FIFO (Linux only)
  // ==================================================================

  @EnabledOnOs(OS.LINUX)
  @Test
  void realFifoDiscardsPreSeekPcmAndDeliversOnlyPostSeekFrames() throws Exception {
    FifoTestUtil.requireMkfifo();
    Path fifo = FifoTestUtil.createTempFifo();
    try (Rig rig = newRig()) {
      Rendezvous pair = openRendezvous(fifo);
      rig.attachReader(pair.read);
      rig.scriptHappy(SEEK_POS);
      rig.activate();

      short[] pre = golden(1600, 0);
      pair.write.write(pcmBytes(pre));
      pair.write.flush();
      await().atMost(Duration.ofSeconds(3)).until(() -> rig.reader.pendingChunks() > 0);

      Result result = rig.handshake.seek(SEEK_POS, true);
      assertThat(result.isOk()).as("real-fifo seek: " + result).isTrue();

      short[] post = golden(1600, 50_000);
      pair.write.write(pcmBytes(post));
      pair.write.flush();
      short[] decoded = drainDecoded(rig, post.length, Duration.ofSeconds(3));
      assertThat(decoded).isEqualTo(post);
      pair.write.close();
    } finally {
      FifoTestUtil.deleteTempFifo(fifo);
    }
  }

  // ==================================================================
  // helpers
  // ==================================================================

  private static String playingData(String uri) {
    return FakeLibrespotDaemon.playingData("", uri, false, "go-librespot");
  }

  private static String playingDataResume(String uri) {
    return FakeLibrespotDaemon.playingData("", uri, true, "go-librespot");
  }

  private static String sharedData(String uri) {
    return FakeLibrespotDaemon.sharedTrackData("", uri, "go-librespot");
  }

  private static String seekData(String uri, long position) {
    return FakeLibrespotDaemon.seekData("", uri, position, 240_000, "go-librespot");
  }

  private static String playingStatus(String uri) {
    return FakeLibrespotDaemon.statusJson(false, false,
        FakeLibrespotDaemon.trackJson(uri, "Track", List.of("Artist"), "Album", "",
            0, 240_000, "", 1, 1, "OGG_VORBIS_160", "vorbis", 160, 44100, null));
  }

  /** A deterministic golden L/R sample sequence (frame-aligned, s16le-encodable). */
  private static short[] golden(int count, int base) {
    short[] out = new short[count];
    for (int i = 0; i < count; i++) {
      out[i] = (short) ((i % 2 == 0) ? base + i : -(base + i));
    }
    return out;
  }

  private static byte[] pcmBytes(short[] shorts) {
    ByteBuffer bb = ByteBuffer.allocate(shorts.length * 2).order(ByteOrder.LITTLE_ENDIAN);
    for (short s : shorts) {
      bb.putShort(s);
    }
    return bb.array();
  }

  /** Accumulates decoded shorts until {@code expectedShorts} (or the timeout). */
  private static short[] drainDecoded(Rig rig, int expectedShorts, Duration timeout) throws Exception {
    List<Short> acc = new ArrayList<>();
    long deadline = System.nanoTime() + timeout.toNanos();
    while (acc.size() < expectedShorts && System.nanoTime() < deadline) {
      FifoReader.Event event = rig.reader.take(100);
      if (event instanceof FifoReader.Event.Data data) {
        short[] frames = rig.decoder.decode(data.bytes());
        for (short s : frames) {
          acc.add(s);
        }
      }
    }
    short[] out = new short[acc.size()];
    for (int i = 0; i < acc.size(); i++) {
      out[i] = acc.get(i);
    }
    return out;
  }

  private static List<RecordedCommand> posts(List<RecordedCommand> commands) {
    return commands.stream().filter(c -> c.method().equals("POST")).collect(Collectors.toList());
  }

  private static List<String> postPaths(List<RecordedCommand> commands) {
    return commands.stream().map(RecordedCommand::path).collect(Collectors.toList());
  }

  /**
   * Opens both ends of a fresh FIFO with the blocking-open rendezvous (the
   * private-helper pattern from FifoReaderTest): the read end opens on a helper
   * thread, the write end opens on the caller's thread — the two blocking opens
   * complete each other in the kernel.
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

  /** An InputStream that never ends and always returns data (drain-cap tests). */
  static final class EndlessInputStream extends InputStream {
    @Override
    public int read() {
      return 0;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      return len;
    }
  }

  private Rig newRig() {
    Rig rig = new Rig();
    rig.start();
    rigs.add(rig);
    return rig;
  }

  /** The full stack under test: fixture daemon + real REST + real WS + pool + machine + handshake. */
  static final class Rig implements AutoCloseable {

    final FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    final AtomicBoolean fifoReopenOk = new AtomicBoolean(true);
    final List<String> logLines = Collections.synchronizedList(new ArrayList<>());
    final List<String> seekPerformed = Collections.synchronizedList(new ArrayList<>());

    GoLibrespotConfig config;
    BackendConfig backend;
    GoLibrespotRestClient rest;
    ExclusivePool pool;
    EventsWebSocketClient ws;
    BackendStateMachine machine;
    Lease lease;
    FifoReader reader;
    final PcmDecoder decoder = new PcmDecoder();
    SeekHandshake handshake;

    private PipedOutputStream pipeOut;

    void start() {
      try {
        daemon.start();
        config = GoLibrespotConfig.from(Map.of(
            "enabled", true,
            "backends", List.of(Map.of(
                "name", "alpha",
                "restBaseUrl", daemon.getHttpUrl(),
                "wsUrl", daemon.getWsUrl(),
                "fifoPath", "C:/tmp/alpha.fifo"))));
        backend = config.getBackends().get(0);
        pool = new ExclusivePool(config.getBackends());
        rest = new GoLibrespotRestClient(backend.getRestBaseUrl(), 1500);
        machine = new BackendStateMachine(
            BackendHandle.of(backend), rest, pool, FAST, fifoReopenOk::get,
            new BackendStateMachine.LifecycleListener() {}, logLines::add);
        ws = new EventsWebSocketClient(backend.getWsUrl(), machine.eventsListener(), 40, 200, 5);
        machine.attachWebSocket(ws);
        ws.start();
        assertThat(daemon.awaitWsClients(1, Duration.ofSeconds(5))).isTrue();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    /** Creates a fresh PCM pipe and points the reader + handshake at it. */
    PipedOutputStream newPipe() throws IOException {
      PipedInputStream in = new PipedInputStream(64 * 1024);
      PipedOutputStream out = new PipedOutputStream(in);
      reader = new FifoReader(in);
      reader.start();
      handshake = newHandshake(DRAIN_FAST);
      pipeOut = out;
      return out;
    }

    /** Points the reader + handshake at an already-open FIFO read-end. */
    void attachReader(InputStream readEnd) {
      reader = new FifoReader(readEnd);
      reader.start();
      handshake = newHandshake(DRAIN_FAST);
    }

    /** Replaces the reader with one over an endless stream (drain-cap tests). */
    void attachEndlessReader() {
      reader = new FifoReader(new EndlessInputStream());
      reader.start();
    }

    SeekHandshake newHandshake(Drain drain) {
      return new SeekHandshake(
          machine, reader, decoder, FAST, drain, () -> seekPerformed.add("seek"), logLines::add);
    }

    /** Scripts the happy path for one absolute seek position. */
    void scriptHappy(long seekPosition) {
      daemon.play(Response.ok().emit("playing", playingData(URI)));
      daemon.status(Response.ok(playingStatus(URI)));
      daemon.pause(Response.ok().emit("paused", sharedData(URI)));
      daemon.seek(Response.ok().emit("seek", seekData(URI, seekPosition)));
      daemon.resume(Response.ok().emit("playing", playingDataResume(URI)));
    }

    /** Acquires a lease and activates the machine (playing event + playing /status). */
    void activate() throws Exception {
      lease = pool.acquire(Duration.ofSeconds(5)).orElseThrow();
      Result r = machine.activate(lease, URI, 0).get(5, TimeUnit.SECONDS);
      assertThat(r.isOk()).as("activation: " + r).isTrue();
    }

    @Override
    public void close() {
      try {
        if (pipeOut != null) {
          pipeOut.close();
        }
      } catch (IOException ignored) {
        // best-effort: unblocks the reader
      }
      if (reader != null) {
        reader.close();
      }
      if (machine != null) {
        machine.close();
      }
      if (ws != null) {
        ws.close();
      }
      if (rest != null) {
        rest.close();
      }
      if (pool != null) {
        pool.shutdown();
      }
      daemon.stop();
    }
  }
}
