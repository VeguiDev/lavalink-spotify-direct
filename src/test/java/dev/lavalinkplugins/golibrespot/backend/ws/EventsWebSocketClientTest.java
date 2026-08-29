package dev.lavalinkplugins.golibrespot.backend.ws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import dev.lavalinkplugins.golibrespot.backend.ws.EventsWebSocketClient.EventsListener;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/**
 * TDD suite for {@link EventsWebSocketClient} against the {@link FakeLibrespotDaemon}
 * (the T6 fixture; never edited). Covers connect/dispatch, tolerant parsing
 * (unknown types, unknown fields, malformed frames), reconnect + bounded
 * jittered backoff, quarantine, the generation filter hook, the stall watchdog
 * and clean close. All reconnects use a fast backoff (40ms → 120ms) and a 2s
 * stall window unless a test needs otherwise.
 */
class EventsWebSocketClientTest {

  private static final Duration BOUNDED = Duration.ofSeconds(5);

  /** Recording listener used to assert on the async dispatch path. */
  private static final class RecordingListener implements EventsListener {
    final List<PlayerEvent> events = new CopyOnWriteArrayList<>();
    final List<PlayerEvent> unknown = new CopyOnWriteArrayList<>();
    final AtomicInteger connects = new AtomicInteger();
    final AtomicInteger disconnects = new AtomicInteger();
    final AtomicInteger quarantines = new AtomicInteger();

    @Override
    public void onEvent(PlayerEvent event) {
      events.add(event);
    }

    @Override
    public void onUnknownEvent(PlayerEvent event) {
      unknown.add(event);
    }

    @Override
    public void onQuarantine() {
      quarantines.incrementAndGet();
    }

    @Override
    public void onConnected() {
      connects.incrementAndGet();
    }

    @Override
    public void onDisconnected() {
      disconnects.incrementAndGet();
    }
  }

  private static final String TRACK_URI = "spotify:track:4uLU6hMCjMI75M1A2tKUQC";

  // ------------------------------------------------------------------
  // Connect + typed dispatch
  // ------------------------------------------------------------------

  @Test
  void connectsAndDispatchesTypedEventsWithData() throws Exception {
    RecordingListener listener = new RecordingListener();
    FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    daemon.start();
    try (EventsWebSocketClient client = new EventsWebSocketClient(daemon.getWsUrl(), listener, 40, 120, 3, 2_000)) {
      client.start();
      assertThat(daemon.awaitWsClients(1, BOUNDED)).isTrue();

      daemon.emit(
          "seek",
          FakeLibrespotDaemon.seekData("spotify:playlist:ctx", TRACK_URI, 12_345, 240_000, "go-librespot"));
      daemon.emit("volume", FakeLibrespotDaemon.volumeData(50, 100));
      daemon.emit("repeat_context", FakeLibrespotDaemon.toggleData(true));

      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.events).hasSize(3));

      PlayerEvent seek = listener.events.get(0);
      assertThat(seek.type()).isEqualTo(EventType.SEEK);
      assertThat(seek.data())
          .containsEntry("position", 12_345L)
          .containsEntry("duration", 240_000L)
          .containsEntry("uri", TRACK_URI)
          .containsEntry("context_uri", "spotify:playlist:ctx")
          .containsEntry("play_origin", "go-librespot");

      PlayerEvent volume = listener.events.get(1);
      assertThat(volume.type()).isEqualTo(EventType.VOLUME);
      assertThat(volume.data()).containsEntry("value", 50L).containsEntry("max", 100L);

      PlayerEvent repeat = listener.events.get(2);
      assertThat(repeat.type()).isEqualTo(EventType.REPEAT_CONTEXT);
      assertThat(repeat.data()).containsEntry("value", true);
    } finally {
      daemon.stop();
    }
  }

  @Test
  void nullDataEventsDispatchWithNullData() throws Exception {
    RecordingListener listener = new RecordingListener();
    FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    daemon.start();
    try (EventsWebSocketClient client = new EventsWebSocketClient(daemon.getWsUrl(), listener, 40, 120, 3, 2_000)) {
      client.start();
      assertThat(daemon.awaitWsClients(1, BOUNDED)).isTrue();

      daemon.emit("playback_ready");
      daemon.emit("active");
      daemon.emit("inactive");
      daemon.emit("stopped", FakeLibrespotDaemon.stoppedData(""));

      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.events).hasSize(4));
      assertThat(listener.events.get(0).type()).isEqualTo(EventType.PLAYBACK_READY);
      assertThat(listener.events.get(0).data()).isNull();
      assertThat(listener.events.get(1).type()).isEqualTo(EventType.ACTIVE);
      assertThat(listener.events.get(1).data()).isNull();
      assertThat(listener.events.get(2).type()).isEqualTo(EventType.INACTIVE);
      assertThat(listener.events.get(2).data()).isNull();
      assertThat(listener.events.get(3).type()).isEqualTo(EventType.STOPPED);
      assertThat(listener.events.get(3).data()).containsEntry("play_origin", "");
    } finally {
      daemon.stop();
    }
  }

  // ------------------------------------------------------------------
  // Tolerant parsing
  // ------------------------------------------------------------------

  @Test
  void unknownEventTypeForwardedToDebugSinkOnly() throws Exception {
    RecordingListener listener = new RecordingListener();
    FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    daemon.start();
    try (EventsWebSocketClient client = new EventsWebSocketClient(daemon.getWsUrl(), listener, 40, 120, 3, 2_000)) {
      client.start();
      assertThat(daemon.awaitWsClients(1, BOUNDED)).isTrue();

      daemon.emit("frobnicate", "{\"opacity\":0.5}");
      daemon.emit("volume", FakeLibrespotDaemon.volumeData(7, 100));

      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.events).hasSize(1));
      assertThat(listener.unknown).hasSize(1);
      PlayerEvent unknownEvent = listener.unknown.get(0);
      assertThat(unknownEvent.type()).isEqualTo(EventType.UNKNOWN);
      assertThat(unknownEvent.data()).containsEntry("opacity", 0.5d);
      // unknown types must never reach the typed path
      assertThat(listener.events.get(0).type()).isEqualTo(EventType.VOLUME);
    } finally {
      daemon.stop();
    }
  }

  @Test
  void unknownDataFieldsAreKeptRaw() throws Exception {
    RecordingListener listener = new RecordingListener();
    FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    daemon.start();
    try (EventsWebSocketClient client = new EventsWebSocketClient(daemon.getWsUrl(), listener, 40, 120, 3, 2_000)) {
      client.start();
      assertThat(daemon.awaitWsClients(1, BOUNDED)).isTrue();

      daemon.emit("volume", "{\"value\":42,\"max\":100,\"future_flag\":true,\"nested\":{\"a\":[1,2,3]}}");

      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.events).hasSize(1));
      Map<String, Object> data = listener.events.get(0).data();
      assertThat(data)
          .containsEntry("value", 42L)
          .containsEntry("max", 100L)
          .containsEntry("future_flag", true);
      @SuppressWarnings("unchecked")
      Map<String, Object> nested = (Map<String, Object>) data.get("nested");
      assertThat(nested).containsEntry("a", List.of(1L, 2L, 3L));
    } finally {
      daemon.stop();
    }
  }

  @Test
  void malformedFramesAreCountedAndConnectionSurvives() throws Exception {
    RecordingListener listener = new RecordingListener();
    FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    daemon.start();
    try (EventsWebSocketClient client = new EventsWebSocketClient(daemon.getWsUrl(), listener, 40, 120, 3, 2_000)) {
      client.start();
      assertThat(daemon.awaitWsClients(1, BOUNDED)).isTrue();

      daemon.emit("volume", "{oops"); // truncated object → malformed
      daemon.emit("volume", "42"); // data present but not an object → malformed
      daemon.emit("volume", FakeLibrespotDaemon.volumeData(50, 100)); // still alive after malformed frames

      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.events).hasSize(1));
      assertThat(client.getMalformedFrames()).isEqualTo(2);
      assertThat(listener.events.get(0).type()).isEqualTo(EventType.VOLUME);
      assertThat(listener.events.get(0).data()).containsEntry("value", 50L);

      // connection not dropped: further events keep flowing
      daemon.emit("seek", FakeLibrespotDaemon.seekData(null, TRACK_URI, 1, 2, "go-librespot"));
      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.events).hasSize(2));
      assertThat(client.getMalformedFrames()).isEqualTo(2); // still 2 — the new frame is valid
    } finally {
      daemon.stop();
    }
  }

  // ------------------------------------------------------------------
  // Reconnect + backoff + quarantine
  // ------------------------------------------------------------------

  @Test
  void reconnectsAfterWsLossAndResumesDelivery() throws Exception {
    RecordingListener listener = new RecordingListener();
    FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    daemon.start();
    try (EventsWebSocketClient client = new EventsWebSocketClient(daemon.getWsUrl(), listener, 40, 120, 3, 2_000)) {
      client.start();
      assertThat(daemon.awaitWsClients(1, BOUNDED)).isTrue();
      // onConnected fires on the manager thread — may lag the server-side accept
      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.connects).hasValue(1));

      daemon.dropWsClients();

      await().atMost(BOUNDED)
          .untilAsserted(
              () -> assertThat(listener.connects.get()).as("reconnect after WS loss").isEqualTo(2));
      assertThat(listener.disconnects).hasValue(1);

      // fresh connection delivers again
      daemon.emit("volume", FakeLibrespotDaemon.volumeData(10, 100));
      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.events).hasSize(1));
      assertThat(listener.events.get(0).data()).containsEntry("value", 10L);
      assertThat(client.isQuarantined()).isFalse();
    } finally {
      daemon.stop();
    }
  }

  @Test
  void successfulReconnectResetsConsecutiveFailureCounter() throws Exception {
    RecordingListener listener = new RecordingListener();
    FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    daemon.start();
    try (EventsWebSocketClient client = new EventsWebSocketClient(daemon.getWsUrl(), listener, 30, 60, 3, 2_000)) {
      client.start();
      assertThat(daemon.awaitWsClients(1, BOUNDED)).isTrue();

      // three loss cycles; every successful reconnect resets the counter, so
      // with threshold 3 the backend must never reach quarantine
      for (int i = 0; i < 3; i++) {
        final int step = i + 1;
        final int expectedConnects = 2 + i;
        daemon.dropWsClients();
        await().atMost(BOUNDED)
            .untilAsserted(
                () -> assertThat(listener.connects.get())
                    .as("reconnect #" + step)
                    .isEqualTo(expectedConnects));
      }

      assertThat(listener.quarantines).hasValue(0);
      assertThat(client.isQuarantined()).isFalse();
      // give a spurious quarantine/extra failure time to surface — none may appear
      Thread.sleep(300);
      assertThat(listener.quarantines).hasValue(0);
      assertThat(client.isQuarantined()).isFalse();
    } finally {
      daemon.stop();
    }
  }

  @Test
  void quarantinesAfterConsecutiveConnectFailures() throws Exception {
    RecordingListener listener = new RecordingListener();
    // WS disabled → every connect attempt is refused; threshold 3
    FakeLibrespotDaemon daemon = new FakeLibrespotDaemon().wsEnabled(false);
    daemon.start();
    try (EventsWebSocketClient client =
        new EventsWebSocketClient(
            "ws://127.0.0.1:" + daemon.getPort() + "/events", listener, 20, 40, 3, 2_000)) {
      client.start();

      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.quarantines).hasValue(1));
      assertThat(client.isQuarantined()).isTrue();
      assertThat(client.getConnectAttempts()).isEqualTo(3);
      assertThat(listener.connects).hasValue(0);

      // no reconnect attempts may happen after quarantine
      long attemptsAfterQuarantine = client.getConnectAttempts();
      Thread.sleep(300);
      assertThat(client.getConnectAttempts()).isEqualTo(attemptsAfterQuarantine);
    } finally {
      daemon.stop();
    }
  }

  @Test
  void backoffDelayIsBoundedCappedAndJittered() {
    long initial = 1_000;
    long max = 30_000;

    assertThat(EventsWebSocketClient.backoffBaseMs(1, initial, max)).isEqualTo(1_000);
    assertThat(EventsWebSocketClient.backoffBaseMs(2, initial, max)).isEqualTo(2_000);
    assertThat(EventsWebSocketClient.backoffBaseMs(3, initial, max)).isEqualTo(4_000);
    assertThat(EventsWebSocketClient.backoffBaseMs(5, initial, max)).isEqualTo(16_000);
    assertThat(EventsWebSocketClient.backoffBaseMs(100, initial, max)).isEqualTo(30_000);
    assertThat(EventsWebSocketClient.backoffBaseMs(1, 10_000, 30_000)).isEqualTo(10_000);

    // jitter stays within ±20% of the base
    for (int i = 0; i < 200; i++) {
      assertThat(EventsWebSocketClient.jitterDelayMs(1_000)).isBetween(800L, 1_199L);
    }
    assertThat(EventsWebSocketClient.jitterDelayMs(1)).isBetween(1L, 1L);
  }

  // ------------------------------------------------------------------
  // Generation filter
  // ------------------------------------------------------------------

  @Test
  void generationFilterDropsEventsFromStaleConnection() throws Exception {
    RecordingListener listener = new RecordingListener();
    AtomicLong generation = new AtomicLong(5);
    FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    daemon.start();
    try (EventsWebSocketClient client = new EventsWebSocketClient(daemon.getWsUrl(), listener, 40, 120, 3, 2_000)) {
      client.setGenerationSupplier(generation::get);
      client.start();
      assertThat(daemon.awaitWsClients(1, BOUNDED)).isTrue();

      // current generation (5) >= connection generation (5) → forwarded
      daemon.emit("volume", FakeLibrespotDaemon.volumeData(1, 100));
      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.events).hasSize(1));

      // generation advances (new lease) → this connection is stale → dropped at the boundary
      generation.set(6);
      daemon.emit("volume", FakeLibrespotDaemon.volumeData(2, 100));
      Thread.sleep(300);
      assertThat(listener.events).hasSize(1);
      assertThat(client.getDroppedByGeneration()).isEqualTo(1);

      // a fresh connection captures the new generation → forwards again
      daemon.dropWsClients();
      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.connects.get()).isEqualTo(2));
      daemon.emit("volume", FakeLibrespotDaemon.volumeData(3, 100));
      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.events).hasSize(2));
      assertThat(listener.events.get(1).data()).containsEntry("value", 3L);
    } finally {
      daemon.stop();
    }
  }

  // ------------------------------------------------------------------
  // Stall watchdog
  // ------------------------------------------------------------------

  @Test
  void heartbeatKeepsSilentConnectionOpen() throws Exception {
    RecordingListener listener = new RecordingListener();
    FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    daemon.start();
    // A tiny idle window exercises multiple PING/PONG heartbeat cycles.
    try (EventsWebSocketClient client =
        new EventsWebSocketClient(daemon.getWsUrl(), listener, 30, 60, 100, 300)) {
      client.start();
      assertThat(daemon.awaitWsClients(1, BOUNDED)).isTrue();
      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.connects).hasValue(1));

      Thread.sleep(1_200);
      assertThat(listener.disconnects).hasValue(0);
      assertThat(listener.connects).hasValue(1);

      daemon.emit("volume", FakeLibrespotDaemon.volumeData(9, 100));
      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.events).hasSize(1));
      assertThat(client.isQuarantined()).isFalse();
    } finally {
      daemon.stop();
    }
  }

  // ------------------------------------------------------------------
  // Clean close
  // ------------------------------------------------------------------

  @Test
  void cleanCloseIsIdempotentAndStopsReconnect() throws Exception {
    RecordingListener listener = new RecordingListener();
    FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    daemon.start();
    EventsWebSocketClient client = new EventsWebSocketClient(daemon.getWsUrl(), listener, 40, 120, 3, 2_000);
    client.start();
    assertThat(daemon.awaitWsClients(1, BOUNDED)).isTrue();
    daemon.emit("volume", FakeLibrespotDaemon.volumeData(1, 100));
    await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.events).hasSize(1));

    client.close();
    assertThat(client.isClosed()).isTrue();
    client.close(); // idempotent

    // daemon-side loss after close must NOT trigger a reconnect
    long attemptsBefore = client.getConnectAttempts();
    daemon.dropWsClients();
    daemon.emit("volume", FakeLibrespotDaemon.volumeData(2, 100));
    Thread.sleep(300);
    assertThat(listener.connects).hasValue(1);
    assertThat(listener.events).hasSize(1);
    assertThat(client.getConnectAttempts()).isEqualTo(attemptsBefore);
    daemon.stop();
  }

  // ------------------------------------------------------------------
  // Drain (no socket back-pressure)
  // ------------------------------------------------------------------

  @Test
  void rapidEventBurstIsDrainedCompletely() throws Exception {
    RecordingListener listener = new RecordingListener();
    FakeLibrespotDaemon daemon = new FakeLibrespotDaemon();
    daemon.start();
    try (EventsWebSocketClient client = new EventsWebSocketClient(daemon.getWsUrl(), listener, 40, 120, 3, 2_000)) {
      client.start();
      assertThat(daemon.awaitWsClients(1, BOUNDED)).isTrue();

      for (int i = 0; i < 300; i++) {
        daemon.emit("volume", FakeLibrespotDaemon.volumeData(i, 100));
      }

      await().atMost(BOUNDED).untilAsserted(() -> assertThat(listener.events).hasSize(300));
      assertThat(listener.events.get(299).data()).containsEntry("value", 299L);
      assertThat(client.getMalformedFrames()).isZero();
    } finally {
      daemon.stop();
    }
  }
}
