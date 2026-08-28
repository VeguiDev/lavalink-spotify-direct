package dev.lavalinkplugins.golibrespot.testfixtures;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon.RecordedCommand;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon.Response;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Self-test for {@link FakeLibrespotDaemon}. Runs entirely on loopback with the
 * JDK {@link HttpClient} and Java-WebSocket's client — no network permissions
 * beyond localhost, plain JUnit 5.
 */
class FakeLibrespotDaemonTest {

  private static final Duration BOUNDED = Duration.ofSeconds(5);

  private final HttpClient http =
      HttpClient.newBuilder().connectTimeout(BOUNDED).build();
  private FakeLibrespotDaemon daemon;
  private final List<WebSocketClient> wsClients = new CopyOnWriteArrayList<>();

  @AfterEach
  void tearDown() {
    for (WebSocketClient client : wsClients) {
      try {
        client.close();
      } catch (Exception ignored) {
        // already closed
      }
    }
    wsClients.clear();
    if (daemon != null) {
      daemon.stop();
    }
  }

  // ------------------------------------------------------------------
  // Starts / parks / serves scripted bodies
  // ------------------------------------------------------------------

  @Test
  void startsParksServingRootAndDefaultStatus() throws Exception {
    daemon = new FakeLibrespotDaemon();
    daemon.start();

    assertThat(daemon.getPort()).isGreaterThan(0);
    assertThat(daemon.getWsPort()).isGreaterThan(0);

    HttpResponse<String> root = send(HttpRequest.newBuilder(uri("/")).GET().build());
    assertThat(root.statusCode()).isEqualTo(200);
    assertThat(root.body()).contains("\"playback_ready\":true");

    HttpResponse<String> status = send(HttpRequest.newBuilder(uri("/status")).GET().build());
    assertThat(status.statusCode()).isEqualTo(200);
    assertThat(status.body()).contains("\"stopped\":true").contains("\"track\":null");
  }

  @Test
  void noSessionStatusIs204NoContent() throws Exception {
    daemon = new FakeLibrespotDaemon().hasSession(false);
    daemon.start();

    HttpResponse<String> status = send(HttpRequest.newBuilder(uri("/status")).GET().build());
    assertThat(status.statusCode()).isEqualTo(204);
    assertThat(status.body()).isEmpty();

    // /web-api also answers 204 without a session
    HttpResponse<String> webApi = send(HttpRequest.newBuilder(uri("/web-api/v1/tracks/x")).GET().build());
    assertThat(webApi.statusCode()).isEqualTo(204);
  }

  @Test
  void servesScriptedStatusBodyExactly() throws Exception {
    String track =
        FakeLibrespotDaemon.trackJson(
            "spotify:track:4uLU6hMCjMI75M1A2tKUQC",
            "Track name",
            List.of("Artist A", "Artist B"),
            "Album",
            null,
            12_345,
            240_000,
            "2020-01-01",
            3,
            1,
            "OGG_VORBIS_160",
            "vorbis",
            160,
            44_100,
            null);
    String body = FakeLibrespotDaemon.statusJson(false, true, track);

    daemon = new FakeLibrespotDaemon().status(Response.of(200, body));
    daemon.start();

    HttpResponse<String> status = send(HttpRequest.newBuilder(uri("/status")).GET().build());
    assertThat(status.statusCode()).isEqualTo(200);
    assertThat(status.body()).isEqualTo(body);
  }

  // ------------------------------------------------------------------
  // REST command recording (exact order)
  // ------------------------------------------------------------------

  @Test
  void recordsReceivedCommandsInOrderWithBodies() throws Exception {
    daemon = new FakeLibrespotDaemon();
    daemon.start();

    send(post("/player/play", FakeLibrespotDaemon.playRequestJson("spotify:track:abc", 0L, false, null)));
    send(post("/player/pause", ""));

    List<RecordedCommand> commands = daemon.awaitCommands(2, BOUNDED);
    assertThat(commands)
        .extracting(RecordedCommand::method, RecordedCommand::path)
        .containsExactly(
            org.assertj.core.groups.Tuple.tuple("POST", "/player/play"),
            org.assertj.core.groups.Tuple.tuple("POST", "/player/pause"));
    assertThat(commands.get(0).body()).contains("\"uri\":\"spotify:track:abc\"").contains("\"position\":0");
    assertThat(commands.get(1).body()).isEmpty();
  }

  @Test
  void playerStopIsNeverImplemented() throws Exception {
    daemon = new FakeLibrespotDaemon();
    daemon.start();

    HttpResponse<String> stop = send(post("/player/stop", ""));
    assertThat(stop.statusCode()).isEqualTo(404);
    // ... and it is recorded, so "never invoked" is assertable from the command log
    assertThat(daemon.getReceivedCommands())
        .extracting(RecordedCommand::path)
        .containsExactly("/player/stop");
  }

  // ------------------------------------------------------------------
  // WebSocket scripting
  // ------------------------------------------------------------------

  @Test
  void emitsScriptedWsSequenceToConnectedClient() throws Exception {
    daemon = new FakeLibrespotDaemon();
    daemon.start();
    RecordingWsClient client = connectWs();

    daemon.emit("playback_ready");
    daemon.emit("playing", FakeLibrespotDaemon.playingData("spotify:playlist:x", "spotify:track:abc", false, "go-librespot"));

    assertThat(client.frames.poll(5, TimeUnit.SECONDS))
        .isEqualTo(FakeLibrespotDaemon.frame("playback_ready", null));
    String playing = client.frames.poll(5, TimeUnit.SECONDS);
    assertThat(playing)
        .contains("\"type\":\"playing\"")
        .contains("\"resume\":false")
        .contains("\"uri\":\"spotify:track:abc\"");
  }

  @Test
  void restCommandCanTriggerEmission() throws Exception {
    daemon =
        new FakeLibrespotDaemon()
            .play(
                Response.ok().emit("playing", FakeLibrespotDaemon.playingData("", "spotify:track:abc", false, "go-librespot")));
    daemon.start();
    RecordingWsClient client = connectWs();

    send(post("/player/play", FakeLibrespotDaemon.playRequestJson("spotify:track:abc", null, null, null)));

    assertThat(client.frames.poll(5, TimeUnit.SECONDS))
        .contains("\"type\":\"playing\"");
  }

  @Test
  void okResponseWithNoEventsIsA200ButNoop() throws Exception {
    daemon = new FakeLibrespotDaemon().play(Response.ok());
    daemon.start();
    RecordingWsClient client = connectWs();

    HttpResponse<String> play = send(post("/player/play", FakeLibrespotDaemon.playRequestJson("spotify:track:abc", null, null, null)));

    assertThat(play.statusCode()).isEqualTo(200);
    assertThat(client.frames.poll(500, TimeUnit.MILLISECONDS)).isNull();
  }

  @Test
  void seekAckMismatchEmitsOffsetSeekEvent() throws Exception {
    daemon = new FakeLibrespotDaemon().seekAckMismatch(true);
    daemon.start();
    RecordingWsClient client = connectWs();

    send(post("/player/seek", FakeLibrespotDaemon.seekRequestJson(120_000, false)));

    String seek = client.frames.poll(5, TimeUnit.SECONDS);
    assertThat(seek)
        .contains("\"type\":\"seek\"")
        .contains("\"position\":125000");
  }

  // ------------------------------------------------------------------
  // Poison modes: HTTP hang + WS loss
  // ------------------------------------------------------------------

  @Test
  void httpHangModeActuallyHangsAndStopReleasesIt() throws Exception {
    daemon = new FakeLibrespotDaemon().hangRest("/player/play");
    daemon.start();

    HttpRequest hanging =
        HttpRequest.newBuilder(uri("/player/play"))
            .timeout(Duration.ofMillis(300))
            .POST(HttpRequest.BodyPublishers.ofString(FakeLibrespotDaemon.playRequestJson("spotify:track:abc", null, null, null)))
            .build();
    assertThatThrownBy(() -> http.send(hanging, HttpResponse.BodyHandlers.ofString()))
        .isInstanceOf(HttpTimeoutException.class);

    // stop() must release the hung handler promptly (bounded assert)
    long start = System.nanoTime();
    daemon.stop();
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
    assertThat(elapsedMs).isLessThan(5_000);
  }

  @Test
  void wsLossForceClosesConnectedClient() throws Exception {
    daemon = new FakeLibrespotDaemon();
    daemon.start();
    RecordingWsClient client = connectWs();

    daemon.dropWsClients();

    assertThat(client.closed.await(5, TimeUnit.SECONDS))
        .as("client socket should be force-closed by the fixture")
        .isTrue();
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  private URI uri(String path) {
    return URI.create("http://127.0.0.1:" + daemon.getPort() + path);
  }

  private HttpRequest post(String path, String body) {
    return HttpRequest.newBuilder(uri(path))
        .POST(HttpRequest.BodyPublishers.ofString(body))
        .build();
  }

  private HttpResponse<String> send(HttpRequest request) throws Exception {
    return http.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private RecordingWsClient connectWs() throws Exception {
    RecordingWsClient client =
        new RecordingWsClient(new URI("ws://127.0.0.1:" + daemon.getWsPort() + "/events"));
    wsClients.add(client);
    client.connect();
    assertThat(client.opened.await(5, TimeUnit.SECONDS))
        .as("ws client should open a connection")
        .isTrue();
    assertThat(daemon.awaitWsClients(1, BOUNDED))
        .as("fixture should observe the ws client")
        .isTrue();
    return client;
  }

  /** Minimal Java-WebSocket test client that records frames, open and close. */
  private static final class RecordingWsClient extends WebSocketClient {

    final BlockingQueue<String> frames = new LinkedBlockingQueue<>();
    final CountDownLatch opened = new CountDownLatch(1);
    final CountDownLatch closed = new CountDownLatch(1);

    RecordingWsClient(URI uri) {
      super(uri);
    }

    @Override
    public void onOpen(ServerHandshake handshake) {
      opened.countDown();
    }

    @Override
    public void onMessage(String message) {
      frames.add(message);
    }

    @Override
    public void onClose(int code, String reason, boolean remote) {
      closed.countDown();
    }

    @Override
    public void onError(Exception ex) {
      // ignore — the fixture itself surfaces failures via assertions
    }
  }
}
