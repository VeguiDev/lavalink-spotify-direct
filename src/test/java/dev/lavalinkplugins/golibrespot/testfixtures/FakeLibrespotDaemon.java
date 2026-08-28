package dev.lavalinkplugins.golibrespot.testfixtures;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

/**
 * In-JVM fake of the go-librespot v0.9.0 HTTP + WebSocket API (see
 * {@code docs/API_CONTRACT.md} — the pinned source of truth).
 *
 * <p>Two loopback-bound servers:
 * <ul>
 *   <li>a JDK {@link HttpServer} on 127.0.0.1:{@link #getPort()} serving
 *       {@code GET /}, {@code GET /status}, {@code POST /player/play|pause|resume|seek},
 *       {@code GET /web-api/*}; {@code POST /player/stop} is deliberately NOT
 *       implemented (always 404 — the plugin must never call it);</li>
 *   <li>a Java-WebSocket {@link WebSocketServer} on 127.0.0.1:{@link #getWsPort()}
 *       serving {@code GET /events} (the WS upgrade path; frames are
 *       {@code {"type": ..., "data": ...}}).</li>
 * </ul>
 *
 * <p>Everything is scriptable per test:
 * <ul>
 *   <li><b>REST</b> — {@link #scriptRest(String, String, Response)} (or the
 *       endpoint helpers {@link #status(Response)}, {@link #play(Response)},
 *       {@link #pause(Response)}, {@link #resume(Response)},
 *       {@link #seek(Response)}, {@link #webApi(Response)}). A {@link Response}
 *       carries a status code + body and may {@link Response#emit(String, String)
 *       attach events} that are pushed to WS clients right after the response is
 *       sent (REST-command-triggered emission). {@link Response#hang()} makes the
 *       handler never answer (client timeout trigger).</li>
 *   <li><b>Events</b> — {@link #emit(String, String)} pushes a frame to every
 *       connected WS client; {@link #emitAfter(String, String, long)} schedules
 *       one (delayed activation). Frames are dropped when no client is connected
 *       (same as the real daemon).</li>
 *   <li><b>WS control</b> — {@link #dropWsClients()} force-closes every socket
 *       (simulated WS loss / EOF).</li>
 *   <li><b>Recording</b> — every REST call is recorded in arrival order;
 *       {@link #getReceivedCommands()} snapshots, {@link #awaitCommands(int, Duration)}
 *       blocks (bounded, condition-variable) until N commands arrived.</li>
 * </ul>
 *
 * <p>Poison-sequence recipes (all supported out of the box):
 * <ul>
 *   <li><b>stale play after stop</b> — {@code emit("stopped", ...); emit("playing", ...);}</li>
 *   <li><b>delayed activation</b> — {@code emitAfter("active", null, 1500)}</li>
 *   <li><b>200-but-noop</b> — {@code play(Response.ok())} (200, no attached events)</li>
 *   <li><b>HTTP hang</b> — {@code hangRest("/player/play")} or {@code play(Response.hang())}</li>
 *   <li><b>WS loss mid-command</b> — {@code play(Response.ok().emit(...).dropWs())}</li>
 *   <li><b>status contradiction</b> — script {@link #status(Response)} with a body
 *       that contradicts previously emitted events</li>
 *   <li><b>restart EOF-then-data</b> — {@code dropWsClients()} then, after the
 *       client reconnects, {@code emit(...)} again</li>
 *   <li><b>seek ack mismatch</b> — {@link #seekAckMismatch(boolean)} makes every
 *       {@code POST /player/seek} additionally emit a {@code seek} event whose
 *       {@code position} is {@value #SEEK_ACK_MISMATCH_OFFSET_MS}ms ahead of the
 *       requested position</li>
 * </ul>
 *
 * <p><b>Usage</b> (plain JUnit, loopback only):
 * <pre>{@code
 * FakeLibrespotDaemon daemon = new FakeLibrespotDaemon().status(Response.of(200, body));
 * try {
 *   daemon.start();
 *   // daemon.getPort() / daemon.getWsPort() -> connect your clients
 *   daemon.emit("playing", FakeLibrespotDaemon.playingData(...));
 *   daemon.awaitCommands(1, Duration.ofSeconds(5));
 * } finally {
 *   daemon.stop(); // idempotent; releases hang handlers + closes WS + HTTP
 * }
 * }</pre>
 *
 * <p>Never extends {@code src/main} and never uses anything but loopback +
 * JDK + Java-WebSocket 1.5.x (test classpath). SINGLE OWNER: later todos (T8+)
 * consume this fixture; do not edit it from other tasks.
 */
public final class FakeLibrespotDaemon implements AutoCloseable {

  /** Offset used by {@link #seekAckMismatch(boolean)} (ms ahead of the requested position). */
  public static final long SEEK_ACK_MISMATCH_OFFSET_MS = 5_000L;

  private static final Pattern POSITION_FIELD = Pattern.compile("\"position\"\\s*:\\s*(-?\\d+)");

  private static final String P_ROOT = "/";
  private static final String P_STATUS = "/status";
  private static final String P_PLAY = "/player/play";
  private static final String P_PAUSE = "/player/pause";
  private static final String P_RESUME = "/player/resume";
  private static final String P_SEEK = "/player/seek";
  private static final String P_STOP = "/player/stop";
  private static final String P_WEB_API = "/web-api/";
  private static final String P_WEB_API_WILDCARD = "/web-api/*";

  private final int httpPort;
  private final int wsPort;

  private HttpServer httpServer;
  private WebSocketServer wsServer;
  private int wsBoundPort = -1;
  private ScheduledExecutorService scheduler;
  private final ThreadFactory daemonThreads = runnable -> {
    Thread t = new Thread(runnable, "fake-librespot-" + System.nanoTime());
    t.setDaemon(true);
    return t;
  };

  // Scripting / config (volatile: read by server threads, written by test thread).
  private final ConcurrentHashMap<RestKey, Response> restScript = new ConcurrentHashMap<>();
  private volatile boolean playbackReady = true;
  private volatile boolean hasSession = true;
  private volatile boolean wsEnabled = true;
  private volatile boolean seekAckMismatch = false;

  // Recording + waiting (guarded by the lock; Conditions are bounded waits).
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition commandsChanged = lock.newCondition();
  private final Condition wsChanged = lock.newCondition();
  private final List<RecordedCommand> received = new ArrayList<>();
  private int wsConnections = 0;

  private final List<CountDownLatch> hangLatches = new CopyOnWriteArrayList<>();

  public FakeLibrespotDaemon() {
    this(0, 0);
  }

  /**
   * @param httpPort HTTP port, 0 = ephemeral (expose via {@link #getPort()})
   * @param wsPort WebSocket port, 0 = ephemeral (expose via {@link #getWsPort()})
   */
  public FakeLibrespotDaemon(int httpPort, int wsPort) {
    this.httpPort = httpPort;
    this.wsPort = wsPort;
  }

  // ------------------------------------------------------------------
  // Lifecycle
  // ------------------------------------------------------------------

  /** Binds both servers on loopback and starts accepting. Idempotent-safe to call once. */
  public void start() throws IOException, InterruptedException {
    httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", httpPort), 0);
    httpServer.setExecutor(Executors.newCachedThreadPool(daemonThreads));
    httpServer.createContext("/", this::handle);
    httpServer.start();

    scheduler = Executors.newSingleThreadScheduledExecutor(daemonThreads);

    if (wsEnabled) {
      int target = wsPort;
      if (target == 0) {
        target = findFreePort();
      }
      wsServer = newWsServer(target);
      wsServer.start();
      wsBoundPort = target;
    }
  }

  /** Stops both servers, releases hung handlers, cancels scheduled emits. Idempotent. */
  public void stop() {
    for (CountDownLatch latch : hangLatches) {
      latch.countDown();
    }
    hangLatches.clear();
    if (scheduler != null) {
      scheduler.shutdownNow();
      scheduler = null;
    }
    if (wsServer != null) {
      dropWsClients();
      try {
        wsServer.stop();
      } catch (Exception ignored) {
        // best effort — server socket may already be closed
      }
      wsServer = null;
    }
    if (httpServer != null) {
      httpServer.stop(0);
      httpServer = null;
    }
    lock.lock();
    try {
      wsConnections = 0;
      wsChanged.signalAll();
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() {
    stop();
  }

  /** @return bound HTTP port, or -1 before {@link #start()}. */
  public int getPort() {
    return httpServer == null ? -1 : httpServer.getAddress().getPort();
  }

  /** @return bound WS port, or -1 when disabled / not started. */
  public int getWsPort() {
    return wsBoundPort;
  }

  public String getHttpUrl() {
    return "http://127.0.0.1:" + getPort();
  }

  public String getWsUrl() {
    return "ws://127.0.0.1:" + getWsPort() + "/events";
  }

  // ------------------------------------------------------------------
  // Configuration (call before start(); volatile reads at request time)
  // ------------------------------------------------------------------

  /** {@code GET /} body {@code {"playback_ready": <value>}} (default {@code true}). */
  public FakeLibrespotDaemon playbackReady(boolean ready) {
    this.playbackReady = ready;
    return this;
  }

  /**
   * {@code true} → {@code GET /status} answers 200 with a status body;
   * {@code false} → 204 no content (no-session mode). Also flips the default
   * {@code /web-api/*} answer (204 vs 404). Default {@code true}.
   */
  public FakeLibrespotDaemon hasSession(boolean session) {
    this.hasSession = session;
    return this;
  }

  /** Whether the {@code /events} WS server is bound (default {@code true}). */
  public FakeLibrespotDaemon wsEnabled(boolean enabled) {
    this.wsEnabled = enabled;
    return this;
  }

  /** Seek-ack mismatch poison: every seek also emits a {@code seek} event offset by {@value #SEEK_ACK_MISMATCH_OFFSET_MS}ms. */
  public FakeLibrespotDaemon seekAckMismatch(boolean mismatch) {
    this.seekAckMismatch = mismatch;
    return this;
  }

  // ------------------------------------------------------------------
  // REST scripting
  // ------------------------------------------------------------------

  /** Scripts an exact method+path; overrides the default behavior for that endpoint. */
  public FakeLibrespotDaemon scriptRest(String method, String path, Response response) {
    restScript.put(new RestKey(method, path), response);
    return this;
  }

  public FakeLibrespotDaemon scriptGet(String path, Response response) {
    return scriptRest("GET", path, response);
  }

  public FakeLibrespotDaemon scriptPost(String path, Response response) {
    return scriptRest("POST", path, response);
  }

  public FakeLibrespotDaemon status(Response response) {
    return scriptRest("GET", P_STATUS, response);
  }

  public FakeLibrespotDaemon play(Response response) {
    return scriptRest("POST", P_PLAY, response);
  }

  public FakeLibrespotDaemon pause(Response response) {
    return scriptRest("POST", P_PAUSE, response);
  }

  public FakeLibrespotDaemon resume(Response response) {
    return scriptRest("POST", P_RESUME, response);
  }

  public FakeLibrespotDaemon seek(Response response) {
    return scriptRest("POST", P_SEEK, response);
  }

  /** Scripts the {@code /web-api/*} prefix catch-all (exact paths win over it). */
  public FakeLibrespotDaemon webApi(Response response) {
    return scriptRest("GET", P_WEB_API_WILDCARD, response);
  }

  /** Makes the given exact path hang forever (never respond) until {@link #stop()}. */
  public FakeLibrespotDaemon hangRest(String path) {
    return scriptRest("POST", path, Response.hang());
  }

  // ------------------------------------------------------------------
  // Events (WS)
  // ------------------------------------------------------------------

  /** Pushes {@code {"type": <type>, "data": <dataJson>}} to every connected WS client (dropped when none). */
  public void emit(String type, String dataJson) {
    sendToClients(frame(type, dataJson));
  }

  /** {@link #emit(String, String)} with {@code data: null}. */
  public void emit(String type) {
    emit(type, null);
  }

  /** Schedules an emit {@code delayMs} in the future (daemon-scheduler thread). */
  public void emitAfter(String type, String dataJson, long delayMs) {
    if (scheduler == null) {
      throw new IllegalStateException("emitAfter requires start()");
    }
    scheduler.schedule(() -> emit(type, dataJson), delayMs, TimeUnit.MILLISECONDS);
  }

  /** Force-closes every connected WS socket (simulated WS loss / EOF). */
  public void dropWsClients() {
    if (wsServer == null) {
      return;
    }
    for (WebSocket conn : wsServer.getConnections()) {
      try {
        conn.closeConnection(1006, "fake ws loss");
      } catch (Exception ignored) {
        // already closed
      }
    }
  }

  /** Bounded wait until at least {@code count} WS clients are connected. */
  public boolean awaitWsClients(int count, Duration timeout) throws InterruptedException {
    lock.lock();
    try {
      long nanos = timeout.toNanos();
      while (wsConnections < count) {
        if (nanos <= 0) {
          return false;
        }
        nanos = wsChanged.awaitNanos(nanos);
      }
      return true;
    } finally {
      lock.unlock();
    }
  }

  // ------------------------------------------------------------------
  // Command recording
  // ------------------------------------------------------------------

  /** Snapshot of every REST call received so far, in arrival order. */
  public List<RecordedCommand> getReceivedCommands() {
    lock.lock();
    try {
      return List.copyOf(received);
    } finally {
      lock.unlock();
    }
  }

  /** Bounded (condition-variable) wait until at least {@code count} commands arrived; returns the snapshot. */
  public List<RecordedCommand> awaitCommands(int count, Duration timeout) throws InterruptedException {
    lock.lock();
    try {
      long nanos = timeout.toNanos();
      while (received.size() < count) {
        if (nanos <= 0) {
          break;
        }
        nanos = commandsChanged.awaitNanos(nanos);
      }
      return List.copyOf(received);
    } finally {
      lock.unlock();
    }
  }

  // ------------------------------------------------------------------
  // HTTP handling
  // ------------------------------------------------------------------

  private void handle(HttpExchange exchange) throws IOException {
    String method = exchange.getRequestMethod();
    String path = exchange.getRequestURI().getPath();
    String body = readBody(exchange);
    recordCommand(method, path, body);

    Response response = resolve(method, path);
    if (P_SEEK.equals(path) && seekAckMismatch && !response.hang) {
      long requested = extractPosition(body);
      if (requested >= 0) {
        response = response.emit(
            "seek",
            seekData("", "", requested + SEEK_ACK_MISMATCH_OFFSET_MS, 0, "go-librespot"));
      }
    }
    if (response.hang) {
      CountDownLatch latch = new CountDownLatch(1);
      hangLatches.add(latch);
      try {
        latch.await();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        hangLatches.remove(latch);
      }
      return; // never respond — released only by stop()
    }
    writeResponse(exchange, response);
    for (Event event : response.events) {
      emit(event.type(), event.dataJson());
    }
    if (response.dropWs) {
      dropWsClients();
    }
  }

  private Response resolve(String method, String path) {
    Response scripted = restScript.get(new RestKey(method, path));
    if (scripted != null) {
      return scripted;
    }
    // POST /player/stop is deliberately NOT implemented (forbidden for this plugin):
    // always 404, never scriptable, still recorded so tests can assert it is never invoked.
    if (P_STOP.equals(path)) {
      return Response.error(404);
    }
    if (P_ROOT.equals(path) && "GET".equals(method)) {
      return Response.ok("{\"playback_ready\":" + playbackReady + "}");
    }
    if (P_STATUS.equals(path) && "GET".equals(method)) {
      return hasSession ? Response.ok(defaultStatusJson()) : Response.noContent();
    }
    if (P_PLAY.equals(path) && "POST".equals(method)) {
      return Response.ok();
    }
    if (P_PAUSE.equals(path) && "POST".equals(method)) {
      return Response.ok();
    }
    if (P_RESUME.equals(path) && "POST".equals(method)) {
      return Response.ok();
    }
    if (P_SEEK.equals(path) && "POST".equals(method)) {
      return Response.ok();
    }
    if (path.startsWith(P_WEB_API)) {
      Response catchAll = restScript.get(new RestKey(method, P_WEB_API_WILDCARD));
      if (catchAll != null) {
        return catchAll;
      }
      // no session -> 204; with session -> 404 (unknown upstream path by default)
      return hasSession ? Response.error(404) : Response.noContent();
    }
    return Response.error(404);
  }

  private static String defaultStatusJson() {
    return statusJson(true, false, null);
  }

  private void writeResponse(HttpExchange exchange, Response response) throws IOException {
    if (response.body != null) {
      exchange.getResponseHeaders().set("Content-Type", "application/json");
    }
    byte[] bytes = response.body == null ? new byte[0] : response.body.getBytes(StandardCharsets.UTF_8);
    if (bytes.length == 0) {
      exchange.sendResponseHeaders(response.status, -1);
    } else {
      exchange.sendResponseHeaders(response.status, bytes.length);
      exchange.getResponseBody().write(bytes);
    }
    exchange.close();
  }

  private static String readBody(HttpExchange exchange) throws IOException {
    byte[] bytes = exchange.getRequestBody().readAllBytes();
    return bytes.length == 0 ? "" : new String(bytes, StandardCharsets.UTF_8);
  }

  private void recordCommand(String method, String path, String body) {
    lock.lock();
    try {
      received.add(new RecordedCommand(method, path, body));
      commandsChanged.signalAll();
    } finally {
      lock.unlock();
    }
  }

  // ------------------------------------------------------------------
  // WebSocket server
  // ------------------------------------------------------------------

  private WebSocketServer newWsServer(int port) {
    return new WebSocketServer(new InetSocketAddress("127.0.0.1", port)) {
      @Override
      public void onOpen(WebSocket conn, ClientHandshake handshake) {
        String path = handshake.getResourceDescriptor();
        if (!"/events".equals(path) && !"/events/".equals(path)) {
          conn.close(1008, "expected /events");
          return;
        }
        lock.lock();
        try {
          wsConnections++;
          wsChanged.signalAll();
        } finally {
          lock.unlock();
        }
      }

      @Override
      public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        lock.lock();
        try {
          wsConnections--;
          wsChanged.signalAll();
        } finally {
          lock.unlock();
        }
      }

      @Override
      public void onMessage(WebSocket conn, String message) {
        // server only reads — the fake never consumes client messages
      }

      @Override
      public void onError(WebSocket conn, Exception ex) {
        // best effort — ignore (e.g. client disconnect races)
      }

      @Override
      public void onStart() {
        // bound; nothing to do (start() already unblocked)
      }
    };
  }

  private void sendToClients(String frame) {
    if (wsServer == null) {
      return;
    }
    for (WebSocket conn : wsServer.getConnections()) {
      try {
        conn.send(frame);
      } catch (Exception ignored) {
        // client vanished — same as the real daemon dropping it
      }
    }
  }

  // ------------------------------------------------------------------
  // Static JSON builders (fixed v0.9.0 shapes; no JSON library on the test classpath)
  // ------------------------------------------------------------------

  /** Frame envelope: {@code {"type": ..., "data": ...}} ({@code data} may be {@code null}). */
  public static String frame(String type, String dataJson) {
    return "{\"type\":" + jsonString(type) + ",\"data\":" + (dataJson == null ? "null" : dataJson) + "}";
  }

  /** Full {@code track} schema (also the {@code metadata} event data). */
  public static String trackJson(
      String uri,
      String name,
      List<String> artistNames,
      String albumName,
      String albumCoverUrl,
      long position,
      long duration,
      String releaseDate,
      int trackNumber,
      int discNumber,
      String format,
      String codec,
      Integer bitrate,
      Integer sampleRate,
      Integer bitDepth) {
    String artists =
        artistNames == null
            ? "[]"
            : artistNames.stream()
                .map(FakeLibrespotDaemon::jsonString)
                .collect(Collectors.joining(",", "[", "]"));
    return "{\"uri\":" + jsonString(uri)
        + ",\"name\":" + jsonString(name)
        + ",\"artist_names\":" + artists
        + ",\"album_name\":" + jsonString(albumName)
        + ",\"album_cover_url\":" + jsonString(albumCoverUrl)
        + ",\"position\":" + position
        + ",\"duration\":" + duration
        + ",\"release_date\":" + jsonString(releaseDate)
        + ",\"track_number\":" + trackNumber
        + ",\"disc_number\":" + discNumber
        + ",\"format\":" + jsonString(format)
        + ",\"codec\":" + jsonString(codec)
        + ",\"bitrate\":" + (bitrate == null ? "null" : bitrate)
        + ",\"sample_rate\":" + (sampleRate == null ? "null" : sampleRate)
        + ",\"bit_depth\":" + (bitDepth == null ? "null" : bitDepth) + "}";
  }

  /** Full {@code ApiStatus} body (top-level {@code track} may be {@code null} → JSON null). */
  public static String statusJson(
      String username,
      String deviceId,
      String deviceType,
      String deviceName,
      String playOrigin,
      boolean stopped,
      boolean paused,
      boolean buffering,
      long volume,
      long volumeSteps,
      boolean repeatContext,
      boolean repeatTrack,
      boolean shuffleContext,
      String trackJsonOrNull) {
    return "{\"username\":" + jsonString(username)
        + ",\"device_id\":" + jsonString(deviceId)
        + ",\"device_type\":" + jsonString(deviceType)
        + ",\"device_name\":" + jsonString(deviceName)
        + ",\"play_origin\":" + jsonString(playOrigin)
        + ",\"stopped\":" + stopped
        + ",\"paused\":" + paused
        + ",\"buffering\":" + buffering
        + ",\"volume\":" + volume
        + ",\"volume_steps\":" + volumeSteps
        + ",\"repeat_context\":" + repeatContext
        + ",\"repeat_track\":" + repeatTrack
        + ",\"shuffle_context\":" + shuffleContext
        + ",\"track\":" + (trackJsonOrNull == null ? "null" : trackJsonOrNull) + "}";
  }

  /** {@link #statusJson(String, String, String, String, String, boolean, boolean, boolean, long, long, boolean, boolean, boolean, String)} with sensible defaults. */
  public static String statusJson(boolean stopped, boolean paused, String trackJsonOrNull) {
    return statusJson(
        "fake-user", "a1b2c3d4e5f6", "COMPUTER", "fake-daemon", "go-librespot",
        stopped, paused, false, 100, 100, false, false, false, trackJsonOrNull);
  }

  /** Shared {@code {context_uri, uri, play_origin}} event data (paused/not_playing/will_play). */
  public static String sharedTrackData(String contextUri, String uri, String playOrigin) {
    return "{\"context_uri\":" + jsonString(contextUri)
        + ",\"uri\":" + jsonString(uri)
        + ",\"play_origin\":" + jsonString(playOrigin) + "}";
  }

  /** {@code playing} event data — adds the {@code resume} flag. */
  public static String playingData(String contextUri, String uri, boolean resume, String playOrigin) {
    return "{\"context_uri\":" + jsonString(contextUri)
        + ",\"uri\":" + jsonString(uri)
        + ",\"resume\":" + resume
        + ",\"play_origin\":" + jsonString(playOrigin) + "}";
  }

  /** {@code seek} event data — {@code position} + {@code duration}. */
  public static String seekData(String contextUri, String uri, long position, long duration, String playOrigin) {
    return "{\"context_uri\":" + jsonString(contextUri)
        + ",\"uri\":" + jsonString(uri)
        + ",\"position\":" + position
        + ",\"duration\":" + duration
        + ",\"play_origin\":" + jsonString(playOrigin) + "}";
  }

  /** {@code stopped} event data — {@code play_origin} only (may be {@code ""}). */
  public static String stoppedData(String playOrigin) {
    return "{\"play_origin\":" + jsonString(playOrigin) + "}";
  }

  /** {@code volume} event data. */
  public static String volumeData(long value, long max) {
    return "{\"value\":" + value + ",\"max\":" + max + "}";
  }

  /** {@code repeat_track} / {@code repeat_context} / {@code shuffle_context} event data. */
  public static String toggleData(boolean value) {
    return "{\"value\":" + value + "}";
  }

  /** {@code POST /player/play} request body (only non-null fields are emitted). */
  public static String playRequestJson(String uri, Long position, Boolean paused, String skipToUri) {
    StringBuilder sb = new StringBuilder("{\"uri\":").append(jsonString(uri));
    if (skipToUri != null) {
      sb.append(",\"skip_to_uri\":").append(jsonString(skipToUri));
    }
    if (paused != null) {
      sb.append(",\"paused\":").append(paused);
    }
    if (position != null) {
      sb.append(",\"position\":").append(position);
    }
    return sb.append('}').toString();
  }

  /** {@code POST /player/seek} request body. */
  public static String seekRequestJson(long position, boolean relative) {
    return "{\"position\":" + position + ",\"relative\":" + relative + "}";
  }

  /** Minimal JSON string escape (the fixture builds only fixed shapes). */
  public static String jsonString(String value) {
    if (value == null) {
      return "null";
    }
    StringBuilder sb = new StringBuilder("\"");
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      switch (c) {
        case '"' -> sb.append("\\\"");
        case '\\' -> sb.append("\\\\");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> {
          if (c < 0x20) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    return sb.append('"').toString();
  }

  private static long extractPosition(String body) {
    if (body == null || body.isEmpty()) {
      return -1;
    }
    Matcher m = POSITION_FIELD.matcher(body);
    return m.find() ? Long.parseLong(m.group(1)) : -1;
  }

  private static int findFreePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0, 0, InetAddress.getLoopbackAddress())) {
      return socket.getLocalPort();
    }
  }

  // ------------------------------------------------------------------
  // Value types
  // ------------------------------------------------------------------

  /** A scripted REST answer: status + body, optional events to emit, optional hang / WS drop. Immutable. */
  public static final class Response {

    private final int status;
    private final String body;
    private final boolean hang;
    private final boolean dropWs;
    private final List<Event> events;

    private Response(int status, String body, boolean hang, boolean dropWs, List<Event> events) {
      this.status = status;
      this.body = body;
      this.hang = hang;
      this.dropWs = dropWs;
      this.events = events;
    }

    public static Response of(int status, String body) {
      return new Response(status, body, false, false, List.of());
    }

    public static Response ok(String body) {
      return of(200, body);
    }

    /** 200 with an empty body (the real daemon's typical play/pause/resume/seek answer). */
    public static Response ok() {
      return of(200, null);
    }

    /** 204 no content (no-session mode). */
    public static Response noContent() {
      return of(204, null);
    }

    /** Typed failure with an empty body (e.g. {@code error(400)}, {@code error(404)}). */
    public static Response error(int status) {
      return of(status, null);
    }

    /** Never respond (client timeout trigger); the handler releases only on {@link FakeLibrespotDaemon#stop()}. */
    public static Response hang() {
      return new Response(0, null, true, false, List.of());
    }

    /** Appends an event to emit to WS clients immediately after the REST response is sent. */
    public Response emit(String type, String dataJson) {
      List<Event> next = new ArrayList<>(events);
      next.add(new Event(type, dataJson));
      return new Response(status, body, hang, dropWs, List.copyOf(next));
    }

    /** Emits an event with {@code data: null}. */
    public Response emit(String type) {
      return emit(type, null);
    }

    /** After the response, force-close every WS socket (WS loss mid-command). */
    public Response dropWs() {
      return new Response(status, body, hang, true, events);
    }
  }

  /** One scripted WS frame. */
  public record Event(String type, String dataJson) {}

  /** One recorded REST call, in arrival order. */
  public record RecordedCommand(String method, String path, String body) {}

  private record RestKey(String method, String path) {}
}
