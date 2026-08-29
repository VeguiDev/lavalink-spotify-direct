package dev.lavalinkplugins.golibrespot.testfixtures;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * In-JVM fake of the Spotify Web API (client-credentials token endpoint +
 * REST endpoints) for the {@code MetadataResolver} test seam.
 *
 * <p>A single JDK {@link HttpServer} on 127.0.0.1:{@link #getPort()} serving
 * {@code POST /api/token} and any {@code /v1/...} endpoint. Everything is
 * scriptable per test:
 * <ul>
 *   <li><b>Scripts</b> — {@link #script(String, String, Response)} keyed by
 *       exact method + path (query strings are ignored for routing; the full
 *       URI is recorded). Defaults: {@code POST /api/token} answers 200 with a
 *       fixture token, anything else 404.</li>
 *   <li><b>Sequences</b> — {@link #enqueue(String, String, Response...)}
 *       consumes responses in order before the script map (401-then-200,
 *       per-page offsets).</li>
 *   <li><b>Status control</b> — {@link Response#ok(String)},
 *       {@link Response#error(int)}, {@link Response#noContent()},
 *       {@link Response#hang()}; {@link Response#withRetryAfter(String)} adds
 *       the {@code Retry-After} header.</li>
 *   <li><b>Live bodies</b> — {@link Response#live(Supplier)} computes the body
 *       at serve time, so page bodies can embed this fake's own bound port via
 *       {@link #nextUrl(String, String)} (never a pre-scripted absolute URL).</li>
 *   <li><b>Recording</b> — every request is recorded in arrival order with its
 *       absolute URL (fake host + path + query); {@link #getReceivedRequests()}
 *       snapshots, {@link #hitCount(String, String)} counts per route.</li>
 * </ul>
 *
 * <p><b>Usage</b> (plain JUnit, loopback only):
 * <pre>{@code
 * FakeSpotifyWebApi fake = new FakeSpotifyWebApi()
 *     .scriptGet("/v1/tracks/4uLU6hMCjMI75M1A2tKUQC", FakeSpotifyWebApi.Response.ok(body));
 * try {
 *   fake.start();
 *   new MetadataResolver(selector, 5000, id, secret, "AR",
 *       HttpClient.newBuilder().build(), fake.getTokenUrl(), fake.getApiBaseUrl())
 *       .resolve(trackId);
 * } finally {
 *   fake.stop(); // idempotent; releases hang handlers
 * }
 * }</pre>
 *
 * <p>Never extends {@code src/main} and never uses anything but loopback +
 * JDK (test classpath). Route maps, queues, counters and the request log are
 * thread-safe: handlers run on the HttpServer executor threads.
 */
public final class FakeSpotifyWebApi implements AutoCloseable {

  private static final String P_TOKEN = "/api/token";

  private final int port;
  private HttpServer httpServer;
  private ExecutorService executor;
  private final ThreadFactory fakeThreads = runnable -> {
    Thread t = new Thread(runnable, "fake-spotify-" + System.nanoTime());
    t.setDaemon(true);
    return t;
  };

  // Scripting / recording (all thread-safe: read/written by server threads).
  private final ConcurrentHashMap<RouteKey, Response> script = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<RouteKey, ConcurrentLinkedQueue<Response>> queues = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<RouteKey, AtomicInteger> counts = new ConcurrentHashMap<>();
  private final List<RecordedRequest> received = new CopyOnWriteArrayList<>();
  private final List<CountDownLatch> hangLatches = new CopyOnWriteArrayList<>();

  public FakeSpotifyWebApi() {
    this(0);
  }

  /** @param port HTTP port, 0 = ephemeral (expose via {@link #getPort()}) */
  public FakeSpotifyWebApi(int port) {
    this.port = port;
  }

  // ------------------------------------------------------------------
  // Lifecycle
  // ------------------------------------------------------------------

  /** Binds the server on loopback and starts accepting. Idempotent-safe to call once. */
  public void start() throws IOException {
    httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", port), 0);
    executor = Executors.newCachedThreadPool(fakeThreads);
    httpServer.setExecutor(executor);
    httpServer.createContext("/", this::handle);
    httpServer.start();
  }

  /** Stops the server and releases hung handlers. Idempotent. */
  public void stop() {
    for (CountDownLatch latch : hangLatches) {
      latch.countDown();
    }
    hangLatches.clear();
    if (httpServer != null) {
      httpServer.stop(0);
      httpServer = null;
    }
  }

  @Override
  public void close() {
    stop();
    if (executor != null) {
      executor.shutdownNow();
      executor = null;
    }
  }

  /** @return bound HTTP port, or -1 before {@link #start()}. */
  public int getPort() {
    return httpServer == null ? -1 : httpServer.getAddress().getPort();
  }

  public String getHttpUrl() {
    return "http://127.0.0.1:" + getPort();
  }

  /** Base URL for injection as the resolver's {@code apiBaseUrl}. */
  public String getApiBaseUrl() {
    return getHttpUrl();
  }

  /** Token endpoint URL for injection as the resolver's {@code tokenUrl}. */
  public String getTokenUrl() {
    return getApiBaseUrl() + P_TOKEN;
  }

  // ------------------------------------------------------------------
  // Scripting (call before or after start(); volatile reads at request time)
  // ------------------------------------------------------------------

  /** Scripts an exact method+path; overrides the default behavior for that route. */
  public FakeSpotifyWebApi script(String method, String path, Response response) {
    script.put(new RouteKey(method, path), response);
    return this;
  }

  public FakeSpotifyWebApi scriptGet(String path, Response response) {
    return script("GET", path, response);
  }

  public FakeSpotifyWebApi scriptPost(String path, Response response) {
    return script("POST", path, response);
  }

  /** Convenience: scripts the {@code POST /api/token} endpoint. */
  public FakeSpotifyWebApi token(Response response) {
    return scriptPost(P_TOKEN, response);
  }

  /** Convenience: scripts {@code GET /v1/tracks/{trackId}}. */
  public FakeSpotifyWebApi track(String trackId, Response response) {
    return scriptGet("/v1/tracks/" + trackId, response);
  }

  /** Appends responses to a route queue; consumed in order before the script map. */
  public FakeSpotifyWebApi enqueue(String method, String path, Response... responses) {
    queues.computeIfAbsent(new RouteKey(method, path), k -> new ConcurrentLinkedQueue<>())
        .addAll(List.of(responses));
    return this;
  }

  public FakeSpotifyWebApi enqueueGet(String path, Response... responses) {
    return enqueue("GET", path, responses);
  }

  public FakeSpotifyWebApi enqueuePost(String path, Response... responses) {
    return enqueue("POST", path, responses);
  }

  // ------------------------------------------------------------------
  // Recording
  // ------------------------------------------------------------------

  /** Snapshot of every request received so far, in arrival order. */
  public List<RecordedRequest> getReceivedRequests() {
    return List.copyOf(received);
  }

  /** @return how many requests hit the given method+path route (0 when none). */
  public int hitCount(String method, String path) {
    AtomicInteger counter = counts.get(new RouteKey(method, path));
    return counter == null ? 0 : counter.get();
  }

  // ------------------------------------------------------------------
  // HTTP handling
  // ------------------------------------------------------------------

  private void handle(HttpExchange exchange) throws IOException {
    String method = exchange.getRequestMethod();
    String path = exchange.getRequestURI().getPath();
    String query = exchange.getRequestURI().getRawQuery();
    String body = readBody(exchange);
    record(method, path, query, body, exchange.getRequestURI().toString());

    Response response = resolve(method, path);
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
  }

  private Response resolve(String method, String path) {
    RouteKey key = new RouteKey(method, path);
    ConcurrentLinkedQueue<Response> queue = queues.get(key);
    if (queue != null) {
      Response next = queue.poll();
      if (next != null) {
        return next;
      }
    }
    Response scripted = script.get(key);
    if (scripted != null) {
      return scripted;
    }
    if (P_TOKEN.equals(path) && "POST".equals(method)) {
      return Response.ok(tokenJson("fixture-access-token", 3600));
    }
    return Response.error(404);
  }

  private void record(String method, String path, String query, String body, String uri) {
    String url = getApiBaseUrl() + uri;
    received.add(new RecordedRequest(method, path, query, body, url));
    counts.computeIfAbsent(new RouteKey(method, path), k -> new AtomicInteger()).incrementAndGet();
  }

  private void writeResponse(HttpExchange exchange, Response response) throws IOException {
    String body = response.body();
    if (body != null) {
      exchange.getResponseHeaders().set("Content-Type", "application/json");
    }
    for (Map.Entry<String, String> header : response.headers().entrySet()) {
      exchange.getResponseHeaders().set(header.getKey(), header.getValue());
    }
    byte[] bytes = body == null ? new byte[0] : body.getBytes(StandardCharsets.UTF_8);
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

  // ------------------------------------------------------------------
  // Static JSON builders (Spotify Web API shapes; no JSON library on the test classpath)
  // ------------------------------------------------------------------

  /** {@code POST /api/token} success body. */
  public static String tokenJson(String accessToken, long expiresInSeconds) {
    return "{\"access_token\":" + jsonString(accessToken)
        + ",\"token_type\":\"Bearer\""
        + ",\"expires_in\":" + expiresInSeconds + "}";
  }

  /**
   * Spotify Web API {@code track} object ({@code id}, {@code name},
   * {@code artists[0].name}, {@code album.name} + {@code album.images[0].url},
   * {@code duration_ms}, {@code external_ids.isrc}). Null fields are omitted.
   */
  public static String trackObjectJson(String id, String name, String artist,
                                       String albumName, String artworkUrl, Long durationMs, String isrc) {
    StringBuilder sb = new StringBuilder("{");
    sb.append("\"id\":").append(jsonString(id));
    sb.append(",\"name\":").append(jsonString(name));
    sb.append(",\"artists\":[{\"name\":").append(jsonString(artist)).append("}]");
    sb.append(",\"album\":{\"name\":").append(jsonString(albumName));
    if (artworkUrl != null) {
      sb.append(",\"images\":[{\"url\":").append(jsonString(artworkUrl)).append("}]");
    }
    sb.append('}');
    if (durationMs != null) {
      sb.append(",\"duration_ms\":").append(durationMs);
    }
    if (isrc != null) {
      sb.append(",\"external_ids\":{\"isrc\":").append(jsonString(isrc)).append('}');
    }
    return sb.append('}').toString();
  }

  /** One playlist page item: {@code {"track": <trackObjectJson>}}. */
  public static String playlistItem(String trackJson) {
    return "{\"track\":" + trackJson + "}";
  }

  /** One album page item: a bare {@code track} object. */
  public static String albumItem(String trackJson) {
    return trackJson;
  }

  /** Paging envelope: {@code {"items": [...], "next": <url|null>}}. */
  public static String pageJson(String itemsJson, String nextUrlOrNull) {
    return "{\"items\":" + itemsJson + ",\"next\":"
        + (nextUrlOrNull == null ? "null" : jsonString(nextUrlOrNull)) + "}";
  }

  /**
   * Absolute follow-up page URL on THIS fake, embedding the bound port.
   * Call after {@link #start()} (or lazily inside {@link Response#live} so it
   * is computed at serve time — never pre-scripted with a hardcoded port).
   */
  public String nextUrl(String path, String query) {
    return getApiBaseUrl() + path + (query == null || query.isBlank() ? "" : "?" + query);
  }

  /** Minimal JSON string escape (the fixture builds only fixed shapes). */
  private static String jsonString(String value) {
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

  // ------------------------------------------------------------------
  // Value types
  // ------------------------------------------------------------------

  /** A scripted answer: status + body (eager or serve-time live), optional headers, optional hang. */
  public static final class Response {

    private final int status;
    private final String body;
    private final Supplier<String> liveBody;
    private final boolean hang;
    private final Map<String, String> headers;

    private Response(int status, String body, Supplier<String> liveBody, boolean hang, Map<String, String> headers) {
      this.status = status;
      this.body = body;
      this.liveBody = liveBody;
      this.hang = hang;
      this.headers = headers;
    }

    public static Response of(int status, String body) {
      return new Response(status, body, null, false, Map.of());
    }

    public static Response ok(String body) {
      return of(200, body);
    }

    /** 200 with an empty body. */
    public static Response ok() {
      return of(200, null);
    }

    /** 204 no content. */
    public static Response noContent() {
      return of(204, null);
    }

    /** Typed failure with an empty body (e.g. {@code error(401)}, {@code error(500)}). */
    public static Response error(int status) {
      return of(status, null);
    }

    /** Never respond (client timeout trigger); the handler releases only on {@link FakeSpotifyWebApi#stop()}. */
    public static Response hang() {
      return new Response(0, null, null, true, Map.of());
    }

    /**
     * 200 whose body is computed at serve time — lets page bodies embed this
     * fake's bound port via {@link FakeSpotifyWebApi#nextUrl(String, String)}.
     */
    public static Response live(Supplier<String> liveBody) {
      return new Response(200, null, liveBody, false, Map.of());
    }

    /** Returns a copy with an extra HTTP response header (e.g. {@code Retry-After}). */
    public Response withHeader(String name, String value) {
      Map<String, String> next = new HashMap<>(headers);
      next.put(name, value);
      return new Response(status, body, liveBody, hang, Map.copyOf(next));
    }

    /** Returns a copy with the {@code Retry-After} header set. */
    public Response withRetryAfter(String seconds) {
      return withHeader("Retry-After", seconds);
    }

    String body() {
      return liveBody == null ? body : liveBody.get();
    }

    Map<String, String> headers() {
      return headers;
    }
  }

  /** One recorded request, in arrival order. {@code url} is absolute (fake host + path + query). */
  public record RecordedRequest(String method, String path, String query, String body, String url) {}

  private record RouteKey(String method, String path) {}
}
