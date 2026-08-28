package dev.lavalinkplugins.golibrespot;

import com.sedmelluq.discord.lavaplayer.player.AudioLoadResultHandler;
import com.sedmelluq.discord.lavaplayer.player.AudioPlayerManager;
import com.sedmelluq.discord.lavaplayer.tools.FriendlyException;
import com.sedmelluq.discord.lavaplayer.track.AudioPlaylist;
import com.sedmelluq.discord.lavaplayer.track.AudioTrack;
import dev.lavalinkplugins.golibrespot.source.GoLibrespotAudioSourceManager;
import dev.lavalinkplugins.golibrespot.source.GoLibrespotAudioTrack;
import dev.lavalinkplugins.golibrespot.testfixtures.FakeLibrespotDaemon;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lavalink.server.LavalinkApplication;
import lavalink.server.bootstrap.PluginManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.io.DefaultResourceLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * T19 integration smoke: boots the REAL Lavalink test server
 * ({@code dev.arbjerg.lavalink:Lavalink-Server:4.2.2}, plain jar on the test
 * classpath) in-JVM against a {@link FakeLibrespotDaemon}, with the plugin
 * package component-scanned into the server's main context, and proves:
 *
 * <ul>
 *   <li>the plugin bean loads and the config binds from {@code plugins.golibrespot};</li>
 *   <li>{@code GET /v4/info} lists the {@code spdirect} source manager and the
 *       {@code golibrespot} plugin;</li>
 *   <li>loading {@code spdirect:<id>} through the real server's
 *       {@code AudioPlayerManager} (the exact load pipeline the REST endpoint
 *       drives, minus the response-encoding step) resolves to a
 *       {@code GoLibrespotAudioTrack} whose metadata was fetched from the fake
 *       daemon's {@code /web-api} passthrough — no lease, no fabricated
 *       duration;</li>
 *   <li>an ordinary {@code spotify:track:} URI falls through both the HTTP
 *       endpoint (loadType {@code empty}) and the manager (noMatches) — never
 *       claimed by this source;</li>
 *   <li>a structurally malformed {@code plugins.golibrespot} fails the Spring
 *       context (startup-fatal) with a clear message.</li>
 * </ul>
 *
 * <p><b>Why the HTTP load is proven through the manager, not
 * {@code /v4/loadtracks}:</b> the server's {@code UtilKt.toTrack} encodes
 * every track in a load response ({@code AudioPlayerManager.encodeTrack} →
 * {@code GoLibrespotAudioSourceManager.encodeTrack}), and spdirect tracks are
 * deliberately <em>non-encodable</em> (T18 contract: the daemon session is not
 * replayable from a byte stream), so a direct HTTP load of an spdirect track
 * returns HTTP 500 here by design. The manager-level load exercises the real
 * plugin pipeline (identifier claim → lease-free metadata via the fake daemon
 * → {@code GoLibrespotAudioTrack}) end to end.</p>
 *
 * <p><b>What this smoke does NOT prove:</b> it does not play audio (no FIFO,
 * no lease, no daemon playback) — the daemon REST/WS command lanes, activation
 * barrier, FIFO rendezvous and PCM path are covered by the T14–T18 unit and
 * concurrency suites instead. The websocket simply connects and idles here.</p>
 *
 * <p>The boot replicates {@code lavalink.server.Launcher}'s two-stage startup
 * ({@link PluginManager} bootstrap context as the parent of the
 * {@link LavalinkApplication} main context) with {@code server.port=0} for an
 * ephemeral port and the plugin's package in the {@code componentScan}
 * property.</p>
 */
class GoLibrespotIntegrationSmokeTest {

  private static final String PASSWORD = "youshallnotpass";
  private static final String TRACK_ID = "4uLU6hMCjMI75M1A2tKUQC";
  private static final String TRACK_NAME = "Test Track";

  private static FakeLibrespotDaemon daemon;
  private static ConfigurableApplicationContext serverContext;
  private static HttpClient http;
  private static String baseUrl;
  private static Path tempDir;

  @BeforeAll
  static void bootRealLavalinkServer() throws Exception {
    tempDir = Files.createTempDirectory("golibrespot-smoke-");

    daemon = new FakeLibrespotDaemon();
    // The /web-api/v1/tracks/{id} passthrough returns the metadata the
    // resolver maps to an AudioTrackInfo (web-api track shape, not the daemon
    // /status track shape).
    daemon.webApi(FakeLibrespotDaemon.Response.ok(webApiTrackJson()));
    daemon.start();

    String pluginsDir = tempDir.resolve("plugins").toString();
    String fifoPath = tempDir.resolve("spdirect-1.fifo").toAbsolutePath().toString();
    String[] args = {
        "--server.port=0",
        "--lavalink.server.password=" + PASSWORD,
        "--lavalink.pluginsDir=" + pluginsDir,
        "--spring.cloud.config.enabled=false",
        "--plugins.golibrespot.enabled=true",
        "--plugins.golibrespot.backends[0].name=gb-1",
        "--plugins.golibrespot.backends[0].restBaseUrl=" + daemon.getHttpUrl(),
        "--plugins.golibrespot.backends[0].wsUrl=" + daemon.getWsUrl(),
        "--plugins.golibrespot.backends[0].fifoPath=" + fifoPath,
        "--logging.level.root=WARN",
    };

    // Stage 1 — plugin bootstrap context (mirrors Launcher.launchPluginBootstrap).
    SpringApplication bootstrap = new SpringApplication(PluginManager.class);
    bootstrap.setBannerMode(Banner.Mode.OFF);
    bootstrap.setWebApplicationType(WebApplicationType.NONE);
    ConfigurableApplicationContext parent = bootstrap.run("--lavalink.pluginsDir=" + pluginsDir);

    // Stage 2 — the real server application (mirrors Launcher.launchMain).
    Properties componentScan = new Properties();
    componentScan.put("componentScan", "dev.lavalinkplugins.golibrespot,lavalink.server");
    serverContext = new SpringApplicationBuilder()
        .sources(LavalinkApplication.class)
        .properties(componentScan)
        .web(WebApplicationType.SERVLET)
        .bannerMode(Banner.Mode.OFF)
        .resourceLoader(new DefaultResourceLoader(parent.getBean(PluginManager.class).getClassLoader()))
        .parent(parent)
        .run(args);

    http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    String port = serverContext.getEnvironment().getProperty("local.server.port");
    assertThat(port).as("server must report its ephemeral HTTP port").isNotBlank();
    baseUrl = "http://127.0.0.1:" + port;
  }

  @AfterAll
  static void tearDown() {
    if (serverContext != null) {
      serverContext.close();
      serverContext = null;
    }
    if (daemon != null) {
      daemon.stop();
    }
  }

  // ------------------------------------------------------------ smoke: /v4/info

  @Test
  @Timeout(120)
  void infoListsSpdirectSourceAndGolibrespotPlugin() throws Exception {
    Map<String, Object> info = getJson("/v4/info");

    assertThat(asList(info.get("sourceManagers")))
        .as("the plugin's source manager must be registered with the AudioPlayerManager")
        .contains("spdirect");
    List<Object> plugins = asList(info.get("plugins"));
    assertThat(plugins)
        .as("the golibrespot plugin descriptor must be reported")
        .anySatisfy(p -> {
          Map<String, Object> plugin = asMap(p);
          assertThat(asString(plugin.get("name"))).isEqualTo("golibrespot");
          assertThat(asString(plugin.get("version"))).isEqualTo("1.0.0");
        });

    // The bean is the AudioSourceManager Lavalink registered.
    assertThat(serverContext.getBean(GoLibrespotAudioSourceManager.class))
        .isInstanceOf(com.sedmelluq.discord.lavaplayer.source.AudioSourceManager.class);
  }

  // ------------------------------------------------------------ smoke: loadtracks

  @Test
  @Timeout(120)
  void spdirectLoadThroughRealServerResolvesToGoLibrespotAudioTrack() throws Exception {
    AudioTrack track = loadSync(serverContext.getBean(AudioPlayerManager.class),
        "spdirect:" + TRACK_ID);

    assertThat(track)
        .as("the real server's load pipeline must produce an spdirect track "
            + "(metadata fetched from the fake daemon, no lease)")
        .isInstanceOf(GoLibrespotAudioTrack.class);
    assertThat(track.getInfo().identifier).isEqualTo("spdirect:" + TRACK_ID);
    assertThat(track.getInfo().title).isEqualTo(TRACK_NAME);
    assertThat(track.getInfo().length).isPositive();
    assertThat(track.getInfo().isStream).isTrue();
  }

  @Test
  @Timeout(120)
  void ordinarySpotifyUriFallsThroughUnclaimed() throws Exception {
    Map<String, Object> load = getJson("/v4/loadtracks?identifier="
        + urlEncode("spotify:track:" + TRACK_ID));
    assertThat(asString(load.get("loadType"))).isEqualTo("empty");

    assertThat(loadSync(serverContext.getBean(AudioPlayerManager.class), "spotify:track:" + TRACK_ID))
        .isNull();
  }

  private static AudioTrack loadSync(AudioPlayerManager manager, String identifier) throws Exception {
    CompletableFuture<AudioTrack> future = new CompletableFuture<>();
    manager.loadItem(identifier, new AudioLoadResultHandler() {
      @Override
      public void trackLoaded(AudioTrack track) {
        future.complete(track);
      }

      @Override
      public void playlistLoaded(AudioPlaylist playlist) {
        future.complete(null);
      }

      @Override
      public void noMatches() {
        future.complete(null);
      }

      @Override
      public void loadFailed(FriendlyException exception) {
        future.completeExceptionally(exception);
      }
    });
    return future.get(30, TimeUnit.SECONDS);
  }

  // ------------------------------------------------------------ smoke: malformed config

  @Test
  @Timeout(60)
  void structurallyMalformedConfigFailsStartup() {
    AnnotationConfigApplicationContext ctx = minimalContext(Map.of(
        "plugins.golibrespot.enabled", "true",
        "plugins.golibrespot.backends[0].name", "gb-1",
        "plugins.golibrespot.backends[0].restBaseUrl", "http://127.0.0.1:1",
        "plugins.golibrespot.backends[0].fifoPath", tempDir.resolve("a.fifo").toString(),
        "plugins.golibrespot.backends[1].name", "gb-1", // duplicate backend name
        "plugins.golibrespot.backends[1].restBaseUrl", "http://127.0.0.1:2",
        "plugins.golibrespot.backends[1].fifoPath", tempDir.resolve("b.fifo").toString()));
    try {
      assertThatThrownBy(ctx::refresh)
          .rootCause()
          .hasMessageContaining("duplicate backend name 'gb-1'");
    } finally {
      ctx.close();
    }
  }

  @Test
  @Timeout(60)
  void fifoCheckFailAbortsStartupWhenFifoMissing() {
    AnnotationConfigApplicationContext ctx = minimalContext(Map.of(
        "plugins.golibrespot.enabled", "true",
        "plugins.golibrespot.fifoCheck", "fail",
        "plugins.golibrespot.backends[0].name", "gb-1",
        "plugins.golibrespot.backends[0].restBaseUrl", "http://127.0.0.1:1",
        "plugins.golibrespot.backends[0].wsUrl", "ws://127.0.0.1:1/events",
        "plugins.golibrespot.backends[0].fifoPath", tempDir.resolve("missing.fifo").toString()));
    try {
      assertThatThrownBy(ctx::refresh)
          .rootCause()
          .hasMessageContaining("fifoCheck=fail");
    } finally {
      ctx.close();
    }
  }

  /** A context that only component-scans the plugin package, fed by a property map. */
  private static AnnotationConfigApplicationContext minimalContext(Map<String, String> properties) {
    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    ctx.getEnvironment().getPropertySources()
        .addFirst(new MapPropertySource("test", new LinkedHashMap<>(properties)));
    ctx.scan("dev.lavalinkplugins.golibrespot");
    return ctx;
  }

  // ------------------------------------------------------------ http + json helpers

  private Map<String, Object> getJson(String path) throws IOException, InterruptedException {
    HttpRequest request = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl + path))
        .header("Authorization", PASSWORD)
        .timeout(Duration.ofSeconds(60))
        .GET()
        .build();
    HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode()).as("GET %s", path).isEqualTo(200);
    return asMap(parseJson(response.body()));
  }

  private static String urlEncode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static String webApiTrackJson() {
    return "{\"name\":\"" + TRACK_NAME + "\","
        + "\"artists\":[{\"name\":\"Test Artist\"}],"
        + "\"album\":{\"name\":\"Test Album\",\"images\":[{\"url\":\"https://example.com/art.jpg\"}]},"
        + "\"duration_ms\":214000,"
        + "\"external_ids\":{\"isrc\":\"GBAAA0000001\"}}";
  }

  // ------------------------------------------------------------ minimal JSON model

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asMap(Object value) {
    return (Map<String, Object>) value;
  }

  private static List<Object> asList(Object value) {
    return value == null ? List.of() : (List<Object>) value;
  }

  private static String asString(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  /** Minimal tolerant JSON parser: Map / List / String / Long / Double / Boolean / null. */
  private static Object parseJson(String json) {
    return new JsonParser(json).parseValue();
  }

  private static final class JsonParser {
    private final String input;
    private int pos;

    JsonParser(String input) {
      this.input = input;
    }

    Object parseValue() {
      skipWhitespace();
      if (pos >= input.length()) {
        return null;
      }
      char c = input.charAt(pos);
      return switch (c) {
        case '{' -> parseObject();
        case '[' -> parseArray();
        case '"' -> parseString();
        case 't', 'f' -> parseBoolean();
        case 'n' -> parseNull();
        default -> parseNumber();
      };
    }

    private Map<String, Object> parseObject() {
      Map<String, Object> result = new LinkedHashMap<>();
      pos++; // '{'
      skipWhitespace();
      if (input.charAt(pos) == '}') {
        pos++;
        return result;
      }
      while (true) {
        skipWhitespace();
        String key = parseString();
        skipWhitespace();
        pos++; // ':'
        result.put(key, parseValue());
        skipWhitespace();
        char c = input.charAt(pos);
        pos++;
        if (c == '}') {
          return result;
        }
      }
    }

    private List<Object> parseArray() {
      List<Object> result = new ArrayList<>();
      pos++; // '['
      skipWhitespace();
      if (input.charAt(pos) == ']') {
        pos++;
        return result;
      }
      while (true) {
        result.add(parseValue());
        skipWhitespace();
        char c = input.charAt(pos);
        pos++;
        if (c == ']') {
          return result;
        }
      }
    }

    private String parseString() {
      pos++; // '"'
      StringBuilder sb = new StringBuilder();
      while (true) {
        char c = input.charAt(pos);
        if (c == '"') {
          pos++;
          return sb.toString();
        }
        if (c == '\\') {
          char next = input.charAt(++pos);
          sb.append(switch (next) {
            case 'n' -> '\n';
            case 't' -> '\t';
            case 'r' -> '\r';
            case 'b' -> '\b';
            case 'f' -> '\f';
            case 'u' -> {
              String hex = input.substring(pos + 1, pos + 5);
              pos += 4;
              yield (char) Integer.parseInt(hex, 16);
            }
            default -> next;
          });
        } else {
          sb.append(c);
        }
        pos++;
      }
    }

    private Boolean parseBoolean() {
      if (input.startsWith("true", pos)) {
        pos += 4;
        return Boolean.TRUE;
      }
      pos += 5; // "false"
      return Boolean.FALSE;
    }

    private Object parseNull() {
      pos += 4; // "null"
      return null;
    }

    private Number parseNumber() {
      int start = pos;
      while (pos < input.length() && "-+0123456789.eE".indexOf(input.charAt(pos)) >= 0) {
        pos++;
      }
      String raw = input.substring(start, pos);
      try {
        return raw.contains(".") || raw.contains("e") || raw.contains("E")
            ? (Number) Double.parseDouble(raw)
            : (Number) Long.parseLong(raw);
      } catch (NumberFormatException e) {
        return 0L;
      }
    }

    private void skipWhitespace() {
      while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
        pos++;
      }
    }
  }
}
