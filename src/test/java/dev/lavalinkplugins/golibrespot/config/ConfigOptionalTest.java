package dev.lavalinkplugins.golibrespot.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;

/**
 * F6 + F5:
 *
 * <ul>
 *   <li><b>F6</b> — an ABSENT {@code plugins.golibrespot} config must not abort
 *       Lavalink startup: Lavalink auto-loads every plugin JAR, so an
 *       unconfigured plugin no-ops gracefully (disabled, zero backends). A
 *       PRESENT but structurally-invalid config still fails startup.</li>
 *   <li><b>F5</b> — {@code restBaseUrl} trailing slashes are normalized at config
 *       parse so the REST client never builds double-slash paths (404 → spurious
 *       quarantine).</li>
 * </ul>
 *
 * <p>The plugin-level tests boot the plugin package in a bare
 * {@link AnnotationConfigApplicationContext} fed by a property map (the T19
 * smoke-test pattern) — component-scanning the plugin constructs the real
 * {@code @Service} + {@code @Bean} graph, so "startup not aborted" is proven
 * against the actual wiring.</p>
 */
class ConfigOptionalTest {

  private static String absoluteFifo() {
    return Path.of(".").toAbsolutePath().normalize().resolve("pipe.sock").toString();
  }

  private static Map<String, Object> backend(String name, String restBaseUrl, String fifoPath) {
    return Map.of("name", name, "restBaseUrl", restBaseUrl, "fifoPath", fifoPath);
  }

  /** A context that only component-scans the plugin package, fed by a property map. */
  private static AnnotationConfigApplicationContext minimalContext(Map<String, String> properties) {
    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    ctx.getEnvironment().getPropertySources()
        .addFirst(new MapPropertySource("test", new LinkedHashMap<>(properties)));
    ctx.scan("dev.lavalinkplugins.golibrespot");
    return ctx;
  }

  // ---------------------------------------------------------------- F6: absent config

  /**
   * Absent {@code plugins.golibrespot}: the plugin context must refresh WITHOUT
   * aborting — the plugin no-ops with a disabled, zero-backend runtime (Lavalink
   * auto-loads every plugin JAR, so an unconfigured plugin must never take the
   * server down).
   */
  @Test
  void absentConfigDoesNotAbortStartupAndYieldsDisabledNoOpRuntime() {
    AnnotationConfigApplicationContext ctx = minimalContext(Map.of());
    try {
      ctx.refresh(); // must NOT throw
      ExclusivePool pool = ctx.getBean(ExclusivePool.class);
      assertThat(pool).isNotNull();
      assertThat(pool.handles()).as("no backends configured").isEmpty();
    } finally {
      ctx.close();
    }
  }

  /**
   * A PRESENT but structurally-invalid config must STILL fail startup (the
   * F6 no-op applies only to absent/empty config).
   */
  @Test
  void presentStructurallyInvalidConfigStillFailsStartup() {
    AnnotationConfigApplicationContext ctx = minimalContext(Map.of(
        "plugins.golibrespot.enabled", "true",
        "plugins.golibrespot.backends[0].name", "gb-1",
        "plugins.golibrespot.backends[0].restBaseUrl", "http://127.0.0.1:1",
        "plugins.golibrespot.backends[0].fifoPath", absoluteFifo(),
        "plugins.golibrespot.backends[1].name", "gb-1", // duplicate backend name
        "plugins.golibrespot.backends[1].restBaseUrl", "http://127.0.0.1:2",
        "plugins.golibrespot.backends[1].fifoPath", absoluteFifo()));
    try {
      assertThatThrownBy(ctx::refresh)
          .rootCause()
          .hasMessageContaining("duplicate backend name 'gb-1'");
    } finally {
      ctx.close();
    }
  }

  /**
   * An EXPLICIT {@code enabled: false} (present, disabled) binds normally and
   * never fails — the deliberate-disable path.
   */
  @Test
  void explicitDisabledConfigBindsWithoutFailure() {
    AnnotationConfigApplicationContext ctx = minimalContext(Map.of(
        "plugins.golibrespot.enabled", "false"));
    try {
      ctx.refresh(); // must NOT throw
      assertThat(ctx.getBean(ExclusivePool.class).handles()).isEmpty();
    } finally {
      ctx.close();
    }
  }

  // ---------------------------------------------------------------- F5: trailing slash normalization

  @Test
  void restBaseUrlTrailingSlashesAreNormalizedAtConfigParse() {
    GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
        "backends", List.of(backend("b", "http://localhost:8888//", absoluteFifo()))));

    assertThat(config.getBackends().get(0).getRestBaseUrl()).isEqualTo("http://localhost:8888");
    assertThat(GoLibrespotConfigValidator.validate(config)).isEmpty();
  }

  @Test
  void restBaseUrlManyTrailingSlashesAreCollapsed() {
    GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
        "backends", List.of(backend("b", "https://daemon.internal:443///", absoluteFifo()))));

    assertThat(config.getBackends().get(0).getRestBaseUrl()).isEqualTo("https://daemon.internal:443");
    assertThat(GoLibrespotConfigValidator.validate(config)).isEmpty();
  }

  @Test
  void restBaseUrlPathPrefixTrailingSlashIsStripped() {
    GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
        "backends", List.of(backend("b", "http://localhost:8888/base/", absoluteFifo()))));

    assertThat(config.getBackends().get(0).getRestBaseUrl()).isEqualTo("http://localhost:8888/base");
    assertThat(GoLibrespotConfigValidator.validate(config)).isEmpty();
  }

  @Test
  void explicitWsUrlTrailingSlashIsNormalized() {
    Map<String, Object> backend = new LinkedHashMap<>(
        backend("b", "http://localhost:8888", absoluteFifo()));
    backend.put("wsUrl", "ws://localhost:8888/events/");

    GoLibrespotConfig config = GoLibrespotConfig.from(Map.of("backends", List.of(backend)));

    assertThat(config.getBackends().get(0).getWsUrl()).isEqualTo("ws://localhost:8888/events");
    assertThat(config.getBackends().get(0).getRestBaseUrl()).isEqualTo("http://localhost:8888");
  }

  @Test
  void noTrailingSlashIsUntouched() {
    GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
        "backends", List.of(backend("b", "http://localhost:8888", absoluteFifo()))));

    assertThat(config.getBackends().get(0).getRestBaseUrl()).isEqualTo("http://localhost:8888");
    assertThat(GoLibrespotConfigValidator.validate(config)).isEmpty();
  }
}
