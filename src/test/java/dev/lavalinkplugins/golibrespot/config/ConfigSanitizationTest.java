package dev.lavalinkplugins.golibrespot.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;

/**
 * F3: the validation-failure exception message must be sanitized before it
 * enters the {@link IllegalStateException} that aborts startup — credentials in
 * {@code restBaseUrl} userinfo (e.g. {@code http://user:pass@host}) must never
 * leak into the context-abort message.
 */
class ConfigSanitizationTest {

  private static String absoluteFifo() {
    return Path.of(".").toAbsolutePath().normalize().resolve("pipe.sock").toString();
  }

  /** A context that only component-scans the plugin package, fed by a property map. */
  private static AnnotationConfigApplicationContext minimalContext(Map<String, String> properties) {
    AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    ctx.getEnvironment().getPropertySources()
        .addFirst(new MapPropertySource("test", new LinkedHashMap<>(properties)));
    ctx.scan("dev.lavalinkplugins.golibrespot");
    return ctx;
  }

  /**
   * An invalid {@code restBaseUrl} that carries userinfo credentials produces a
   * validation failure that echoes the URL — the startup-abort message must
   * redact the userinfo, never the raw credentials.
   */
  @Test
  void pluginAbortMessageRedactsRestBaseUrlUserinfo() {
    AnnotationConfigApplicationContext ctx = minimalContext(Map.of(
        "plugins.golibrespot.enabled", "true",
        "plugins.golibrespot.backends[0].name", "gb-1",
        // invalid (no host) but carries userinfo credentials the message must redact
        "plugins.golibrespot.backends[0].restBaseUrl", "http://user:pass@",
        "plugins.golibrespot.backends[0].fifoPath", absoluteFifo()));
    try {
      assertThatThrownBy(ctx::refresh)
          .rootCause()
          .hasMessageContaining("restBaseUrl")
          .hasMessageContaining("***")
          .hasMessageNotContaining("user")
          .hasMessageNotContaining("pass");
    } finally {
      ctx.close();
    }
  }

  /**
   * A restBaseUrl that fails URI parsing (space in host) while carrying
   * userinfo is also redacted — every failure message that echoes the URL.
   */
  @Test
  void pluginAbortMessageRedactsUserinfoRegardlessOfFailureReason() {
    AnnotationConfigApplicationContext ctx = minimalContext(Map.of(
        "plugins.golibrespot.enabled", "true",
        "plugins.golibrespot.backends[0].name", "gb-1",
        "plugins.golibrespot.backends[0].restBaseUrl", "http://user:pass@ho st",
        "plugins.golibrespot.backends[0].fifoPath", absoluteFifo()));
    try {
      assertThatThrownBy(ctx::refresh)
          .rootCause()
          .hasMessageContaining("restBaseUrl")
          .hasMessageNotContaining("user")
          .hasMessageNotContaining("pass");
    } finally {
      ctx.close();
    }
  }
}
