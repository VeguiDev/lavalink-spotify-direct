package dev.lavalinkplugins.golibrespot;

import dev.arbjerg.lavalink.api.IPlayer;
import dev.arbjerg.lavalink.api.ISocketContext;
import dev.arbjerg.lavalink.api.PluginEventHandler;
import dev.lavalinkplugins.golibrespot.config.ConfigBinding;
import dev.lavalinkplugins.golibrespot.config.ConfigFactory;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfigValidator;
import dev.lavalinkplugins.golibrespot.logging.LogSanitizer;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import dev.lavalinkplugins.golibrespot.source.CoordinatorFactory;
import dev.lavalinkplugins.golibrespot.source.GoLibrespotAudioSourceManager;
import dev.lavalinkplugins.golibrespot.source.PlayerLifecycleBridge;
import jakarta.annotation.PreDestroy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

/**
 * The Lavalink plugin entrypoint (T19): {@code @Service} extending
 * {@link PluginEventHandler}, component-scanned into the server's main context
 * from the {@code lavalink-plugins/golibrespot.properties} {@code path} (the
 * plugin's package). It binds {@code plugins.golibrespot} from the server's
 * {@link Environment} (via {@link ConfigBinding}, which reconstructs the raw
 * subtree the T4 {@link GoLibrespotConfig} binds strictly), fails startup on
 * structural configuration errors, builds the whole production backend graph
 * through {@link ConfigFactory}, exposes the pool / metadata resolver /
 * coordinator factory beans that the {@code @Service} {@link GoLibrespotAudioSourceManager}
 * auto-wires (Lavalink registers every {@code AudioSourceManager} bean), and
 * attaches the T18 {@link PlayerLifecycleBridge} to every player.
 *
 * <p><b>Startup-fatal vs degraded.</b> Structural errors (duplicate backend
 * names, invalid URLs, non-absolute FIFO paths, empty backends while enabled,
 * unknown/unparseable keys) abort startup with a clear, sanitized message.
 * Daemon/FIFO unavailability does NOT abort: the FIFO existence policy
 * ({@code fifoCheck warn|fail}) is enforced here — {@code fail} aborts startup
 * while {@code warn} (default) logs a warning — and daemon readiness is
 * established asynchronously by each backend's events websocket (connect
 * failures quarantine the backend; loads then fail clearly while Lavalink
 * stays healthy).</p>
 *
 * <p><b>Teardown.</b> {@link #shutdown()} closes the coordinator factory,
 * which idempotently tears down every backend chain (stop sequence →
 * coordinator/reader → websocket → machine → opener → pool) and REST client.</p>
 */
@Service
public class GoLibrespotPlugin extends PluginEventHandler {

  private static final Logger log = LoggerFactory.getLogger(GoLibrespotPlugin.class);
  private static final LogSanitizer sanitizer = LogSanitizer.defaults();
  private static final String CONFIG_PREFIX = "plugins.golibrespot";
  private static final java.util.regex.Pattern USERINFO_PATTERN =
      java.util.regex.Pattern.compile("(?i)(https?://)([^/@\\s]+)@");

  private final GoLibrespotConfig config;
  private final ConfigFactory.Runtime runtime;
  private final PlayerLifecycleBridge bridge = new PlayerLifecycleBridge();

  @Autowired
  public GoLibrespotPlugin(Environment environment) {
    this.config = bindConfig(environment);
    checkFifoExistencePolicy(config);
    this.runtime = ConfigFactory.build(config, line -> log.debug("{}", line));
    log.info("go-librespot plugin loaded: {} backend(s) configured, {}",
        config.getBackends().size(),
        config.isEnabled() ? "spotify source enabled" : "disabled");
  }

  // ------------------------------------------------------------ config binding

  /**
   * Binds {@code plugins.golibrespot} into the T4 config and enforces the
   * startup-fatal structural rules. Any failure throws and aborts the Spring
   * context (Lavalink refuses to start with an invalid plugin config).
   */
  private static GoLibrespotConfig bindConfig(Environment environment) {
    Map<String, Object> raw = ConfigBinding.subtree(environment, CONFIG_PREFIX);
    if (raw == null || raw.isEmpty()) {
      // F6: an ABSENT plugins.golibrespot config must not abort Lavalink
      // startup — Lavalink auto-loads every plugin JAR, so an unconfigured
      // plugin no-ops gracefully as disabled with zero backends.
      log.warn("plugins.golibrespot is not configured: the plugin is DISABLED "
          + "(no backends; spdirect tracks will not load)");
      return GoLibrespotConfig.from(Map.of("enabled", false));
    }
    GoLibrespotConfig config;
    try {
      config = GoLibrespotConfig.from(raw);
    } catch (IllegalArgumentException e) {
      // binding errors (unknown keys / unparseable values) name the field
      throw new IllegalStateException(
          "go-librespot configuration binding failed: " + sanitizeConfigMessage(String.valueOf(e.getMessage())), e);
    }
    List<String> failures = GoLibrespotConfigValidator.validate(config);
    if (!failures.isEmpty()) {
      String joined = String.join("; ", failures);
      String sanitized = sanitizeConfigMessage(joined);
      log.error("go-librespot configuration is invalid: {}", sanitized);
      throw new IllegalStateException("go-librespot configuration is invalid: " + sanitized);
    }
    return config;
  }

  /**
   * F3: sanitizes a config message destined for a log line or the startup-abort
   * exception: the default {@link LogSanitizer} rules plus URI-userinfo
   * redaction (the default credential rules do not cover {@code user:pass@host}).
   */
  private static String sanitizeConfigMessage(String message) {
    String sanitized = sanitizer.sanitize(message);
    return USERINFO_PATTERN.matcher(sanitized).replaceAll("$1***@");
  }

  /**
   * The {@code fifoCheck} policy (T4): {@code fail} makes a missing FIFO
   * startup-fatal (the operator explicitly opted into that), {@code warn}
   * (default) logs a warning and keeps the plugin up — the FIFO is created by
   * the deployment sidecar and may legitimately not exist yet at boot.
   */
  private static void checkFifoExistencePolicy(GoLibrespotConfig config) {
    for (var backend : config.getBackends()) {
      Path path = backend.getFifoPath();
      if (path != null && Files.notExists(path)) {
        String msg = "backend '" + backend.getName() + "' FIFO '" + path + "' does not exist yet";
        if (config.getFifoCheck() == GoLibrespotConfig.FifoCheck.FAIL) {
          log.error("go-librespot fifoCheck=fail: {}", sanitizer.sanitize(msg));
          throw new IllegalStateException("go-librespot fifoCheck=fail: " + msg);
        }
        log.warn("go-librespot fifoCheck=warn (degraded): {}", sanitizer.sanitize(msg));
      }
    }
  }

  // ------------------------------------------------------------ Spring beans

  /**
   * The shared pool, metadata resolver and coordinator factory the
   * {@code @Service} {@link GoLibrespotAudioSourceManager} auto-wires
   * (component scan constructs it from these beans). These are the exact
   * instances the runtime owns, so teardown reaches the same objects.
   */
  @Bean
  public ExclusivePool golibrespotPool() {
    return runtime.pool();
  }

  @Bean
  public MetadataResolver golibrespotMetadataResolver() {
    return runtime.metadataResolver();
  }

  @Bean
  public CoordinatorFactory golibrespotCoordinatorFactory() {
    return runtime.coordinatorFactory();
  }

  // ------------------------------------------------------------ player lifecycle

  @Override
  public void onNewPlayer(ISocketContext socketContext, IPlayer player) {
    bridge.attach(player);
  }

  @Override
  public void onDestroyPlayer(ISocketContext socketContext, IPlayer player) {
    bridge.detach(player);
  }

  // ------------------------------------------------------------ teardown

  /** Closes every backend chain, the websockets, the pool and the REST clients. Idempotent. */
  @PreDestroy
  public void shutdown() {
    log.info("go-librespot plugin shutting down: closing backend chains, websockets and pool");
    runtime.coordinatorFactory().close();
  }
}
