package dev.lavalinkplugins.golibrespot.config;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * T4 — config binding + startup-fatal validation.
 *
 * <p>Binds plain {@code Map}s shaped like the YAML object under
 * {@code plugins.golibrespot}. The validator must be pure (no FIFO stat, no
 * Spring context) and return a typed failure list.</p>
 */
class GoLibrespotConfigTest {

    private static String absoluteFifo() {
        return Path.of(".").toAbsolutePath().normalize().resolve("pipe.sock").toString();
    }

    private static Map<String, Object> backend(String name, String restBaseUrl, String fifoPath) {
        return Map.of("name", name, "restBaseUrl", restBaseUrl, "fifoPath", fifoPath);
    }

    // ---------------------------------------------------------------- binding

    @Test
    void validConfigBindsWithDefaults() {
        Map<String, Object> yaml = Map.of(
                "backends", List.of(backend("primary", "http://localhost:8888", absoluteFifo())));

        GoLibrespotConfig config = GoLibrespotConfig.from(yaml);

        assertThat(config.isEnabled()).isTrue();
        assertThat(config.getFifoCheck()).isEqualTo(GoLibrespotConfig.FifoCheck.WARN);
        assertThat(config.getRestTimeoutMs()).isEqualTo(GoLibrespotConfig.ConfigDefaults.REST_TIMEOUT_MS);
        assertThat(config.getMetadataTimeoutMs()).isEqualTo(GoLibrespotConfig.ConfigDefaults.METADATA_TIMEOUT_MS);
        assertThat(config.getActivationTimeoutMs()).isEqualTo(GoLibrespotConfig.ConfigDefaults.ACTIVATION_TIMEOUT_MS);
        assertThat(config.getSeekTimeoutMs()).isEqualTo(GoLibrespotConfig.ConfigDefaults.SEEK_TIMEOUT_MS);
        assertThat(config.getDrainTimeoutMs()).isEqualTo(GoLibrespotConfig.ConfigDefaults.DRAIN_TIMEOUT_MS);
        assertThat(config.getDrainByteCap()).isEqualTo(GoLibrespotConfig.ConfigDefaults.DRAIN_BYTE_CAP);
        assertThat(config.getWsReconnectInitialMs()).isEqualTo(GoLibrespotConfig.ConfigDefaults.WS_RECONNECT_INITIAL_MS);
        assertThat(config.getWsReconnectMaxMs()).isEqualTo(GoLibrespotConfig.ConfigDefaults.WS_RECONNECT_MAX_MS);
        assertThat(config.getWsFailuresBeforeQuarantine())
                .isEqualTo(GoLibrespotConfig.ConfigDefaults.WS_FAILURES_BEFORE_QUARANTINE);
        assertThat(config.getPoolAcquireTimeoutMs()).isEqualTo(GoLibrespotConfig.ConfigDefaults.POOL_ACQUIRE_TIMEOUT_MS);
        assertThat(GoLibrespotConfigValidator.validate(config)).isEmpty();
    }

    @Test
    void explicitGlobalValuesAreRespected() {
        Map<String, Object> yaml = Map.ofEntries(
                Map.entry("enabled", false),
                Map.entry("fifoCheck", "fail"),
                Map.entry("restTimeoutMs", 100),
                Map.entry("metadataTimeoutMs", 200),
                Map.entry("activationTimeoutMs", 300),
                Map.entry("seekTimeoutMs", 400),
                Map.entry("drainTimeoutMs", 500),
                Map.entry("drainByteCap", 1024),
                Map.entry("wsReconnectInitialMs", 50),
                Map.entry("wsReconnectMaxMs", 5000),
                Map.entry("wsFailuresBeforeQuarantine", 2),
                Map.entry("poolAcquireTimeoutMs", 900),
                Map.entry("backends", List.of(backend("primary", "http://localhost:8888", absoluteFifo()))));

        GoLibrespotConfig config = GoLibrespotConfig.from(yaml);

        assertThat(config.isEnabled()).isFalse();
        assertThat(config.getFifoCheck()).isEqualTo(GoLibrespotConfig.FifoCheck.FAIL);
        assertThat(config.getRestTimeoutMs()).isEqualTo(100);
        assertThat(config.getMetadataTimeoutMs()).isEqualTo(200);
        assertThat(config.getActivationTimeoutMs()).isEqualTo(300);
        assertThat(config.getSeekTimeoutMs()).isEqualTo(400);
        assertThat(config.getDrainTimeoutMs()).isEqualTo(500);
        assertThat(config.getDrainByteCap()).isEqualTo(1024);
        assertThat(config.getWsReconnectInitialMs()).isEqualTo(50);
        assertThat(config.getWsReconnectMaxMs()).isEqualTo(5000);
        assertThat(config.getWsFailuresBeforeQuarantine()).isEqualTo(2);
        assertThat(config.getPoolAcquireTimeoutMs()).isEqualTo(900);
        assertThat(GoLibrespotConfigValidator.validate(config)).isEmpty();
    }

    @Test
    void wsUrlIsDerivedFromRestBaseUrl() {
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "backends", List.of(backend("primary", "http://localhost:8888", absoluteFifo()))));

        assertThat(config.getBackends().get(0).getWsUrl()).isEqualTo("ws://localhost:8888/events");
    }

    @Test
    void wsUrlDerivationHandlesHttpsAndTrailingSlash() {
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "backends", List.of(backend("secure", "https://daemon.internal:443/", absoluteFifo()))));

        assertThat(config.getBackends().get(0).getWsUrl()).isEqualTo("wss://daemon.internal:443/events");
    }

    @Test
    void explicitWsUrlWinsOverDerivation() {
        Map<String, Object> backend = new java.util.HashMap<>(
                backend("primary", "http://localhost:8888", absoluteFifo()));
        backend.put("wsUrl", "ws://custom:9999/events");

        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of("backends", List.of(backend)));

        assertThat(config.getBackends().get(0).getWsUrl()).isEqualTo("ws://custom:9999/events");
    }

    @Test
    void unknownTopLevelKeyFailsBinding() {
        Map<String, Object> yaml = Map.of(
                "restTimeutMs", 100, // typo
                "backends", List.of(backend("primary", "http://localhost:8888", absoluteFifo())));

        assertThatThrownBy(() -> GoLibrespotConfig.from(yaml))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("restTimeutMs");
    }

    @Test
    void unknownBackendKeyFailsBinding() {
        Map<String, Object> backend = new java.util.HashMap<>(
                backend("primary", "http://localhost:8888", absoluteFifo()));
        backend.put("fifoPaht", absoluteFifo()); // typo

        assertThatThrownBy(() -> GoLibrespotConfig.from(Map.of("backends", List.of(backend))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fifoPaht");
    }

    @Test
    void nonNumericTimeoutFailsBinding() {
        assertThatThrownBy(() -> GoLibrespotConfig.from(Map.of(
                "restTimeoutMs", "five",
                "backends", List.of(backend("primary", "http://localhost:8888", absoluteFifo())))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("restTimeoutMs");
    }

    // ---------------------------------------------------------------- validation

    @Test
    void duplicateBackendNamesFail() {
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "backends", List.of(
                        backend("dup", "http://localhost:8888", absoluteFifo()),
                        backend("dup", "http://localhost:9999", absoluteFifo()))));

        List<String> failures = GoLibrespotConfigValidator.validate(config);

        assertThat(failures).isNotEmpty()
                .anySatisfy(msg -> assertThat(msg).contains("duplicate").contains("dup"));
    }

    @Test
    void invalidRestBaseUrlFails() {
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "backends", List.of(backend("primary", "not a url", absoluteFifo()))));

        List<String> failures = GoLibrespotConfigValidator.validate(config);

        assertThat(failures).anySatisfy(msg -> assertThat(msg).contains("restBaseUrl"));
    }

    @Test
    void nonHttpRestBaseUrlFails() {
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "backends", List.of(backend("primary", "ftp://files.example.com/pub", absoluteFifo()))));

        List<String> failures = GoLibrespotConfigValidator.validate(config);

        assertThat(failures).anySatisfy(msg -> assertThat(msg).contains("restBaseUrl"));
    }

    @Test
    void relativeFifoPathFails() {
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "backends", List.of(backend("primary", "http://localhost:8888", "relative/pipe.sock"))));

        List<String> failures = GoLibrespotConfigValidator.validate(config);

        assertThat(failures).anySatisfy(msg -> assertThat(msg).contains("fifoPath").contains("absolute"));
    }

    @Test
    void blankFifoPathFails() {
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "backends", List.of(backend("primary", "http://localhost:8888", ""))));

        List<String> failures = GoLibrespotConfigValidator.validate(config);

        assertThat(failures).anySatisfy(msg -> assertThat(msg).contains("fifoPath"));
    }

    @Test
    void nonPositiveGlobalTimeoutFails() {
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "restTimeoutMs", 0,
                "activationTimeoutMs", -1,
                "backends", List.of(backend("primary", "http://localhost:8888", absoluteFifo()))));

        List<String> failures = GoLibrespotConfigValidator.validate(config);

        assertThat(failures).anySatisfy(msg -> assertThat(msg).contains("restTimeoutMs"));
        assertThat(failures).anySatisfy(msg -> assertThat(msg).contains("activationTimeoutMs"));
    }

    @Test
    void nonPositiveBackendOverrideFails() {
        Map<String, Object> backend = new java.util.HashMap<>(
                backend("primary", "http://localhost:8888", absoluteFifo()));
        backend.put("drainByteCap", 0);

        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of("backends", List.of(backend)));

        List<String> failures = GoLibrespotConfigValidator.validate(config);

        assertThat(failures).anySatisfy(msg -> assertThat(msg).contains("drainByteCap"));
    }

    @Test
    void unknownFifoCheckFailsBinding() {
        assertThatThrownBy(() -> GoLibrespotConfig.from(Map.of("fifoCheck", "shout")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fifoCheck");
    }

    @Test
    void emptyBackendsWhenEnabledFails() {
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of("enabled", true));

        List<String> failures = GoLibrespotConfigValidator.validate(config);

        assertThat(failures).anySatisfy(msg -> assertThat(msg).contains("backends"));
    }

    @Test
    void emptyBackendsWhenDisabledIsValid() {
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of("enabled", false));

        assertThat(GoLibrespotConfigValidator.validate(config)).isEmpty();
    }

    @Test
    void missingBackendNameFails() {
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "backends", List.of(Map.of(
                        "restBaseUrl", "http://localhost:8888",
                        "fifoPath", absoluteFifo()))));

        List<String> failures = GoLibrespotConfigValidator.validate(config);

        assertThat(failures).anySatisfy(msg -> assertThat(msg).contains("name"));
    }

    @Test
    void missingRestBaseUrlFails() {
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "backends", List.of(Map.of(
                        "name", "primary",
                        "fifoPath", absoluteFifo()))));

        List<String> failures = GoLibrespotConfigValidator.validate(config);

        assertThat(failures).anySatisfy(msg -> assertThat(msg).contains("restBaseUrl"));
    }

    // ---------------------------------------------------------------- overrides

    @Test
    void perBackendOverrideWinsOverGlobal() {
        Map<String, Object> withOverride = new java.util.HashMap<>(
                backend("fast", "http://localhost:8888", absoluteFifo()));
        withOverride.put("restTimeoutMs", 1234);

        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "restTimeoutMs", 5000,
                "backends", List.of(
                        withOverride,
                        backend("plain", "http://localhost:9999", absoluteFifo()))));

        BackendConfig fast = config.getBackends().get(0);
        BackendConfig plain = config.getBackends().get(1);

        assertThat(config.effectiveRestTimeoutMs(fast)).isEqualTo(1234);
        assertThat(config.effectiveRestTimeoutMs(plain)).isEqualTo(5000);
    }

    @Test
    void otherOverridesResolveToo() {
        Map<String, Object> backend = new java.util.HashMap<>(
                backend("primary", "http://localhost:8888", absoluteFifo()));
        backend.put("drainByteCap", 8192);

        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "drainByteCap", 4194304,
                "backends", List.of(backend)));

        BackendConfig b = config.getBackends().get(0);

        assertThat(config.effectiveDrainByteCap(b)).isEqualTo(8192);
    }

    // ---------------------------------------------------------------- FIFO policy

    @Test
    void fifoExistenceIsNotCheckedByValidator() {
        // Absolute path that certainly does not exist — must still validate.
        GoLibrespotConfig config = GoLibrespotConfig.from(Map.of(
                "backends", List.of(backend(
                        "primary",
                        "http://localhost:8888",
                        Path.of(".").toAbsolutePath().normalize().resolve("definitely-not-created-" + System.nanoTime() + ".sock").toString()))));

        assertThat(GoLibrespotConfigValidator.validate(config)).isEmpty();
    }
}
