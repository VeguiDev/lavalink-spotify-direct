package dev.lavalinkplugins.golibrespot.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable configuration for the go-librespot plugin (namespace
 * {@code plugins.golibrespot}).
 *
 * <p>Bound from a plain {@link Map} shaped like the YAML object under
 * {@code plugins.golibrespot}. Binding is strict: unknown keys and unparseable
 * values are rejected with an {@link IllegalArgumentException} naming the
 * offending field (fail-on-unknown). Semantic startup-fatal rules — duplicate
 * backend names, malformed URLs, non-absolute FIFO paths, non-positive
 * timeouts, empty backends while {@code enabled=true} — are enforced by
 * {@link GoLibrespotConfigValidator}, which returns a typed failure list and
 * never touches the filesystem.</p>
 */
public final class GoLibrespotConfig {

    /**
     * Canonical defaults for every configurable value (mirrors the DECISIONS.md
     * constants table: REST 5s, metadata 5s, activation barrier 15s, pause-ack
     * 5s, seek-ack 10s, drain caps 5s/4MiB, WS backoff 1s→30s, quarantine at 5
     * consecutive failures, pool acquire 30s).
     */
    public interface ConfigDefaults {
        int REST_TIMEOUT_MS = 5_000;
        int METADATA_TIMEOUT_MS = 5_000;
        int ACTIVATION_TIMEOUT_MS = 15_000;
        int SEEK_TIMEOUT_MS = 10_000;
        int DRAIN_TIMEOUT_MS = 5_000;
        int DRAIN_BYTE_CAP = 4_194_304; // 4 MiB
        int WS_RECONNECT_INITIAL_MS = 1_000;
        int WS_RECONNECT_MAX_MS = 30_000;
        int WS_FAILURES_BEFORE_QUARANTINE = 5;
        int POOL_ACQUIRE_TIMEOUT_MS = 30_000;
        FifoCheck FIFO_CHECK = FifoCheck.WARN;
        boolean ENABLED = true;
    }

    /** FIFO existence policy: {@code warn} = warn/degraded, {@code fail} = startup-fatal. */
    public enum FifoCheck {
        WARN,
        FAIL
    }

    private static final Set<String> KNOWN_KEYS = Set.of(
            "enabled", "fifoCheck", "backends",
            "restTimeoutMs", "metadataTimeoutMs", "activationTimeoutMs", "seekTimeoutMs",
            "drainTimeoutMs", "drainByteCap", "wsReconnectInitialMs", "wsReconnectMaxMs",
            "wsFailuresBeforeQuarantine", "poolAcquireTimeoutMs");

    private final boolean enabled;
    private final List<BackendConfig> backends;
    private final int restTimeoutMs;
    private final int metadataTimeoutMs;
    private final int activationTimeoutMs;
    private final int seekTimeoutMs;
    private final int drainTimeoutMs;
    private final int drainByteCap;
    private final int wsReconnectInitialMs;
    private final int wsReconnectMaxMs;
    private final int wsFailuresBeforeQuarantine;
    private final int poolAcquireTimeoutMs;
    private final FifoCheck fifoCheck;

    private GoLibrespotConfig(boolean enabled, List<BackendConfig> backends, int restTimeoutMs,
                              int metadataTimeoutMs, int activationTimeoutMs, int seekTimeoutMs,
                              int drainTimeoutMs, int drainByteCap, int wsReconnectInitialMs,
                              int wsReconnectMaxMs, int wsFailuresBeforeQuarantine,
                              int poolAcquireTimeoutMs, FifoCheck fifoCheck) {
        this.enabled = enabled;
        this.backends = List.copyOf(backends);
        this.restTimeoutMs = restTimeoutMs;
        this.metadataTimeoutMs = metadataTimeoutMs;
        this.activationTimeoutMs = activationTimeoutMs;
        this.seekTimeoutMs = seekTimeoutMs;
        this.drainTimeoutMs = drainTimeoutMs;
        this.drainByteCap = drainByteCap;
        this.wsReconnectInitialMs = wsReconnectInitialMs;
        this.wsReconnectMaxMs = wsReconnectMaxMs;
        this.wsFailuresBeforeQuarantine = wsFailuresBeforeQuarantine;
        this.poolAcquireTimeoutMs = poolAcquireTimeoutMs;
        this.fifoCheck = fifoCheck;
    }

    /**
     * Binds a config from the raw YAML-derived map.
     *
     * @param map the object under {@code plugins.golibrespot}
     * @throws IllegalArgumentException on unknown keys or unparseable values
     */
    public static GoLibrespotConfig from(Map<String, Object> map) {
        Objects.requireNonNull(map, "config map must not be null");
        rejectUnknownKeys(map, KNOWN_KEYS);

        boolean enabled = asBoolean(map.get("enabled"), "enabled", ConfigDefaults.ENABLED);
        FifoCheck fifoCheck = asFifoCheck(map.get("fifoCheck"), "fifoCheck", ConfigDefaults.FIFO_CHECK);
        List<BackendConfig> backends = parseBackends(map.get("backends"));

        return new GoLibrespotConfig(
                enabled,
                backends,
                asInt(map.get("restTimeoutMs"), "restTimeoutMs", ConfigDefaults.REST_TIMEOUT_MS),
                asInt(map.get("metadataTimeoutMs"), "metadataTimeoutMs", ConfigDefaults.METADATA_TIMEOUT_MS),
                asInt(map.get("activationTimeoutMs"), "activationTimeoutMs", ConfigDefaults.ACTIVATION_TIMEOUT_MS),
                asInt(map.get("seekTimeoutMs"), "seekTimeoutMs", ConfigDefaults.SEEK_TIMEOUT_MS),
                asInt(map.get("drainTimeoutMs"), "drainTimeoutMs", ConfigDefaults.DRAIN_TIMEOUT_MS),
                asInt(map.get("drainByteCap"), "drainByteCap", ConfigDefaults.DRAIN_BYTE_CAP),
                asInt(map.get("wsReconnectInitialMs"), "wsReconnectInitialMs", ConfigDefaults.WS_RECONNECT_INITIAL_MS),
                asInt(map.get("wsReconnectMaxMs"), "wsReconnectMaxMs", ConfigDefaults.WS_RECONNECT_MAX_MS),
                asInt(map.get("wsFailuresBeforeQuarantine"), "wsFailuresBeforeQuarantine",
                        ConfigDefaults.WS_FAILURES_BEFORE_QUARANTINE),
                asInt(map.get("poolAcquireTimeoutMs"), "poolAcquireTimeoutMs", ConfigDefaults.POOL_ACQUIRE_TIMEOUT_MS),
                fifoCheck);
    }

    // ------------------------------------------------------------ effective values

    /** Per-backend override wins over the global value. */
    public int effectiveRestTimeoutMs(BackendConfig backend) {
        return effective(backend.getRestTimeoutMs(), restTimeoutMs);
    }

    public int effectiveMetadataTimeoutMs(BackendConfig backend) {
        return effective(backend.getMetadataTimeoutMs(), metadataTimeoutMs);
    }

    public int effectiveActivationTimeoutMs(BackendConfig backend) {
        return effective(backend.getActivationTimeoutMs(), activationTimeoutMs);
    }

    public int effectiveSeekTimeoutMs(BackendConfig backend) {
        return effective(backend.getSeekTimeoutMs(), seekTimeoutMs);
    }

    public int effectiveDrainTimeoutMs(BackendConfig backend) {
        return effective(backend.getDrainTimeoutMs(), drainTimeoutMs);
    }

    public int effectiveDrainByteCap(BackendConfig backend) {
        return effective(backend.getDrainByteCap(), drainByteCap);
    }

    public int effectiveWsReconnectInitialMs(BackendConfig backend) {
        return effective(backend.getWsReconnectInitialMs(), wsReconnectInitialMs);
    }

    public int effectiveWsReconnectMaxMs(BackendConfig backend) {
        return effective(backend.getWsReconnectMaxMs(), wsReconnectMaxMs);
    }

    public int effectiveWsFailuresBeforeQuarantine(BackendConfig backend) {
        return effective(backend.getWsFailuresBeforeQuarantine(), wsFailuresBeforeQuarantine);
    }

    public int effectivePoolAcquireTimeoutMs(BackendConfig backend) {
        return effective(backend.getPoolAcquireTimeoutMs(), poolAcquireTimeoutMs);
    }

    private static int effective(Integer override, int global) {
        return override != null ? override : global;
    }

    // ------------------------------------------------------------ getters

    public boolean isEnabled() {
        return enabled;
    }

    public List<BackendConfig> getBackends() {
        return backends;
    }

    public int getRestTimeoutMs() {
        return restTimeoutMs;
    }

    public int getMetadataTimeoutMs() {
        return metadataTimeoutMs;
    }

    public int getActivationTimeoutMs() {
        return activationTimeoutMs;
    }

    public int getSeekTimeoutMs() {
        return seekTimeoutMs;
    }

    public int getDrainTimeoutMs() {
        return drainTimeoutMs;
    }

    public int getDrainByteCap() {
        return drainByteCap;
    }

    public int getWsReconnectInitialMs() {
        return wsReconnectInitialMs;
    }

    public int getWsReconnectMaxMs() {
        return wsReconnectMaxMs;
    }

    public int getWsFailuresBeforeQuarantine() {
        return wsFailuresBeforeQuarantine;
    }

    public int getPoolAcquireTimeoutMs() {
        return poolAcquireTimeoutMs;
    }

    public FifoCheck getFifoCheck() {
        return fifoCheck;
    }

    // ------------------------------------------------------------ binding helpers (package-private for BackendConfig)

    private static List<BackendConfig> parseBackends(Object value) {
        if (value == null) {
            return List.of();
        }
        if (!(value instanceof List<?> list)) {
            throw new IllegalArgumentException("invalid value for 'backends': expected a list");
        }
        List<BackendConfig> result = new ArrayList<>(list.size());
        for (int i = 0; i < list.size(); i++) {
            Map<String, Object> backendMap = asStringMap(list.get(i), "backends[" + i + "]");
            normalizeBackendUrls(backendMap);
            result.add(BackendConfig.from(backendMap, "backends[" + i + "]"));
        }
        return result;
    }

    /**
     * F5: normalizes trailing slashes on {@code restBaseUrl} (and an explicitly
     * configured {@code wsUrl}) at config parse time. A base ending in {@code /}
     * or {@code //} would otherwise produce double-slash request paths
     * ({@code http://host:port//player/play}) → 404 → spurious quarantine.
     */
    private static void normalizeBackendUrls(Map<String, Object> backendMap) {
        if (backendMap == null) {
            return;
        }
        Object rest = backendMap.get("restBaseUrl");
        if (rest instanceof String s) {
            backendMap.put("restBaseUrl", stripTrailingSlashes(s));
        }
        Object ws = backendMap.get("wsUrl");
        if (ws instanceof String s) {
            backendMap.put("wsUrl", stripTrailingSlashes(s));
        }
    }

    private static String stripTrailingSlashes(String url) {
        int end = url.length();
        while (end > 0 && url.charAt(end - 1) == '/') {
            end--;
        }
        return end == url.length() ? url : url.substring(0, end);
    }

    static void rejectUnknownKeys(Map<String, Object> map, Set<String> knownKeys) {
        for (String key : map.keySet()) {
            if (!knownKeys.contains(key)) {
                throw new IllegalArgumentException("unknown property '" + key + "'");
            }
        }
    }

    static Map<String, Object> asStringMap(Object value, String field) {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> raw) {
            Map<String, Object> result = new HashMap<>();
            for (Map.Entry<?, ?> entry : raw.entrySet()) {
                result.put(String.valueOf(entry.getKey()), entry.getValue());
            }
            return result;
        }
        throw new IllegalArgumentException("invalid value for '" + field + "': expected an object");
    }

    static int asInt(Object value, String field, int defaultValue) {
        Integer parsed = asIntOrNull(value, field);
        return parsed != null ? parsed : defaultValue;
    }

    static Integer asIntOrNull(Object value, String field) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String string) {
            try {
                return Integer.parseInt(string.trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid value for '" + field + "': '" + string + "'", e);
            }
        }
        throw new IllegalArgumentException(
                "invalid value for '" + field + "': expected a number, got " + value.getClass().getSimpleName());
    }

    static String asString(Object value, String field, String defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof String string) {
            return string;
        }
        throw new IllegalArgumentException(
                "invalid value for '" + field + "': expected a string, got " + value.getClass().getSimpleName());
    }

    static boolean asBoolean(Object value, String field, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof String string) {
            if ("true".equalsIgnoreCase(string)) {
                return true;
            }
            if ("false".equalsIgnoreCase(string)) {
                return false;
            }
        }
        throw new IllegalArgumentException("invalid value for '" + field + "': expected true or false");
    }

    static FifoCheck asFifoCheck(Object value, String field, FifoCheck defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof String string) {
            try {
                return FifoCheck.valueOf(string.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "invalid value for '" + field + "': '" + string + "' (expected warn or fail)", e);
            }
        }
        throw new IllegalArgumentException(
                "invalid value for '" + field + "': expected 'warn' or 'fail', got " + value.getClass().getSimpleName());
    }
}
