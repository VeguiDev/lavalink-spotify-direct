package dev.lavalinkplugins.golibrespot.config;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/**
 * Immutable single-backend configuration: {@code {name, restBaseUrl, wsUrl,
 * fifoPath}} plus optional per-backend timeout overrides.
 *
 * <p>{@code wsUrl} is optional and, when absent, is derived from
 * {@code restBaseUrl} by swapping {@code http→ws}/{@code https→wss} and
 * appending {@code /events}. Per-backend override fields are nullable
 * {@link Integer}s; resolution against the globals happens on
 * {@link GoLibrespotConfig} (e.g. {@code effectiveRestTimeoutMs}).</p>
 */
public final class BackendConfig {

    private static final Set<String> KNOWN_KEYS = Set.of(
            "name", "restBaseUrl", "wsUrl", "fifoPath",
            "restTimeoutMs", "metadataTimeoutMs", "activationTimeoutMs", "seekTimeoutMs",
            "drainTimeoutMs", "drainByteCap", "wsReconnectInitialMs", "wsReconnectMaxMs",
            "wsFailuresBeforeQuarantine", "poolAcquireTimeoutMs");

    private final String name;
    private final String restBaseUrl;
    private final String wsUrl;
    private final Path fifoPath;
    private final Integer restTimeoutMs;
    private final Integer metadataTimeoutMs;
    private final Integer activationTimeoutMs;
    private final Integer seekTimeoutMs;
    private final Integer drainTimeoutMs;
    private final Integer drainByteCap;
    private final Integer wsReconnectInitialMs;
    private final Integer wsReconnectMaxMs;
    private final Integer wsFailuresBeforeQuarantine;
    private final Integer poolAcquireTimeoutMs;

    private BackendConfig(String name, String restBaseUrl, String wsUrl, Path fifoPath,
                          Integer restTimeoutMs, Integer metadataTimeoutMs, Integer activationTimeoutMs,
                          Integer seekTimeoutMs, Integer drainTimeoutMs, Integer drainByteCap,
                          Integer wsReconnectInitialMs, Integer wsReconnectMaxMs,
                          Integer wsFailuresBeforeQuarantine, Integer poolAcquireTimeoutMs) {
        this.name = name;
        this.restBaseUrl = restBaseUrl;
        this.wsUrl = wsUrl;
        this.fifoPath = fifoPath;
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
    }

    /**
     * Binds one backend entry.
     *
     * @param map the backend object; {@code null} is treated as "missing"
     * @param pathPrefix dotted/indexed location for error messages, e.g. {@code backends[0]}
     */
    static BackendConfig from(Map<String, Object> map, String pathPrefix) {
        if (map == null) {
            throw new IllegalArgumentException(pathPrefix + " must be an object");
        }
        GoLibrespotConfig.rejectUnknownKeys(map, KNOWN_KEYS);

        String name = GoLibrespotConfig.asString(map.get("name"), pathPrefix + ".name", null);
        String restBaseUrl = GoLibrespotConfig.asString(map.get("restBaseUrl"), pathPrefix + ".restBaseUrl", null);
        String wsUrl = GoLibrespotConfig.asString(map.get("wsUrl"), pathPrefix + ".wsUrl", null);
        if (wsUrl == null) {
            wsUrl = deriveWsUrl(restBaseUrl);
        }
        Path fifoPath = asPath(map.get("fifoPath"), pathPrefix + ".fifoPath");

        return new BackendConfig(
                name,
                restBaseUrl,
                wsUrl,
                fifoPath,
                GoLibrespotConfig.asIntOrNull(map.get("restTimeoutMs"), pathPrefix + ".restTimeoutMs"),
                GoLibrespotConfig.asIntOrNull(map.get("metadataTimeoutMs"), pathPrefix + ".metadataTimeoutMs"),
                GoLibrespotConfig.asIntOrNull(map.get("activationTimeoutMs"), pathPrefix + ".activationTimeoutMs"),
                GoLibrespotConfig.asIntOrNull(map.get("seekTimeoutMs"), pathPrefix + ".seekTimeoutMs"),
                GoLibrespotConfig.asIntOrNull(map.get("drainTimeoutMs"), pathPrefix + ".drainTimeoutMs"),
                GoLibrespotConfig.asIntOrNull(map.get("drainByteCap"), pathPrefix + ".drainByteCap"),
                GoLibrespotConfig.asIntOrNull(map.get("wsReconnectInitialMs"), pathPrefix + ".wsReconnectInitialMs"),
                GoLibrespotConfig.asIntOrNull(map.get("wsReconnectMaxMs"), pathPrefix + ".wsReconnectMaxMs"),
                GoLibrespotConfig.asIntOrNull(map.get("wsFailuresBeforeQuarantine"),
                        pathPrefix + ".wsFailuresBeforeQuarantine"),
                GoLibrespotConfig.asIntOrNull(map.get("poolAcquireTimeoutMs"), pathPrefix + ".poolAcquireTimeoutMs"));
    }

    private static Path asPath(Object value, String path) {
        if (value == null) {
            return null;
        }
        String raw = GoLibrespotConfig.asString(value, path, null);
        try {
            return Path.of(raw);
        } catch (InvalidPathException e) {
            throw new IllegalArgumentException(path + " is not a valid path: '" + raw + "'", e);
        }
    }

    /**
     * Derives the events websocket URL from a REST base URL: {@code http→ws},
     * {@code https→wss}, append {@code /events}. Returns {@code null} when the
     * base URL is absent or not parseable as an http(s) URL (the validator
     * reports the base URL problem instead).
     */
    static String deriveWsUrl(String restBaseUrl) {
        if (restBaseUrl == null) {
            return null;
        }
        try {
            URI uri = new URI(restBaseUrl);
            String scheme = uri.getScheme();
            String wsScheme;
            if ("http".equalsIgnoreCase(scheme)) {
                wsScheme = "ws";
            } else if ("https".equalsIgnoreCase(scheme)) {
                wsScheme = "wss";
            } else {
                return null;
            }
            if (uri.getHost() == null) {
                return null;
            }
            StringBuilder wsUrl = new StringBuilder(wsScheme).append("://").append(uri.getHost());
            if (uri.getPort() != -1) {
                wsUrl.append(':').append(uri.getPort());
            }
            return wsUrl.append("/events").toString();
        } catch (URISyntaxException e) {
            return null;
        }
    }

    // ------------------------------------------------------------ getters

    public String getName() {
        return name;
    }

    public String getRestBaseUrl() {
        return restBaseUrl;
    }

    /** Explicitly configured or derived from {@code restBaseUrl}; may be {@code null} until validated. */
    public String getWsUrl() {
        return wsUrl;
    }

    public Path getFifoPath() {
        return fifoPath;
    }

    /** Optional override; {@code null} = use global {@code restTimeoutMs}. */
    public Integer getRestTimeoutMs() {
        return restTimeoutMs;
    }

    public Integer getMetadataTimeoutMs() {
        return metadataTimeoutMs;
    }

    public Integer getActivationTimeoutMs() {
        return activationTimeoutMs;
    }

    public Integer getSeekTimeoutMs() {
        return seekTimeoutMs;
    }

    public Integer getDrainTimeoutMs() {
        return drainTimeoutMs;
    }

    public Integer getDrainByteCap() {
        return drainByteCap;
    }

    public Integer getWsReconnectInitialMs() {
        return wsReconnectInitialMs;
    }

    public Integer getWsReconnectMaxMs() {
        return wsReconnectMaxMs;
    }

    public Integer getWsFailuresBeforeQuarantine() {
        return wsFailuresBeforeQuarantine;
    }

    public Integer getPoolAcquireTimeoutMs() {
        return poolAcquireTimeoutMs;
    }

    @Override
    public String toString() {
        return "BackendConfig{name='" + name + "', restBaseUrl='" + restBaseUrl + "', wsUrl='" + wsUrl
                + "', fifoPath=" + fifoPath + '}';
    }
}
