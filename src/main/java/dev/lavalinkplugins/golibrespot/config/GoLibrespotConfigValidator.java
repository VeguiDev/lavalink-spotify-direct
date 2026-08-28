package dev.lavalinkplugins.golibrespot.config;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Hand-written startup-fatal validation for {@link GoLibrespotConfig}.
 *
 * <p>The source of truth for config correctness: enforcement is done here (not
 * via Jakarta bean-validation annotations, whose API is not guaranteed on the
 * classpath) and returns a typed failure list — empty means the config is
 * valid. Explicitly does NOT check FIFO existence: startup must tolerate the
 * deployment race where the pipe is created by the daemon sidecar after the
 * plugin boots (see {@code fifoCheck warn|fail} policy).</p>
 */
public final class GoLibrespotConfigValidator {

    private GoLibrespotConfigValidator() {
    }

    /**
     * Validates a bound config.
     *
     * @param config the config to check; {@code null} yields a single failure
     * @return empty list when valid, otherwise one message per problem, each
     *         naming the offending field
     */
    public static List<String> validate(GoLibrespotConfig config) {
        List<String> failures = new ArrayList<>();
        if (config == null) {
            failures.add("config must not be null");
            return failures;
        }

        if (config.isEnabled() && config.getBackends().isEmpty()) {
            failures.add("'backends' must not be empty when 'enabled' is true");
        }

        Set<String> seenNames = new HashSet<>();
        for (BackendConfig backend : config.getBackends()) {
            String name = backend.getName();
            if (name != null && !name.isBlank() && !seenNames.add(name)) {
                failures.add("duplicate backend name '" + name + "'");
            }
        }

        checkPositive(config.getRestTimeoutMs(), "restTimeoutMs", failures);
        checkPositive(config.getMetadataTimeoutMs(), "metadataTimeoutMs", failures);
        checkPositive(config.getActivationTimeoutMs(), "activationTimeoutMs", failures);
        checkPositive(config.getSeekTimeoutMs(), "seekTimeoutMs", failures);
        checkPositive(config.getDrainTimeoutMs(), "drainTimeoutMs", failures);
        checkPositive(config.getDrainByteCap(), "drainByteCap", failures);
        checkPositive(config.getWsReconnectInitialMs(), "wsReconnectInitialMs", failures);
        checkPositive(config.getWsReconnectMaxMs(), "wsReconnectMaxMs", failures);
        checkPositive(config.getWsFailuresBeforeQuarantine(), "wsFailuresBeforeQuarantine", failures);
        checkPositive(config.getPoolAcquireTimeoutMs(), "poolAcquireTimeoutMs", failures);

        List<BackendConfig> backends = config.getBackends();
        for (int i = 0; i < backends.size(); i++) {
            validateBackend(backends.get(i), "backends[" + i + "]", failures);
        }

        return failures;
    }

    /** Convenience: {@code true} when {@link #validate} returns no failures. */
    public static boolean isValid(GoLibrespotConfig config) {
        return validate(config).isEmpty();
    }

    private static void validateBackend(BackendConfig backend, String prefix, List<String> failures) {
        if (backend.getName() == null || backend.getName().isBlank()) {
            failures.add(prefix + ".name must not be blank");
        }
        if (backend.getRestBaseUrl() == null || backend.getRestBaseUrl().isBlank()) {
            failures.add(prefix + ".restBaseUrl must not be blank");
        } else if (!isHttpUrl(backend.getRestBaseUrl())) {
            failures.add(prefix + ".restBaseUrl must be a valid http(s) URL: '" + backend.getRestBaseUrl() + "'");
        }
        if (backend.getFifoPath() == null || backend.getFifoPath().toString().isBlank()) {
            failures.add(prefix + ".fifoPath must be set");
        } else if (!backend.getFifoPath().isAbsolute()) {
            failures.add(prefix + ".fifoPath must be an absolute path: '" + backend.getFifoPath() + "'");
        }

        checkPositive(backend.getRestTimeoutMs(), prefix + ".restTimeoutMs", failures);
        checkPositive(backend.getMetadataTimeoutMs(), prefix + ".metadataTimeoutMs", failures);
        checkPositive(backend.getActivationTimeoutMs(), prefix + ".activationTimeoutMs", failures);
        checkPositive(backend.getSeekTimeoutMs(), prefix + ".seekTimeoutMs", failures);
        checkPositive(backend.getDrainTimeoutMs(), prefix + ".drainTimeoutMs", failures);
        checkPositive(backend.getDrainByteCap(), prefix + ".drainByteCap", failures);
        checkPositive(backend.getWsReconnectInitialMs(), prefix + ".wsReconnectInitialMs", failures);
        checkPositive(backend.getWsReconnectMaxMs(), prefix + ".wsReconnectMaxMs", failures);
        checkPositive(backend.getWsFailuresBeforeQuarantine(), prefix + ".wsFailuresBeforeQuarantine", failures);
        checkPositive(backend.getPoolAcquireTimeoutMs(), prefix + ".poolAcquireTimeoutMs", failures);
    }

    private static void checkPositive(Integer value, String field, List<String> failures) {
        if (value != null && value <= 0) {
            failures.add(field + " must be a positive number (got " + value + ")");
        }
    }

    private static boolean isHttpUrl(String url) {
        try {
            URI uri = new URI(url);
            String scheme = uri.getScheme();
            return ("http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme)) && uri.getHost() != null;
        } catch (URISyntaxException e) {
            return false;
        }
    }
}
