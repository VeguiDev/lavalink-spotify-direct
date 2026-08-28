package dev.lavalinkplugins.golibrespot.pool;

import dev.lavalinkplugins.golibrespot.config.BackendConfig;
import java.util.Objects;

/**
 * Immutable per-backend registration record managed by the {@link ExclusivePool}.
 *
 * <p>Carries the owning {@link BackendConfig} so later consumers (the backend
 * state machine and lifecycle coordinator) can reach the REST client, events
 * websocket and FIFO components of a granted backend through the lease.</p>
 */
public final class BackendHandle {

    private final String backendId;
    private final BackendConfig config;

    /**
     * @param backendId unique, non-blank backend identifier (the configured name)
     * @param config    the immutable backend configuration
     */
    public BackendHandle(String backendId, BackendConfig config) {
        if (backendId == null || backendId.isBlank()) {
            throw new IllegalArgumentException("backendId must not be blank");
        }
        this.backendId = backendId;
        this.config = Objects.requireNonNull(config, "config");
    }

    /** Builds a handle from a backend config, using its (non-blank) name as the id. */
    public static BackendHandle of(BackendConfig config) {
        Objects.requireNonNull(config, "config");
        String name = config.getName();
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("backend config name must not be blank");
        }
        return new BackendHandle(name, config);
    }

    public String getBackendId() {
        return backendId;
    }

    public BackendConfig getConfig() {
        return config;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof BackendHandle other && backendId.equals(other.backendId);
    }

    @Override
    public int hashCode() {
        return backendId.hashCode();
    }

    @Override
    public String toString() {
        return "BackendHandle{backendId='" + backendId + "'}";
    }
}
