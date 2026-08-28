package dev.lavalinkplugins.golibrespot.source;

import dev.lavalinkplugins.golibrespot.config.BackendConfig;
import dev.lavalinkplugins.golibrespot.pool.BackendHandle;
import java.util.Objects;

/**
 * Seam that produces the {@link PlaybackCoordinator} for one backend — the
 * production wiring (T19) returns a {@link CoordinatorBackedPlayback} over the
 * real T15 coordinator + T17 stop sequence; tests inject a fake. The manager
 * caches one coordinator per backend id and calls {@link #close()} on shutdown.
 *
 * <p>Functional interface with a no-op default {@link #close()} so a plain
 * lambda is a valid factory; the production factory overrides {@link #close()}
 * to tear down the coordinator chain it created.</p>
 */
@FunctionalInterface
public interface CoordinatorFactory {

  /**
   * Creates (or resolves) the playback coordinator driving {@code handle}.
   *
   * @param handle the backend this coordinator drives (id = config name)
   * @param config the backend's immutable configuration
   */
  PlaybackCoordinator create(BackendHandle handle, BackendConfig config);

  /**
   * Closes every coordinator this factory created. Default: no-op (function-backed
   * factories own nothing); the production factory tears down its chain here.
   */
  default void close() {
    // no-op for function-backed factories
  }

  /** Requires non-null args (defensive; called by the manager before delegation). */
  static void requireArgs(BackendHandle handle, BackendConfig config) {
    Objects.requireNonNull(handle, "handle");
    Objects.requireNonNull(config, "config");
  }
}
