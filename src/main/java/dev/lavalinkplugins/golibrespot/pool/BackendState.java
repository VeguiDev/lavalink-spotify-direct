package dev.lavalinkplugins.golibrespot.pool;

/**
 * Lifecycle state of a backend inside the {@link ExclusivePool}.
 *
 * <ul>
 *   <li>{@link #READY} — healthy and eligible; the pool hands it out on acquire.</li>
 *   <li>{@link #LEASED} — an outstanding lease exists; never handed out again
 *       and never stolen.</li>
 *   <li>{@link #QUARANTINING} — transient quarantine (e.g. repeated WS failures).
 *       Not eligible; re-admitted via {@code markReady} after the backend proves
 *       healthy again (fresh WS + idle status + FIFO reopen — T14's job).</li>
 *   <li>{@link #DEGRADED} — process-permanent quarantine (stop-taint or
 *       contradictory state per DECISIONS.md). Not eligible for the rest of the
 *       process lifetime; {@code markReady} refuses to re-admit.</li>
 *   <li>{@link #DEAD} — terminal (pool shutdown). All operations no-op.</li>
 * </ul>
 */
public enum BackendState {
    READY,
    LEASED,
    QUARANTINING,
    DEGRADED,
    DEAD
}
