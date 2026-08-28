package dev.lavalinkplugins.golibrespot.pool;

/**
 * Exclusive, generation-stamped right to use one backend.
 *
 * <p>A lease is obtained from {@link ExclusivePool#acquire(java.time.Duration)}
 * and returned with {@link #release()}. Release is <em>idempotent</em>: a
 * second call, a release after the backend was quarantined or shut down, or a
 * release of a stale lease whose generation no longer matches the pool's
 * current one are all safe no-ops and never resurrect the backend.</p>
 *
 * <p>The generation is the only defense against stale v0.9.0 daemon events
 * (they carry no ids/sequence numbers): it increments on every acquire (and on
 * every quarantine), so a same-URI replay — acquire → release → acquire — can
 * always tell the old lease apart from the new one.</p>
 */
public final class Lease implements AutoCloseable {

    private final ExclusivePool pool;
    private final BackendHandle backend;
    private final long generation;
    private final long leaseId;

    /** Package-private: leases are minted by the pool only. */
    Lease(ExclusivePool pool, BackendHandle backend, long generation, long leaseId) {
        this.pool = pool;
        this.backend = backend;
        this.generation = generation;
        this.leaseId = leaseId;
    }

    /** The backend this lease grants exclusive access to. */
    public BackendHandle backend() {
        return backend;
    }

    /** Pool-side generation at grant time; stale leases carry a lower value. */
    public long generation() {
        return generation;
    }

    /** Pool-wide monotonic lease id; strictly increasing across grants. */
    public long leaseId() {
        return leaseId;
    }

    /**
     * Idempotent release. The first valid call returns the backend to the pool
     * (handing it straight to the FIFO queue head if someone is waiting); every
     * later call is a no-op.
     */
    public void release() {
        pool.releaseInternal(this);
    }

    /**
     * Whether this lease is still the current owner of its backend. {@code false}
     * after release, after a quarantine/shutdown that invalidated the lease, or
     * when a newer lease has been granted.
     */
    public boolean isActive() {
        return pool.isActive(this);
    }

    /** try-with-resources support; identical to {@link #release()}. */
    @Override
    public void close() {
        release();
    }

    @Override
    public String toString() {
        return "Lease{backend='" + backend.getBackendId() + "', generation=" + generation + ", leaseId=" + leaseId + '}';
    }
}
