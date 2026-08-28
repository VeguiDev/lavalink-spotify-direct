package dev.lavalinkplugins.golibrespot.pool;

import dev.lavalinkplugins.golibrespot.config.BackendConfig;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fair, exclusive, health-aware backend lease pool.
 *
 * <p>A backend is a play-time resource: tracks are played on exactly one backend
 * at a time, so ownership is granted through time-boxed, exclusive
 * {@link Lease}s. The pool never acquires at load time and never steals a
 * {@link BackendState#LEASED} backend.</p>
 *
 * <p>Guarantees:</p>
 * <ul>
 *   <li><b>Fair FIFO waiters</b> — blocked acquirers queue in arrival order and
 *       are granted, one at a time, exactly in that order as backends free up.
 *       A freed backend is handed directly to the queue head (never re-marked
 *       READY first), and {@link #markReady(String)} re-admission honors the
 *       queue too.</li>
 *   <li><b>Bounded acquire</b> — {@link #acquire(Duration)} blocks at most the
 *       given duration (default {@code poolAcquireTimeoutMs}, 30s) and returns
 *       {@link Optional#empty()} on timeout; it never hangs.</li>
 *   <li><b>Generation-stamped leases</b> — the per-backend generation
 *       increments on every acquire and every quarantine. A stale lease's
 *       release/event is rejected by generation, which is the only defense
 *       against replay of stale v0.9.0 daemon events (no ids, no sequence
 *       numbers): same-URI acquire→release→acquire can always distinguish the
 *       old lease from the new one.</li>
 *   <li><b>Idempotent CAS release</b> — release is a compare-and-set on the
 *       (state, generation, leaseId) tuple: double release, release after
 *       quarantine, and release of an invalidated lease are all safe no-ops
 *       that never resurrect the backend.</li>
 *   <li><b>Health-aware</b> — only {@link BackendState#READY} backends are ever
 *       handed out. Transient quarantine ({@code markQuarantined(id, false)})
 *       lands in {@link BackendState#QUARANTINING} and is re-admitted by
 *       {@link #markReady(String)}; permanent quarantine (stop-taint /
 *       contradictory state per DECISIONS.md) lands in
 *       {@link BackendState#DEGRADED} and stays out for the process lifetime.</li>
 *   <li><b>Clean shutdown</b> — {@link #shutdown()} marks every backend
 *       {@link BackendState#DEAD} (invalidating outstanding leases), wakes all
 *       queued waiters with an empty result, and drains + terminates the grant
 *       executor, so no pool threads leak.</li>
 * </ul>
 *
 * <p>Thread model: a single {@link ReentrantLock} protects all state
 * transitions and the waiter queue. Waiter wake-up is delivered through a
 * single-threaded <em>grant executor</em> (created internally, or injected for
 * testing) that shutdown drains before terminating.</p>
 */
public final class ExclusivePool implements AutoCloseable {

    /** Default acquire timeout in ms (mirrors {@code poolAcquireTimeoutMs} 30s). */
    public static final long DEFAULT_ACQUIRE_TIMEOUT_MS = 30_000;

    /**
     * Immutable per-backend slot. All transitions are CAS'd under the pool lock;
     * the (state, generation, leaseId) tuple is the release validity key.
     */
    private record Slot(BackendState state, long generation, long leaseId) {
        static Slot initial() {
            return new Slot(BackendState.READY, 0, 0);
        }
    }

    private static final class BackendHolder {
        final BackendHandle handle;
        final AtomicReference<Slot> slot = new AtomicReference<>(Slot.initial());

        BackendHolder(BackendHandle handle) {
            this.handle = handle;
        }
    }

    /**
     * A blocked acquirer queued FIFO. The grantor publishes the lease under the
     * pool lock; the grant executor then wakes the waiter. Timeout / shutdown
     * mark the waiter cancelled under the pool lock.
     */
    private static final class Waiter {
        private final CountDownLatch done = new CountDownLatch(1);
        volatile Lease lease; // set under the pool lock by the granting thread
        volatile boolean cancelled; // set under the pool lock (timeout / shutdown / interrupt)

        /**
         * Waits while the caller holds {@code lock} exactly once: releases it for
         * the wait and re-acquires before returning.
         *
         * @return {@code true} when woken by a grant delivery or shutdown,
         *         {@code false} on timeout
         */
        boolean await(ReentrantLock lock, Duration timeout) throws InterruptedException {
            lock.unlock();
            try {
                return done.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } finally {
                lock.lock();
            }
        }

        void wake() {
            done.countDown();
        }
    }

    private final ReentrantLock poolLock = new ReentrantLock();
    private final List<BackendHolder> holders = new ArrayList<>();
    private final Map<String, BackendHolder> byBackendId = new HashMap<>();
    private final Deque<Waiter> waiters = new ArrayDeque<>();
    private final AtomicLong leaseIds = new AtomicLong();
    private final ExecutorService grantExecutor;
    private final long defaultAcquireTimeoutMs;
    private int cursor; // round-robin over holders, guarded by poolLock
    private volatile boolean shutDown;

    /** Creates a pool with the default 30s acquire timeout and an internal grant executor. */
    public ExclusivePool(List<BackendConfig> backends) {
        this(backends, DEFAULT_ACQUIRE_TIMEOUT_MS, null);
    }

    /** Creates a pool with an explicit default acquire timeout and an internal grant executor. */
    public ExclusivePool(List<BackendConfig> backends, long defaultAcquireTimeoutMs) {
        this(backends, defaultAcquireTimeoutMs, null);
    }

    /**
     * Creates a pool with an explicit default acquire timeout and grant executor.
     *
     * <p>The pool takes ownership of {@code grantExecutor} and shuts it down
     * (draining pending wake-ups) in {@link #shutdown()}/{@link #close()}.</p>
     *
     * @param backends               registered backends (duplicate names rejected)
     * @param defaultAcquireTimeoutMs positive default for {@link #acquire()}
     * @param grantExecutor          single-threaded executor used to wake queued
     *                               waiters; {@code null} creates an internal one
     *                               (daemon thread named {@code golibrespot-pool-grant})
     */
    public ExclusivePool(List<BackendConfig> backends, long defaultAcquireTimeoutMs, ExecutorService grantExecutor) {
        Objects.requireNonNull(backends, "backends");
        if (defaultAcquireTimeoutMs <= 0) {
            throw new IllegalArgumentException("defaultAcquireTimeoutMs must be positive, got " + defaultAcquireTimeoutMs);
        }
        this.defaultAcquireTimeoutMs = defaultAcquireTimeoutMs;
        Set<String> seen = new HashSet<>();
        for (BackendConfig config : backends) {
            BackendHandle handle = BackendHandle.of(config);
            if (!seen.add(handle.getBackendId())) {
                throw new IllegalArgumentException("duplicate backend name: " + handle.getBackendId());
            }
            BackendHolder holder = new BackendHolder(handle);
            holders.add(holder);
            byBackendId.put(handle.getBackendId(), holder);
        }
        this.grantExecutor = grantExecutor != null ? grantExecutor
                : Executors.newSingleThreadExecutor(r -> {
                    Thread t = new Thread(r, "golibrespot-pool-grant");
                    t.setDaemon(true);
                    return t;
                });
    }

    // ------------------------------------------------------------ acquire

    /** Acquires a lease, waiting at most {@code defaultAcquireTimeoutMs}. */
    public Optional<Lease> acquire() throws InterruptedException {
        return acquire(Duration.ofMillis(defaultAcquireTimeoutMs));
    }

    /**
     * Acquires an exclusive lease on any {@link BackendState#READY} backend,
     * waiting at most {@code timeout}.
     *
     * <p>If no backend is free the caller joins the FIFO waiter queue; the
     * first waiter is granted as backends free up. On timeout the acquire
     * returns {@link Optional#empty()} (the waiter is cancelled and never
     * granted afterwards). After shutdown, acquires return empty immediately.</p>
     *
     * @return the granted lease, or empty on timeout / shutdown
     * @throws InterruptedException if interrupted while waiting
     */
    public Optional<Lease> acquire(Duration timeout) throws InterruptedException {
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must not be negative: " + timeout);
        }
        poolLock.lock();
        try {
            if (shutDown) {
                return Optional.empty();
            }
            BackendHolder free = findReady();
            if (free != null) {
                return Optional.of(grant(free, null));
            }
            Waiter waiter = new Waiter();
            waiters.addLast(waiter);
            boolean woken;
            try {
                woken = waiter.await(poolLock, timeout);
            } catch (InterruptedException e) {
                if (waiter.lease != null) {
                    // A grant raced the interrupt: the backend is already ours — take it.
                    Thread.currentThread().interrupt();
                    return Optional.of(waiter.lease);
                }
                waiter.cancelled = true;
                throw e;
            }
            if (waiter.lease != null) {
                return Optional.of(waiter.lease); // grant raced the timeout — still ours
            }
            if (woken) {
                return Optional.empty(); // shutdown wake without a grant
            }
            waiter.cancelled = true; // timed out — never granted afterwards
            return Optional.empty();
        } finally {
            poolLock.unlock();
        }
    }

    // ------------------------------------------------------------ release

    /**
     * Idempotent lease release. The first call for the current
     * (state, generation, leaseId) tuple returns the backend to the pool,
     * handing it straight to the FIFO queue head when waiters exist. Every
     * later call — double release, release after quarantine/shutdown, release
     * of a stale generation — is a no-op that never resurrects the backend.
     */
    void releaseInternal(Lease lease) {
        BackendHolder holder = byBackendId.get(lease.backend().getBackendId());
        if (holder == null) {
            return;
        }
        poolLock.lock();
        try {
            Slot cur = holder.slot.get();
            if (cur.state != BackendState.LEASED
                    || cur.generation != lease.generation()
                    || cur.leaseId != lease.leaseId()) {
                return; // idempotent no-op: double release or stale/invalidated lease
            }
            Waiter next = pollHead();
            if (next != null) {
                grant(holder, next); // hand the freed backend directly to the queue head
            } else {
                holder.slot.compareAndSet(cur, new Slot(BackendState.READY, cur.generation, cur.leaseId));
            }
        } finally {
            poolLock.unlock();
        }
    }

    // ------------------------------------------------------------ health transitions

    /**
     * Quarantines a backend, invalidating any outstanding lease.
     *
     * @param permanent {@code true} = stop-taint / contradictory state →
     *                  {@link BackendState#DEGRADED}, never re-admitted in this
     *                  process; {@code false} = transient →
     *                  {@link BackendState#QUARANTINING}, re-admissible via
     *                  {@link #markReady(String)}. DEGRADED and DEAD are sticky.
     */
    public void markQuarantined(String backendId, boolean permanent) {
        BackendHolder holder = requireHolder(backendId);
        poolLock.lock();
        try {
            Slot cur = holder.slot.get();
            if (cur.state == BackendState.DEAD || cur.state == BackendState.DEGRADED) {
                return; // terminal states never downgrade
            }
            BackendState nextState = permanent ? BackendState.DEGRADED : BackendState.QUARANTINING;
            // Generation bump invalidates any outstanding lease on this backend.
            holder.slot.set(new Slot(nextState, cur.generation + 1, 0));
        } finally {
            poolLock.unlock();
        }
    }

    /**
     * Re-admits a transiently quarantined backend (fresh WS + idle status +
     * FIFO reopen have confirmed health — T14's job). The re-admitted backend
     * is granted to the FIFO queue head first when waiters exist. No-op for
     * READY/LEASED backends and permanently for DEGRADED/DEAD ones.
     */
    public void markReady(String backendId) {
        BackendHolder holder = requireHolder(backendId);
        poolLock.lock();
        try {
            Slot cur = holder.slot.get();
            if (cur.state != BackendState.QUARANTINING) {
                return; // READY/LEASED = no-op; DEGRADED/DEAD = permanent, stays out
            }
            Waiter next = pollHead();
            if (next != null) {
                grant(holder, next); // re-admission still honors the FIFO queue
            } else {
                holder.slot.compareAndSet(cur, new Slot(BackendState.READY, cur.generation, 0));
            }
        } finally {
            poolLock.unlock();
        }
    }

    // ------------------------------------------------------------ inspection

    /** Current lifecycle state of a registered backend. */
    public BackendState stateOf(String backendId) {
        return requireHolder(backendId).slot.get().state;
    }

    /** Immutable snapshot of the registered backend handles, in registration order. */
    public List<BackendHandle> handles() {
        poolLock.lock();
        try {
            List<BackendHandle> out = new ArrayList<>(holders.size());
            for (BackendHolder holder : holders) {
                out.add(holder.handle);
            }
            return List.copyOf(out);
        } finally {
            poolLock.unlock();
        }
    }

    /** Number of non-cancelled waiters currently queued (test/diagnostic aid). */
    int waitingCount() {
        poolLock.lock();
        try {
            int count = 0;
            for (Waiter waiter : waiters) {
                if (!waiter.cancelled) {
                    count++;
                }
            }
            return count;
        } finally {
            poolLock.unlock();
        }
    }

    /** Whether {@link #shutdown()} has been called. */
    public boolean isShutdown() {
        return shutDown;
    }

    /** Whether {@code lease} is still the current owner of its backend. */
    boolean isActive(Lease lease) {
        BackendHolder holder = byBackendId.get(lease.backend().getBackendId());
        if (holder == null) {
            return false;
        }
        Slot cur = holder.slot.get();
        return cur.state == BackendState.LEASED
                && cur.generation == lease.generation()
                && cur.leaseId == lease.leaseId();
    }

    // ------------------------------------------------------------ shutdown

    /**
     * Shuts the pool down and drains it: every backend becomes
     * {@link BackendState#DEAD} (invalidating outstanding leases), all queued
     * waiters are woken with an empty result, and the grant executor is
     * terminated (pending wake-ups drained first). Idempotent.
     */
    public void shutdown() {
        poolLock.lock();
        try {
            if (shutDown) {
                return;
            }
            shutDown = true;
            for (BackendHolder holder : holders) {
                Slot cur = holder.slot.get();
                holder.slot.set(new Slot(BackendState.DEAD, cur.generation + 1, 0));
            }
            for (Waiter waiter : waiters) {
                waiter.cancelled = true;
                waiter.wake();
            }
            waiters.clear();
        } finally {
            poolLock.unlock();
        }
        grantExecutor.shutdown();
        try {
            grantExecutor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Alias for {@link #shutdown()}. */
    @Override
    public void close() {
        shutdown();
    }

    // ------------------------------------------------------------ internals

    /** First READY backend in round-robin order, or {@code null}. Caller holds the lock. */
    private BackendHolder findReady() {
        int n = holders.size();
        if (n == 0) {
            return null;
        }
        for (int i = 0; i < n; i++) {
            BackendHolder holder = holders.get((cursor + i) % n);
            if (holder.slot.get().state == BackendState.READY) {
                cursor = (cursor + i + 1) % n;
                return holder;
            }
        }
        return null;
    }

    /** FIFO head skipping cancelled waiters, or {@code null}. Caller holds the lock. */
    private Waiter pollHead() {
        while (!waiters.isEmpty()) {
            Waiter waiter = waiters.pollFirst();
            if (!waiter.cancelled) {
                return waiter;
            }
        }
        return null;
    }

    /**
     * Transitions a free backend (READY, released-LEASED, or QUARANTINING via
     * markReady — never a live lease) to LEASED with a fresh generation and
     * minted lease, and, for a queued waiter, publishes the lease under the
     * pool lock and schedules its wake-up on the grant executor. Caller holds
     * the lock.
     */
    private Lease grant(BackendHolder holder, Waiter waiter) {
        Slot cur = holder.slot.get();
        Slot next = new Slot(BackendState.LEASED, cur.generation + 1, leaseIds.incrementAndGet());
        if (!holder.slot.compareAndSet(cur, next)) {
            throw new IllegalStateException(
                    "concurrent state change while granting " + holder.handle.getBackendId());
        }
        Lease lease = new Lease(this, holder.handle, next.generation(), next.leaseId());
        if (waiter != null) {
            waiter.lease = lease;
            grantExecutor.execute(waiter::wake); // async wake-up; drained by shutdown
        }
        return lease;
    }

    private BackendHolder requireHolder(String backendId) {
        Objects.requireNonNull(backendId, "backendId");
        BackendHolder holder = byBackendId.get(backendId);
        if (holder == null) {
            throw new IllegalArgumentException("unknown backend: " + backendId);
        }
        return holder;
    }
}
