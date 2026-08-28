package dev.lavalinkplugins.golibrespot.pool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import dev.lavalinkplugins.golibrespot.config.BackendConfig;
import dev.lavalinkplugins.golibrespot.config.GoLibrespotConfig;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Contract tests for {@link ExclusivePool}: fair FIFO waiter queue, bounded
 * acquire timeout, exclusive ownership, generation-stamped idempotent leases,
 * health-aware (READY-only) handouts, quarantine/degraded transitions,
 * never-steal semantics and a leak-free shutdown.
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class ExclusivePoolTest {

    private final List<ExclusivePool> pools = new ArrayList<>();

    @AfterEach
    void tearDown() {
        pools.forEach(ExclusivePool::shutdown);
        pools.clear();
    }

    // ------------------------------------------------------------ happy path

    @Test
    void acquireGrantsReadyBackendImmediately() {
        ExclusivePool pool = newPool(backends("alpha"));
        Optional<Lease> lease = acquireQuiet(pool, Duration.ofSeconds(5));
        assertThat(lease).isPresent();
        Lease l = lease.get();
        assertThat(l.backend().getBackendId()).isEqualTo("alpha");
        assertThat(l.generation()).isEqualTo(1);
        assertThat(l.leaseId()).isEqualTo(1);
        assertThat(l.isActive()).isTrue();
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);
        l.release();
        assertThat(l.isActive()).isFalse();
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.READY);
    }

    // ------------------------------------------------------------ bounded timeout

    @Test
    void acquireTimesOutWithinBoundWhenNoBackendsReady() {
        ExclusivePool pool = newPool(backends());
        long start = System.nanoTime();
        Optional<Lease> lease = acquireQuiet(pool, Duration.ofMillis(200));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(lease).isEmpty();
        assertThat(elapsedMs).isGreaterThanOrEqualTo(150L).isLessThan(5_000L);
    }

    @Test
    void acquireBlocksWhileLeasedAndTimesOutWithoutStealing() {
        ExclusivePool pool = newPool(backends("alpha"));
        Lease holder = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();
        long start = System.nanoTime();
        Optional<Lease> second = acquireQuiet(pool, Duration.ofMillis(200));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(second).isEmpty();
        assertThat(elapsedMs).isGreaterThanOrEqualTo(150L).isLessThan(5_000L);
        assertThat(holder.isActive()).isTrue(); // never stolen while held
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);
        holder.release();
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.READY);
    }

    @Test
    void defaultAcquireTimeoutHonored() {
        ExclusivePool pool = newPool(backends("alpha"), 250L, null);
        Lease holder = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();
        long start = System.nanoTime();
        assertThat(acquireQuiet(pool)).isEmpty(); // uses the 250ms default
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs).isGreaterThanOrEqualTo(150L).isLessThan(5_000L);
        holder.release();
    }

    // ------------------------------------------------------------ fairness

    @Test
    void grantsWaitersInFifoArrivalOrder() {
        ExclusivePool pool = newPool(backends("alpha"));
        Lease holder = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();

        List<String> grantOrder = Collections.synchronizedList(new ArrayList<>());
        Map<String, Lease> grants = new ConcurrentHashMap<>();

        // Launch B, wait until it is queued, then C, then D — pins arrival order B < C < D.
        Thread threadB = startWaiter("B", pool, grantOrder, grants);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(pool.waitingCount()).isEqualTo(1));
        Thread threadC = startWaiter("C", pool, grantOrder, grants);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(pool.waitingCount()).isEqualTo(2));
        Thread threadD = startWaiter("D", pool, grantOrder, grants);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(pool.waitingCount()).isEqualTo(3));

        holder.release(); // must hand off to B, the queue head
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(grantOrder).containsExactly("B"));
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);
        assertThat(pool.waitingCount()).isEqualTo(2);

        grants.get("B").release();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(grantOrder).containsExactly("B", "C"));

        grants.get("C").release();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(grantOrder).containsExactly("B", "C", "D"));

        grants.get("D").release();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.READY));
        awaitThreadExit(threadB);
        awaitThreadExit(threadC);
        awaitThreadExit(threadD);
    }

    @Test
    void handoffGrantsQueuedWaiterImmediatelyOnRelease() throws Exception {
        ExclusivePool pool = newPool(backends("alpha"));
        Lease holder = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();
        AtomicReference<Optional<Lease>> result = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            result.set(acquireQuiet(pool, Duration.ofSeconds(30)));
            done.countDown();
        }, "handoff-waiter");
        waiter.start();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(pool.waitingCount()).isEqualTo(1));

        holder.release();
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(result.get()).isPresent();
        Lease granted = result.get().get();
        assertThat(granted.backend().getBackendId()).isEqualTo("alpha");
        assertThat(granted.generation()).isEqualTo(2); // first acquire was generation 1
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);
        granted.release();
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.READY);
    }

    // ------------------------------------------------------------ exclusivity

    @Test
    void neverTwoHoldersOfSameBackendUnderStress() throws Exception {
        ExclusivePool pool = newPool(backends("alpha"));
        int threads = 4;
        int iterations = 50;
        AtomicBoolean inUse = new AtomicBoolean();
        AtomicInteger violations = new AtomicInteger();
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                for (int i = 0; i < iterations; i++) {
                    Optional<Lease> lease = acquireQuiet(pool, Duration.ofSeconds(10));
                    if (lease.isEmpty()) {
                        violations.incrementAndGet();
                        return;
                    }
                    if (!inUse.compareAndSet(false, true)) {
                        violations.incrementAndGet(); // two holders at once — pool stole or double-granted
                    }
                    LockSupport.parkNanos(Duration.ofMillis(1).toNanos());
                    inUse.set(false);
                    lease.get().release();
                }
            }, "stressor-" + t);
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join(Duration.ofSeconds(60).toMillis());
        }
        for (Thread worker : workers) {
            assertThat(worker.isAlive()).isFalse();
        }
        assertThat(violations).hasValue(0);
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.READY);
    }

    // ------------------------------------------------------------ idempotent release

    @Test
    void doubleReleaseIsIdempotentNoOp() {
        ExclusivePool pool = newPool(backends("alpha"));
        Lease lease = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();
        lease.release();
        lease.release(); // must not throw and must not resurrect the backend
        assertThat(lease.isActive()).isFalse();
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.READY);
        Optional<Lease> again = acquireQuiet(pool, Duration.ofSeconds(5));
        assertThat(again).isPresent();
        assertThat(again.get().generation()).isEqualTo(2);
    }

    @Test
    void releaseAfterQuarantineDoesNotResurrectBackend() {
        ExclusivePool pool = newPool(backends("alpha"));
        Lease lease = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();
        pool.markQuarantined("alpha", false);
        assertThat(lease.isActive()).isFalse(); // quarantine invalidated the lease
        lease.release(); // stale — must not resurrect
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.QUARANTINING);
        assertThat(acquireQuiet(pool, Duration.ofMillis(150))).isEmpty();
    }

    // ------------------------------------------------------------ generation / stale replay

    @Test
    void staleReleaseDoesNotAffectSecondLeaseSameBackend() {
        ExclusivePool pool = newPool(backends("alpha"));
        Lease first = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();
        first.release();
        Lease second = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();
        assertThat(second.generation()).isEqualTo(2);
        first.release(); // stale replay of the old release (same-URI scenario)
        assertThat(second.isActive()).isTrue(); // second lease unaffected
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.LEASED);
        second.release();
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.READY);
    }

    @Test
    void generationIncrementsOnEveryAcquire() {
        ExclusivePool pool = newPool(backends("alpha"));
        Lease l1 = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();
        l1.release();
        Lease l2 = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();
        l2.release();
        Lease l3 = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();
        assertThat(l1.generation()).isEqualTo(1);
        assertThat(l2.generation()).isEqualTo(2);
        assertThat(l3.generation()).isEqualTo(3);
        assertThat(l1.leaseId()).isLessThan(l2.leaseId());
        assertThat(l2.leaseId()).isLessThan(l3.leaseId());
        assertThat(l1.isActive()).isFalse();
        assertThat(l3.isActive()).isTrue();
    }

    // ------------------------------------------------------------ health awareness

    @Test
    void permanentQuarantineNeverReAdmitted() {
        ExclusivePool pool = newPool(backends("alpha"));
        pool.markQuarantined("alpha", true);
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.DEGRADED);
        assertThat(acquireQuiet(pool, Duration.ofMillis(150))).isEmpty();
        pool.markReady("alpha"); // re-admission must be refused for permanent quarantine
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.DEGRADED);
        assertThat(acquireQuiet(pool, Duration.ofMillis(150))).isEmpty();
        pool.markQuarantined("alpha", false); // transient quarantine must not downgrade DEGRADED
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.DEGRADED);
    }

    @Test
    void transientQuarantineReAdmittedByMarkReady() {
        ExclusivePool pool = newPool(backends("alpha"));
        pool.markQuarantined("alpha", false);
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.QUARANTINING);
        assertThat(acquireQuiet(pool, Duration.ofMillis(150))).isEmpty();
        pool.markReady("alpha");
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.READY);
        Optional<Lease> lease = acquireQuiet(pool, Duration.ofSeconds(5));
        assertThat(lease).isPresent();
        lease.get().release();
    }

    // ------------------------------------------------------------ shutdown

    @Test
    void shutdownMarksBackendsDeadAndRejectsAcquire() {
        ExclusivePool pool = newPool(backends("alpha"));
        Lease lease = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();
        pool.shutdown();
        assertThat(pool.isShutdown()).isTrue();
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.DEAD);
        assertThat(lease.isActive()).isFalse(); // outstanding lease invalidated
        long start = System.nanoTime();
        assertThat(acquireQuiet(pool, Duration.ofSeconds(5))).isEmpty(); // immediate, no wait
        assertThat((System.nanoTime() - start) / 1_000_000).isLessThan(500L);
        pool.markReady("alpha"); // no-op on DEAD
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.DEAD);
        lease.release(); // stale — no-op
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.DEAD);
        pool.shutdown(); // idempotent
        assertThat(pool.isShutdown()).isTrue();
    }

    @Test
    void shutdownDrainsQueuedWaitersAndTerminatesGrantExecutor() throws Exception {
        AtomicReference<Thread> grantThread = new AtomicReference<>();
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "test-grant");
            grantThread.set(t);
            return t;
        });
        ExclusivePool pool = newPool(backends("alpha"), 30_000L, executor);
        executor.submit(() -> { }).get(5, TimeUnit.SECONDS); // force the grant thread to exist
        Lease holder = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();

        AtomicReference<Optional<Lease>> result = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);
        Thread waiter = new Thread(() -> {
            result.set(acquireQuiet(pool, Duration.ofSeconds(60)));
            done.countDown();
        }, "drain-waiter");
        waiter.start();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(pool.waitingCount()).isEqualTo(1));

        pool.close(); // drains: queued waiter cancelled, grant executor terminated
        assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(result.get()).isEmpty();
        assertThat(executor.isTerminated()).isTrue();
        // The waiter's done-latch fires from the grant path the instant
        // acquire() returns — BEFORE the thread has finished unwinding, so
        // isAlive() right after the latch is a happens-before race. Await
        // actual termination first, then assert it (the pool must still
        // drain every waiter).
        awaitThreadExit(waiter);
        assertThat(holder.isActive()).isFalse();
        Thread grant = grantThread.get();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(grant.isAlive()).isFalse());
    }

    @Test
    void churnReusesSingleGrantThread() throws Exception {
        AtomicInteger created = new AtomicInteger();
        ExecutorService executor = Executors.newSingleThreadExecutor(r -> {
            created.incrementAndGet();
            Thread t = new Thread(r, "churn-grant");
            t.setDaemon(true);
            return t;
        });
        ExclusivePool pool = newPool(backends("alpha"), 30_000L, executor);
        int iterations = 300;
        AtomicInteger failures = new AtomicInteger();
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> workers = new ArrayList<>();
        for (int t = 0; t < 2; t++) {
            Thread worker = new Thread(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < iterations; i++) {
                    Optional<Lease> lease = acquireQuiet(pool, Duration.ofSeconds(30));
                    if (lease.isEmpty()) {
                        failures.incrementAndGet();
                        return;
                    }
                    LockSupport.parkNanos(10_000);
                    lease.get().release();
                }
            }, "churn-" + t);
            workers.add(worker);
            worker.start();
        }
        start.countDown();
        for (Thread worker : workers) {
            worker.join(Duration.ofSeconds(60).toMillis());
        }
        assertThat(failures).hasValue(0);
        assertThat(created.get()).isEqualTo(1); // grant executor never grew a second thread
        assertThat(pool.stateOf("alpha")).isEqualTo(BackendState.READY);
    }

    // ------------------------------------------------------------ validation

    @Test
    void unknownBackendRejectedOnMarkCalls() {
        ExclusivePool pool = newPool(backends("alpha"));
        assertThatThrownBy(() -> pool.markQuarantined("ghost", false)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> pool.markQuarantined("ghost", true)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> pool.markReady("ghost")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> pool.stateOf("ghost")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void duplicateBackendNamesRejected() {
        assertThatThrownBy(() -> new ExclusivePool(backends("alpha", "alpha")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate");
    }

    // ------------------------------------------------------------ lease lifecycle

    @Test
    void leaseIsActiveReflectsCurrentOwnership() {
        ExclusivePool pool = newPool(backends("alpha"));
        Lease lease = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();
        assertThat(lease.isActive()).isTrue();
        pool.markQuarantined("alpha", false);
        assertThat(lease.isActive()).isFalse(); // invalidated by quarantine
        lease.release();
        assertThat(lease.isActive()).isFalse();
        pool.markReady("alpha");
        Lease second = acquireQuiet(pool, Duration.ofSeconds(5)).orElseThrow();
        assertThat(second.isActive()).isTrue();
        assertThat(lease.isActive()).isFalse(); // old lease stays dead
        second.close(); // AutoCloseable path is release()
        assertThat(second.isActive()).isFalse();
    }

    // ------------------------------------------------------------ helpers

    /**
     * Awaits the actual termination of a worker thread before asserting on
     * its liveness: a done-latch / order-list observation only proves the
     * thread finished the observed work, not that it finished unwinding, so
     * checking isAlive() immediately is a happens-before race under load.
     * The assertion stays — a worker that genuinely hangs fails here.
     */
    private static void awaitThreadExit(Thread thread) {
        await().atMost(Duration.ofSeconds(5)).until(() -> !thread.isAlive());
        assertThat(thread.isAlive()).isFalse();
    }

    private static Thread startWaiter(String id, ExclusivePool pool, List<String> grantOrder, Map<String, Lease> grants) {
        Thread thread = new Thread(() -> {
            Optional<Lease> lease = acquireQuiet(pool, Duration.ofSeconds(30));
            lease.ifPresent(l -> {
                grants.put(id, l);
                grantOrder.add(id);
            });
        }, "waiter-" + id);
        thread.start();
        return thread;
    }

    private static Optional<Lease> acquireQuiet(ExclusivePool pool, Duration timeout) {
        try {
            return pool.acquire(timeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private static Optional<Lease> acquireQuiet(ExclusivePool pool) {
        try {
            return pool.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private ExclusivePool newPool(List<BackendConfig> backends) {
        return newPool(backends, 30_000L, null);
    }

    private ExclusivePool newPool(List<BackendConfig> backends, long timeoutMs, ExecutorService executor) {
        ExclusivePool pool = new ExclusivePool(backends, timeoutMs, executor);
        pools.add(pool);
        return pool;
    }

    private static List<BackendConfig> backends(String... names) {
        List<Map<String, Object>> raw = new ArrayList<>();
        for (int i = 0; i < names.length; i++) {
            String name = names[i];
            raw.add(Map.of(
                    "name", name,
                    "restBaseUrl", "http://127.0.0.1:" + (20_000 + i),
                    "fifoPath", Path.of(System.getProperty("java.io.tmpdir"), "golibrespot-test", name + ".fifo").toString()));
        }
        return GoLibrespotConfig.from(Map.of("enabled", true, "backends", raw)).getBackends();
    }
}
