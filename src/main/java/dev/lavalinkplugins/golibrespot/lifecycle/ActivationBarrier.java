package dev.lavalinkplugins.golibrespot.lifecycle;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * One-shot activation barrier guarding a backend lease generation.
 *
 * <p>Created by the {@link LifecycleCoordinator} for each activation (first
 * activation and each play-over-play replacement) and satisfied by the machine's
 * confirmed activation result ({@link #markActivated()}) or failed by a
 * quarantine / degradation / timeout / abort ({@link #fail}).
 *
 * <p><b>process()/barrier contract</b> (T18's {@code process()} blocks on this
 * via {@link LifecycleCoordinator#awaitActivated}): {@link #awaitActivated(Duration)}
 * returns normally only when the barrier is satisfied; it throws a typed
 * {@link ActivationException} on failure, quarantine or timeout; a waiting
 * caller is never released with a null/empty "pending" signal that Lavaplayer
 * would read as end-of-track. The barrier is bounded by the caller's duration —
 * the coordinator additionally drives the machine's own activation timeout
 * (≤15s by default) into a quarantine, which fails the barrier, so a pending
 * {@code awaitActivated} always resolves within the activation budget.</p>
 *
 * <p>Thread-safety: {@link #markActivated()} and {@link #fail} are
 * synchronized first-wins transitions; multiple threads may await
 * concurrently.</p>
 */
public final class ActivationBarrier {

  /** Mutable one-shot state. */
  public enum State {
    /** Activation in flight — {@code awaitActivated} blocks. */
    PENDING,
    /** Activation confirmed — {@code awaitActivated} returns immediately. */
    SATISFIED,
    /** Activation failed — {@code awaitActivated} throws the stored failure. */
    FAILED
  }

  /** The stored failure (kind + reason), set once on {@link #fail}. */
  record Failure(ActivationException.Kind kind, String reason) {}

  private final long generation;
  private final String expectedUri;
  private final CountDownLatch release = new CountDownLatch(1);
  private final AtomicReference<Failure> failure = new AtomicReference<>();
  private volatile State state = State.PENDING;

  /**
   * @param generation the lease generation this barrier guards (machine's
   *     current generation at activation)
   * @param expectedUri the track URI whose current-generation {@code playing}
   *     confirms activation
   */
  public ActivationBarrier(long generation, String expectedUri) {
    if (expectedUri == null) {
      throw new NullPointerException("expectedUri");
    }
    this.generation = generation;
    this.expectedUri = expectedUri;
  }

  /** The generation this barrier guards (see {@link LifecycleCoordinator#generation()}). */
  public long generation() {
    return generation;
  }

  /** The expected current-generation URI this barrier awaits {@code playing} for. */
  public String expectedUri() {
    return expectedUri;
  }

  public State state() {
    return state;
  }

  public boolean isSatisfied() {
    return state == State.SATISFIED;
  }

  public boolean isFailed() {
    return state == State.FAILED;
  }

  /** The stored failure reason, or {@code null} when not failed. */
  public String failureReason() {
    Failure f = failure.get();
    return f == null ? null : f.reason();
  }

  /** Confirms activation: releases every waiting caller. First transition wins. */
  public synchronized void markActivated() {
    if (state != State.PENDING) {
      return;
    }
    state = State.SATISFIED;
    release.countDown();
  }

  /**
   * Fails activation with a typed kind + reason: releases waiting callers with
   * an {@link ActivationException}. First transition wins.
   */
  public synchronized void fail(ActivationException.Kind kind, String reason) {
    Objects.requireNonNull(kind, "kind");
    Objects.requireNonNull(reason, "reason");
    if (state != State.PENDING) {
      return;
    }
    failure.compareAndSet(null, new Failure(kind, reason));
    state = State.FAILED;
    release.countDown();
  }

  /**
   * Blocks until the barrier is satisfied, failed, or {@code timeout} elapses.
   *
   * @return normally when the barrier is satisfied (activation confirmed)
   * @throws ActivationException on failure / quarantine / degradation / death
   *     (immediately when already failed) or when {@code timeout} elapses
   *     ({@link ActivationException.Kind#TIMEOUT})
   * @throws InterruptedException if the calling thread is interrupted
   */
  public void awaitActivated(Duration timeout) throws ActivationException, InterruptedException {
    Objects.requireNonNull(timeout, "timeout");
    if (timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must not be negative: " + timeout);
    }
    if (state == State.SATISFIED) {
      return;
    }
    Failure f = failure.get();
    if (f != null) {
      throw new ActivationException(f.kind(), f.reason());
    }
    long deadline = System.nanoTime() + timeout.toNanos();
    while (true) {
      long remaining = deadline - System.nanoTime();
      if (remaining <= 0) {
        Failure ff = failure.get();
        if (ff != null) {
          throw new ActivationException(ff.kind(), ff.reason());
        }
        throw new ActivationException(ActivationException.Kind.TIMEOUT,
            "activation timed out after " + timeout + " waiting for '" + expectedUri + "'");
      }
      release.await(remaining, TimeUnit.NANOSECONDS);
      if (state == State.SATISFIED) {
        return;
      }
      Failure ff = failure.get();
      if (ff != null) {
        throw new ActivationException(ff.kind(), ff.reason());
      }
      // spurious or raced wake while still pending — loop with the remaining budget
    }
  }
}
