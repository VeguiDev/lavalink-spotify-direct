package dev.lavalinkplugins.golibrespot.lifecycle;

import java.util.Objects;

/**
 * Typed activation failure thrown by {@link ActivationBarrier#awaitActivated}
 * and {@link LifecycleCoordinator#awaitActivated} (and thus observed by T18's
 * {@code process()} loop) when activation cannot complete: the backend was
 * quarantined / degraded / dead, the barrier timed out, or the session was
 * cancelled.
 *
 * <p>The {@link Kind} mirrors the machine's {@link BackendStateMachine.Outcome}
 * taxonomy so callers can distinguish a retryable backend problem
 * ({@link Kind#QUARANTINED}) from a permanent one ({@link Kind#DEGRADED},
 * {@link Kind#DEAD}) without coupling to the machine type.</p>
 */
public final class ActivationException extends Exception {

  /** Why activation failed. */
  public enum Kind {
    /** The activation budget elapsed before a current-generation {@code playing} was confirmed. */
    TIMEOUT,
    /** The backend was transiently quarantined while activating. */
    QUARANTINED,
    /** The backend was permanently degraded (contradictory state / stop-taint). */
    DEGRADED,
    /** The backend process is unreachable / the machine is dead. */
    DEAD,
    /** Generic non-quarantine failure (no lease, FIFO open aborted, no session). */
    FAILED,
    /** The session was cancelled (coordinator closed). */
    CANCELED
  }

  private final Kind kind;

  public ActivationException(Kind kind, String message) {
    super(message);
    this.kind = Objects.requireNonNull(kind, "kind");
  }

  public ActivationException(Kind kind, String message, Throwable cause) {
    super(message, cause);
    this.kind = Objects.requireNonNull(kind, "kind");
  }

  /** The activation-failure category. */
  public Kind kind() {
    return kind;
  }
}
