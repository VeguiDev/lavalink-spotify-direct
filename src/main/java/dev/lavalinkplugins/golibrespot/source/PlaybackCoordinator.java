package dev.lavalinkplugins.golibrespot.source;

import dev.lavalinkplugins.golibrespot.lifecycle.ActivationException;
import dev.lavalinkplugins.golibrespot.lifecycle.BackendStateMachine;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * The playback-coordination seam the T18 source manager, track and player
 * bridge consume — a narrow view over the T15 {@code LifecycleCoordinator} +
 * T17 {@code StopSequence} chain so tests can inject a fake and the production
 * wiring can hide the full backend stack (see {@link CoordinatorBackedPlayback}).
 *
 * <p>Contract:</p>
 * <ul>
 *   <li>{@link #start}/{@link #replace} drive sessions (never at load);</li>
 *   <li>{@link #logicalStop}/{@link #destroy}/{@link #pauseRemote}/
 *       {@link #resumeRemote}/{@link #quarantine} are the bridge's async,
 *       bounded lanes (never block the Lavaplayer thread on a daemon round-trip);</li>
 *   <li>{@link #awaitActivated}/{@link #nextFrame} are the T18 {@code process()}
 *       read path — {@code nextFrame} returns {@code null} ONLY at end-of-stream
 *       and never while the activation barrier is pending;</li>
 *   <li>{@link #seek} maps to the T16 seek handshake and returns the machine's
 *       {@link BackendStateMachine.Result} (the track fails on a non-OK result);</li>
 *   <li>{@link #isActive} is the bridge's ownership guard (only react to events
 *       whose track belongs to an active session of this plugin instance).</li>
 * </ul>
 */
public interface PlaybackCoordinator {

  /** Starts a new track session (acquires the lease at play start, never at load). */
  CompletableFuture<BackendStateMachine.Result> start(String uri, long positionMs);

  /** Play-over-play replacement on the HELD lease (no stop, no release-then-acquire). */
  CompletableFuture<BackendStateMachine.Result> replace(String uri, long positionMs);

  /** Logical stop = remote pause + confirmation + generation retirement. */
  CompletableFuture<BackendStateMachine.Result> logicalStop();

  /** Async player-destroy release (lease returned exactly once, never blocking the caller). */
  CompletableFuture<BackendStateMachine.Result> destroy();

  /** Remote pause via the state machine (bounded future; the bridge never awaits it). */
  CompletableFuture<BackendStateMachine.Result> pauseRemote();

  /** Remote resume via the state machine (bounded future; the bridge never awaits it). */
  CompletableFuture<BackendStateMachine.Result> resumeRemote();

  /** Quarantine the backend (typed, bounded; the machine owns the lease release). */
  CompletableFuture<BackendStateMachine.Result> quarantine(String reason);

  /** Blocks on the activation barrier; throws the typed failure on any failure. */
  void awaitActivated(Duration timeout) throws ActivationException, InterruptedException;

  /**
   * Next decoded stereo frames, or {@code null} at end-of-stream, or an empty
   * array for a transient no-data call. Never {@code null} while activation is
   * pending (a null return would prematurely end the track).
   */
  short[] nextFrame(Duration timeout) throws ActivationException, InterruptedException;

  /** The T16 seek handshake result (position is daemon-authoritative). */
  BackendStateMachine.Result seek(long positionMs);

  /** True while this coordinator holds (or is activating) a lease. */
  boolean isActive();

  /** The current session's expected URI, or {@code null} when idle. */
  String expectedUri();

  /** The current session's requested start position. */
  long positionMs();

  /** The current session's machine generation. */
  long generation();
}
