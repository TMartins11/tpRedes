package br.unifal.redes.sender.network;

import java.util.concurrent.TimeUnit;

/**
 * Tracks the state of the Go-Back-N sender's retransmission timeout.
 *
 * <p>This class is a pure protocol-support component. It stores a configured
 * timeout duration and the instant a timer was last armed, and exposes
 * calculations — elapsed time, remaining time, expiration — that the
 * sender's FSM polls to decide when a retransmission burst is due.
 *
 * <p>This class performs no actual retransmission, no network I/O, no file
 * I/O, and schedules no background work of any kind. There is no
 * {@link Thread}, {@code Timer}, or {@code ScheduledExecutorService}
 * involved — the FSM is expected to call {@link #hasExpired()} periodically
 * (e.g. from its own polling loop) and react accordingly.
 *
 * <p>Elapsed-time calculations use {@link System#nanoTime()} rather than
 * {@link System#currentTimeMillis()}, since {@code nanoTime} is monotonic
 * and unaffected by wall-clock adjustments, making it the correct choice for
 * measuring durations.
 *
 * <p>Thread-safety: this class is not synchronized. It is intended to be
 * owned and driven by a single sender FSM thread; callers that need
 * concurrent access must coordinate externally.
 */
public final class TimeoutManager {

    private final long timeoutNanos;
    private final long timeoutMillis;

    private boolean running;
    private long startNanos;

    /**
     * Creates a new timeout manager with the given timeout duration.
     *
     * @param timeoutMillis the timeout duration, in milliseconds; must be &gt; 0
     * @throws IllegalArgumentException if {@code timeoutMillis} is not positive
     */
    public TimeoutManager(long timeoutMillis) {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be > 0, got: " + timeoutMillis);
        }
        this.timeoutMillis = timeoutMillis;
        this.timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        this.running = false;
    }

    // -------------------------------------------------------------------------
    // FSM-facing mutators
    // -------------------------------------------------------------------------

    /**
     * Arms the timer, capturing the current instant as its start point.
     *
     * <p>Use this for the first arming of a fresh timeout. If a timer may
     * already be running and should simply be reset, use {@link #restart()}
     * instead.
     *
     * @throws IllegalStateException if the timer is already running
     */
    public void start() {
        if (running) {
            throw new IllegalStateException(
                    "Timer is already running; call restart() to reset it or cancel() first");
        }
        arm();
    }

    /**
     * Re-arms the timer, resetting its start point to the current instant,
     * regardless of whether it was already running.
     *
     * <p>This is the typical operation performed when a partial ACK arrives
     * while packets are still in flight: the timeout clock restarts from now.
     */
    public void restart() {
        arm();
    }

    /**
     * Disarms the timer. After this call, {@link #isRunning()} returns
     * {@code false} and {@link #hasExpired()} returns {@code false} until the
     * timer is armed again via {@link #start()} or {@link #restart()}.
     *
     * <p>Idempotent — calling {@code cancel()} on an already-stopped timer
     * has no effect.
     */
    public void cancel() {
        running = false;
    }

    // -------------------------------------------------------------------------
    // Queries
    // -------------------------------------------------------------------------

    /**
     * @return {@code true} if the timer is currently armed (started or
     *         restarted, and not yet cancelled)
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Indicates whether the configured timeout duration has elapsed since the
     * timer was last armed.
     *
     * @return {@code true} if the timer is running and its elapsed time has
     *         reached or exceeded the configured timeout; {@code false} if
     *         the timer is not running or has not yet expired
     */
    public boolean hasExpired() {
        return running && elapsedNanosSinceArmed() >= timeoutNanos;
    }

    /**
     * @return the time elapsed, in milliseconds, since the timer was last
     *         armed, or {@code 0} if the timer is not currently running
     */
    public long elapsedMillis() {
        if (!running) {
            return 0L;
        }
        return TimeUnit.NANOSECONDS.toMillis(elapsedNanosSinceArmed());
    }

    /**
     * @return the time remaining, in milliseconds, before the timer expires,
     *         clamped to {@code 0} if it has already expired, or {@code 0} if
     *         the timer is not currently running
     */
    public long remainingMillis() {
        if (!running) {
            return 0L;
        }
        long remainingNanos = timeoutNanos - elapsedNanosSinceArmed();
        return TimeUnit.NANOSECONDS.toMillis(Math.max(0L, remainingNanos));
    }

    /** @return the configured timeout duration, in milliseconds */
    public long getTimeoutMillis() {
        return timeoutMillis;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private void arm() {
        this.startNanos = System.nanoTime();
        this.running = true;
    }

    private long elapsedNanosSinceArmed() {
        return System.nanoTime() - startNanos;
    }

    @Override
    public String toString() {
        return "TimeoutManager{"
                + "running=" + running
                + ", timeoutMillis=" + timeoutMillis
                + ", elapsedMillis=" + elapsedMillis()
                + ", remainingMillis=" + remainingMillis()
                + '}';
    }
}