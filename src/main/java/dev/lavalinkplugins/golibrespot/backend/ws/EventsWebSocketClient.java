package dev.lavalinkplugins.golibrespot.backend.ws;

import dev.lavalinkplugins.golibrespot.logging.LogSanitizer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;
import java.util.logging.Logger;

/**
 * One persistent, auto-reconnecting WebSocket connection to a go-librespot
 * daemon's {@code /events} endpoint ({@code docs/API_CONTRACT.md} §2.8/§3).
 *
 * <p>Design contract:</p>
 * <ul>
 *   <li><b>Prompt drain, never back up the socket.</b> Frames are parsed on the
 *       JDK WebSocket dispatch thread and handed to a bounded queue
 *       (capacity {@value #QUEUE_CAPACITY}, drop-oldest on overflow); a dedicated
 *       drain thread pops the queue and dispatches to the {@link EventsListener}.
 *       The daemon's synchronous per-client {@code Emit} has a 10 s write timeout
 *       (API_CONTRACT.md §3.5) — this client never blocks the socket on a slow
 *       consumer.</li>
 *   <li><b>Tolerant parsing.</b> Frames are {@code {"type": ..., "data": {...}}}.
 *       Unknown {@code type} values map to {@link EventType#UNKNOWN} (forwarded to
 *       the {@link EventsListener#onUnknownEvent(PlayerEvent)} debug-only sink),
 *       unknown data fields are kept raw in {@link PlayerEvent#data()}, and
 *       malformed frames are counted ({@link #getMalformedFrames()}), logged at
 *       debug level, and <b>never</b> drop the connection.</li>
 *   <li><b>Bounded reconnects.</b> On connect failure or connection loss the
 *       client reconnects with exponential backoff starting at
 *       {@code reconnectInitialMs} (default 1000), doubling up to
 *       {@code reconnectMaxMs} (default 30000), with ±20% jitter. After
 *       {@code failuresBeforeQuarantine} (default 5) <i>consecutive</i> failures
 *       the client stops reconnecting, flips {@link #isQuarantined()} and invokes
 *       {@link EventsListener#onQuarantine()}. Any successful connect resets the
 *       counter.</li>
 *   <li><b>Generation filter hook.</b> A {@link LongSupplier} (see
 *       {@link #setGenerationSupplier(LongSupplier)}) is sampled when the
 *       connection opens; events dispatched by a connection whose sampled
 *       generation is below the supplier's <i>current</i> value are dropped at
 *       the dispatch boundary (stale-connection suppression; counted via
 *       {@link #getDroppedByGeneration()}).</li>
 *   <li><b>Stall watchdog.</b> If no frame arrives within
 *       {@code watchdogStallMs} (default 30 s) the connection is treated as dead:
 *       it is aborted and the normal reconnect path runs.</li>
 *   <li><b>Clean close.</b> {@link #close()} sends a CLOSE frame, stops the
 *       reconnect loop and the watchdog, and joins the drain thread (bounded).
 *       Idempotent; after close no reconnect is ever attempted.</li>
 * </ul>
 *
 * <p>Zero runtime dependencies beyond the JDK ({@code java.net.http});
 * {@link LogSanitizer} redacts anything logged. Connection/reconnect state lives
 * on a dedicated manager thread; the socket is fed by the JDK's WebSocket
 * dispatch thread.</p>
 */
public final class EventsWebSocketClient implements AutoCloseable {

    /** Default stall window: no message within this → connection treated as dead. */
    static final long DEFAULT_WATCHDOG_STALL_MS = 30_000L;

    /** Bounded drain queue capacity (drop-oldest beyond this). */
    private static final int QUEUE_CAPACITY = 512;

    private static final Logger LOGGER = Logger.getLogger(EventsWebSocketClient.class.getName());

    private final String wsUrl;
    private final String sanitizedUrl;
    private final long reconnectInitialMs;
    private final long reconnectMaxMs;
    private final int failuresBeforeQuarantine;
    private final long watchdogStallMs;
    private final long connectTimeoutMs;
    private final EventsListener listener;
    private final LogSanitizer sanitizer;
    private final HttpClient httpClient;

    private final BlockingQueue<PlayerEvent> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicLong malformedFrames = new AtomicLong();
    private final AtomicLong connectAttempts = new AtomicLong();
    private final AtomicLong receivedEvents = new AtomicLong();
    private final AtomicLong droppedByGeneration = new AtomicLong();

    private volatile boolean started;
    private volatile boolean quarantined;
    private volatile WebSocket socket;
    private volatile long lastMessageAt;
    private volatile long connectionGeneration = -1;
    private volatile LongSupplier generationSupplier;
    private volatile Attempt currentAttempt;

    private volatile Thread managerThread;
    private volatile Thread drainThread;
    private volatile ScheduledExecutorService watchdogExecutor;

    /**
     * @param wsUrl                     the daemon events WebSocket URL (from
     *                                  {@code BackendConfig.getWsUrl()}, derived as
     *                                  {@code ws://host:port/events})
     * @param listener                  typed dispatch callbacks
     * @param reconnectInitialMs        first reconnect delay (ms); default 1000
     * @param reconnectMaxMs            reconnect delay cap (ms); default 30000
     * @param failuresBeforeQuarantine  consecutive failures before quarantine (default 5)
     */
    public EventsWebSocketClient(
            String wsUrl,
            EventsListener listener,
            long reconnectInitialMs,
            long reconnectMaxMs,
            int failuresBeforeQuarantine) {
        this(wsUrl, listener, reconnectInitialMs, reconnectMaxMs, failuresBeforeQuarantine,
                DEFAULT_WATCHDOG_STALL_MS);
    }

    /** Test-friendly constructor: overrides the stall-watchdog window. */
    EventsWebSocketClient(
            String wsUrl,
            EventsListener listener,
            long reconnectInitialMs,
            long reconnectMaxMs,
            int failuresBeforeQuarantine,
            long watchdogStallMs) {
        this(wsUrl, listener, reconnectInitialMs, reconnectMaxMs, failuresBeforeQuarantine,
                watchdogStallMs, 5_000L, LogSanitizer.defaults());
    }

    EventsWebSocketClient(
            String wsUrl,
            EventsListener listener,
            long reconnectInitialMs,
            long reconnectMaxMs,
            int failuresBeforeQuarantine,
            long watchdogStallMs,
            long connectTimeoutMs,
            LogSanitizer sanitizer) {
        this.wsUrl = Objects.requireNonNull(wsUrl, "wsUrl must not be null");
        this.listener = Objects.requireNonNull(listener, "listener must not be null");
        if (reconnectInitialMs <= 0) {
            throw new IllegalArgumentException("reconnectInitialMs must be positive");
        }
        if (reconnectMaxMs < reconnectInitialMs) {
            throw new IllegalArgumentException("reconnectMaxMs must be >= reconnectInitialMs");
        }
        if (failuresBeforeQuarantine <= 0) {
            throw new IllegalArgumentException("failuresBeforeQuarantine must be positive");
        }
        if (watchdogStallMs <= 0) {
            throw new IllegalArgumentException("watchdogStallMs must be positive");
        }
        if (connectTimeoutMs <= 0) {
            throw new IllegalArgumentException("connectTimeoutMs must be positive");
        }
        this.reconnectInitialMs = reconnectInitialMs;
        this.reconnectMaxMs = reconnectMaxMs;
        this.failuresBeforeQuarantine = failuresBeforeQuarantine;
        this.watchdogStallMs = watchdogStallMs;
        this.connectTimeoutMs = connectTimeoutMs;
        this.sanitizer = Objects.requireNonNull(sanitizer, "sanitizer must not be null");
        this.sanitizedUrl = sanitizer.sanitizeUrl(wsUrl);
        validateWsUrl(wsUrl);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                .build();
    }

    private static void validateWsUrl(String wsUrl) {
        URI uri;
        try {
            uri = URI.create(wsUrl);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid wsUrl: '" + wsUrl + "'", e);
        }
        String scheme = uri.getScheme();
        if (scheme == null || !("ws".equalsIgnoreCase(scheme) || "wss".equalsIgnoreCase(scheme))) {
            throw new IllegalArgumentException("wsUrl must use ws:// or wss://: '" + wsUrl + "'");
        }
        if (uri.getHost() == null) {
            throw new IllegalArgumentException("wsUrl must have a host: '" + wsUrl + "'");
        }
    }

    /**
     * Arms (or, with {@code null}, disarms) the generation filter hook. The
     * supplier is sampled when a connection opens; events from a connection
     * whose sampled generation is below the supplier's current value are
     * dropped at the dispatch boundary. If armed while a connection is already
     * up, the generation is captured lazily on the first dispatched event.
     */
    public void setGenerationSupplier(LongSupplier generationSupplier) {
        this.generationSupplier = generationSupplier;
    }

    /** Consecutive WS failures observed so far. */
    public long getConnectAttempts() {
        return connectAttempts.get();
    }

    /** Malformed frames received (tolerated, connection kept). */
    public long getMalformedFrames() {
        return malformedFrames.get();
    }

    /** Frames parsed and enqueued (includes unknown-type frames). */
    public long getReceivedEvents() {
        return receivedEvents.get();
    }

    /** Events dropped by the generation filter. */
    public long getDroppedByGeneration() {
        return droppedByGeneration.get();
    }

    /** {@code true} once the quarantine threshold was reached (reconnect loop stopped). */
    public boolean isQuarantined() {
        return quarantined;
    }

    /** {@code true} after {@link #close()}. */
    public boolean isClosed() {
        return closed.get();
    }

    /**
     * Starts the connection manager, the drain thread and the stall watchdog.
     * Idempotence guard: calling twice throws {@link IllegalStateException}.
     */
    public synchronized void start() {
        if (started) {
            throw new IllegalStateException("client already started");
        }
        started = true;
        drainThread = newThread(this::drainLoop, "events-ws-drain");
        drainThread.start();
        managerThread = newThread(this::managerLoop, "events-ws-manager");
        managerThread.start();
        watchdogExecutor = Executors.newSingleThreadScheduledExecutor(
                r -> newThread(r, "events-ws-watchdog"));
        long period = Math.max(50, Math.min(watchdogStallMs / 4, 1_000));
        watchdogExecutor.scheduleAtFixedRate(this::watchdogTick, period, period, TimeUnit.MILLISECONDS);
    }

    /**
     * Clean shutdown: sends a CLOSE frame, stops the reconnect loop and the
     * watchdog, and joins the drain thread (bounded). Idempotent; no reconnect
     * is ever attempted afterwards.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        WebSocket ws = socket;
        if (ws != null) {
            try {
                ws.sendClose(WebSocket.NORMAL_CLOSURE, "plugin shutdown");
            } catch (Exception ignored) {
                // best effort — socket may already be gone
            }
        }
        Attempt attempt = currentAttempt;
        if (attempt != null) {
            attempt.disconnectLatch.countDown();
        }
        ScheduledExecutorService watchdog = watchdogExecutor;
        if (watchdog != null) {
            watchdog.shutdownNow();
        }
        Thread manager = managerThread;
        if (manager != null && manager != Thread.currentThread()) {
            manager.interrupt();
            joinBounded(manager);
        }
        Thread drain = drainThread;
        if (drain != null && drain != Thread.currentThread()) {
            drain.interrupt();
            joinBounded(drain);
        }
    }

    // ------------------------------------------------------------------
    // Connection manager loop (reconnect + backoff + quarantine)
    // ------------------------------------------------------------------

    private void managerLoop() {
        int consecutiveFailures = 0;
        while (!closed.get() && !quarantined) {
            connectAttempts.incrementAndGet();
            Attempt attempt = new Attempt();
            if (tryConnect(attempt)) {
                consecutiveFailures = 0;
                safeNotify(listener::onConnected);
                boolean lost = awaitDisconnect(attempt.disconnectLatch);
                currentAttempt = null;
                socket = null;
                if (!lost) {
                    break; // closed while waiting
                }
                safeNotify(listener::onDisconnected);
                consecutiveFailures++;
            } else {
                consecutiveFailures++;
            }
            if (consecutiveFailures >= failuresBeforeQuarantine) {
                quarantined = true;
                LOGGER.fine("events ws: quarantined after " + consecutiveFailures
                        + " consecutive failures (" + sanitizedUrl + ")");
                safeNotify(listener::onQuarantine);
                break;
            }
            long delay = jitterDelayMs(backoffBaseMs(consecutiveFailures, reconnectInitialMs, reconnectMaxMs));
            LOGGER.fine("events ws: reconnect in " + delay + "ms, consecutive failures="
                    + consecutiveFailures + " (" + sanitizedUrl + ")");
            park(delay);
        }
    }

    private boolean tryConnect(Attempt attempt) {
        try {
            CompletableFuture<WebSocket> future = httpClient.newWebSocketBuilder()
                    .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                    .buildAsync(URI.create(wsUrl), new SocketListener(attempt));
            WebSocket ws = future.get(connectTimeoutMs, TimeUnit.MILLISECONDS);
            socket = ws;
            currentAttempt = attempt; // paired with the socket above — watchdog uses both
            LOGGER.fine("events ws: connected to " + sanitizedUrl);
            return true;
        } catch (Exception e) {
            attempt.abandoned.set(true);
            LOGGER.fine("events ws: connect to " + sanitizedUrl + " failed: "
                    + sanitizer.sanitize(String.valueOf(e)));
            return false;
        }
    }

    /** Blocks until the connection dies (true) or the client is closed (false). */
    private boolean awaitDisconnect(CountDownLatch latch) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    if (latch.await(1, TimeUnit.SECONDS)) {
                        return !closed.get();
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                }
                if (closed.get()) {
                    return false;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // ------------------------------------------------------------------
    // Drain path (bounded queue + dedicated thread — never blocks the socket)
    // ------------------------------------------------------------------

    private void enqueue(PlayerEvent event) {
        receivedEvents.incrementAndGet();
        if (!queue.offer(event)) {
            queue.poll(); // drop oldest: the daemon must never see back-pressure
            queue.offer(event);
            LOGGER.fine("events ws: event queue full, dropped oldest (capacity=" + QUEUE_CAPACITY + ")");
        }
    }

    private void drainLoop() {
        while (!closed.get() && !quarantined) {
            PlayerEvent event;
            try {
                event = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                continue;
            }
            dispatch(event);
        }
    }

    private void dispatch(PlayerEvent event) {
        LongSupplier gen = generationSupplier;
        if (gen != null) {
            long captured = connectionGeneration;
            if (captured < 0) {
                captured = connectionGeneration = gen.getAsLong();
            }
            if (captured < gen.getAsLong()) {
                droppedByGeneration.incrementAndGet();
                return;
            }
        }
        try {
            if (event.type() == EventType.UNKNOWN) {
                listener.onUnknownEvent(event);
            } else {
                listener.onEvent(event);
            }
        } catch (Throwable t) {
            LOGGER.fine("events ws: listener threw: " + sanitizer.sanitize(String.valueOf(t)));
        }
    }

    // ------------------------------------------------------------------
    // Stall watchdog
    // ------------------------------------------------------------------

    private void watchdogTick() {
        WebSocket ws = socket;
        Attempt attempt = currentAttempt;
        if (ws == null || attempt == null || closed.get() || quarantined) {
            return;
        }
        long last = lastMessageAt;
        if (last > 0 && System.currentTimeMillis() - last > watchdogStallMs) {
            LOGGER.fine("events ws: no message for " + watchdogStallMs
                    + "ms, aborting stalled connection (" + sanitizedUrl + ")");
            try {
                ws.abort();
            } catch (Exception ignored) {
                // already closed
            }
            // abort() does NOT invoke the listener's onClose — signal the
            // manager directly so the reconnect path runs
            attempt.disconnectLatch.countDown();
        }
    }

    // ------------------------------------------------------------------
    // Backoff math (package-private for direct unit testing)
    // ------------------------------------------------------------------

    /**
     * Exponential backoff base for the given consecutive-failure count:
     * {@code min(initial * 2^(count-1), max)}.
     */
    static long backoffBaseMs(int consecutiveFailures, long initialMs, long maxMs) {
        long base = initialMs;
        for (int i = 1; i < consecutiveFailures && base < maxMs; i++) {
            base = Math.min(maxMs, base * 2);
        }
        return base;
    }

    /** Applies ±20% jitter to a backoff delay. */
    static long jitterDelayMs(long baseMs) {
        if (baseMs <= 1) {
            return 1;
        }
        double factor = 0.8 + ThreadLocalRandom.current().nextDouble() * 0.4; // [0.8, 1.2)
        return Math.max(1, (long) (baseMs * factor));
    }

    // ------------------------------------------------------------------
    // JDK WebSocket listener (per connect attempt)
    // ------------------------------------------------------------------

    private final class SocketListener implements WebSocket.Listener {

        private final Attempt attempt;

        SocketListener(Attempt attempt) {
            this.attempt = attempt;
        }

        @Override
        public void onOpen(WebSocket ws) {
            ws.request(1);
            if (attempt.abandoned.get() || closed.get() || quarantined) {
                ws.abort(); // late handshake of an abandoned/closed attempt
                return;
            }
            lastMessageAt = System.currentTimeMillis();
            LongSupplier gen = generationSupplier;
            connectionGeneration = gen == null ? -1 : gen.getAsLong();
        }

        @Override
        public CompletionStage<?> onText(WebSocket ws, CharSequence data, boolean last) {
            ws.request(1);
            lastMessageAt = System.currentTimeMillis();
            if (!last) {
                attempt.partial.append(data);
                return null;
            }
            String frameText = attempt.partial.length() == 0
                    ? data.toString()
                    : attempt.partial.append(data).toString();
            attempt.partial.setLength(0);
            PlayerEvent event = FrameParser.parse(frameText);
            if (event == null) {
                malformedFrames.incrementAndGet();
                LOGGER.fine("events ws: ignoring malformed frame (total=" + malformedFrames.get() + ")");
                return null;
            }
            enqueue(event);
            return null;
        }

        @Override
        public void onError(WebSocket ws, Throwable error) {
            LOGGER.fine("events ws: socket error: " + sanitizer.sanitize(String.valueOf(error)));
        }

        @Override
        public CompletionStage<?> onClose(WebSocket ws, int statusCode, String reason) {
            if (!attempt.abandoned.get() && !closed.get()) {
                attempt.disconnectLatch.countDown();
            }
            return null;
        }
    }

    /** Per-connection-attempt state. */
    private static final class Attempt {
        final AtomicBoolean abandoned = new AtomicBoolean();
        final CountDownLatch disconnectLatch = new CountDownLatch(1);
        final StringBuilder partial = new StringBuilder();
    }

    // ------------------------------------------------------------------
    // Tolerant frame parser (no JSON library on the classpath)
    // ------------------------------------------------------------------

    /**
     * Minimal hand-rolled JSON parser producing plain
     * {@link String}/{@link Long}/{@link Double}/{@link Boolean}/{@code null}/
     * {@link List}/{@link Map} values. Tolerant: anything malformed yields
     * {@code null} (counted + logged by the caller, connection kept).
     */
    private static final class FrameParser {

        private final String src;
        private int pos;

        private FrameParser(String src) {
            this.src = src;
        }

        /**
         * Parses one {@code {"type": ..., "data": {...}}} frame. Returns
         * {@code null} when the frame is malformed or lacks a string {@code type}.
         */
        static PlayerEvent parse(String frame) {
            if (frame == null || frame.isEmpty()) {
                return null;
            }
            FrameParser parser = new FrameParser(frame);
            Object root;
            try {
                root = parser.parseValue();
                parser.skipWs();
            } catch (Malformed e) {
                return null;
            }
            if (!(root instanceof Map) || parser.pos != frame.length()) {
                return null;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) root;
            Object typeObj = map.get("type");
            if (!(typeObj instanceof String type) || type.isEmpty()) {
                return null;
            }
            Object data = map.get("data");
            Map<String, Object> dataMap;
            if (data == null) {
                dataMap = null;
            } else if (data instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> raw = (Map<String, Object>) data;
                dataMap = raw;
            } else {
                return null; // data present but not an object → malformed
            }
            return new PlayerEvent(EventType.fromWire(type), dataMap);
        }

        private Object parseValue() {
            skipWs();
            if (pos >= src.length()) {
                throw new Malformed();
            }
            char c = src.charAt(pos);
            switch (c) {
                case '{':
                    return parseObject();
                case '[':
                    return parseArray();
                case '"':
                    return parseString();
                case 't':
                    expect("true");
                    return Boolean.TRUE;
                case 'f':
                    expect("false");
                    return Boolean.FALSE;
                case 'n':
                    expect("null");
                    return null;
                default:
                    return parseNumber();
            }
        }

        private Map<String, Object> parseObject() {
            pos++; // '{'
            Map<String, Object> map = new HashMap<>();
            skipWs();
            if (peek() == '}') {
                pos++;
                return map;
            }
            while (true) {
                skipWs();
                if (pos >= src.length() || src.charAt(pos) != '"') {
                    throw new Malformed();
                }
                String key = parseString();
                skipWs();
                if (pos >= src.length() || src.charAt(pos) != ':') {
                    throw new Malformed();
                }
                pos++;
                map.put(key, parseValue());
                skipWs();
                if (pos >= src.length()) {
                    throw new Malformed();
                }
                char sep = src.charAt(pos);
                if (sep == '}') {
                    pos++;
                    return map;
                }
                if (sep != ',') {
                    throw new Malformed();
                }
                pos++;
            }
        }

        private List<Object> parseArray() {
            pos++; // '['
            List<Object> list = new ArrayList<>();
            skipWs();
            if (peek() == ']') {
                pos++;
                return list;
            }
            while (true) {
                list.add(parseValue());
                skipWs();
                if (pos >= src.length()) {
                    throw new Malformed();
                }
                char sep = src.charAt(pos);
                if (sep == ']') {
                    pos++;
                    return list;
                }
                if (sep != ',') {
                    throw new Malformed();
                }
                pos++;
            }
        }

        private String parseString() {
            pos++; // '"'
            StringBuilder sb = new StringBuilder();
            while (true) {
                if (pos >= src.length()) {
                    throw new Malformed();
                }
                char c = src.charAt(pos++);
                switch (c) {
                    case '"' -> {
                        return sb.toString();
                    }
                    case '\\' -> {
                        if (pos >= src.length()) {
                            throw new Malformed();
                        }
                        char esc = src.charAt(pos++);
                        switch (esc) {
                            case '"' -> sb.append('"');
                            case '\\' -> sb.append('\\');
                            case '/' -> sb.append('/');
                            case 'b' -> sb.append('\b');
                            case 'f' -> sb.append('\f');
                            case 'n' -> sb.append('\n');
                            case 'r' -> sb.append('\r');
                            case 't' -> sb.append('\t');
                            case 'u' -> {
                                if (pos + 4 > src.length()) {
                                    throw new Malformed();
                                }
                                try {
                                    sb.append((char) Integer.parseInt(src.substring(pos, pos + 4), 16));
                                } catch (NumberFormatException e) {
                                    throw new Malformed();
                                }
                                pos += 4;
                            }
                            default -> throw new Malformed();
                        }
                    }
                    default -> sb.append(c);
                }
            }
        }

        private Object parseNumber() {
            int start = pos;
            while (pos < src.length()) {
                char c = src.charAt(pos);
                if ((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E') {
                    pos++;
                } else {
                    break;
                }
            }
            if (pos == start) {
                throw new Malformed();
            }
            String num = src.substring(start, pos);
            try {
                if (num.indexOf('.') >= 0 || num.indexOf('e') >= 0 || num.indexOf('E') >= 0) {
                    return Double.parseDouble(num);
                }
                return Long.parseLong(num);
            } catch (NumberFormatException e) {
                throw new Malformed();
            }
        }

        private void expect(String token) {
            if (!src.startsWith(token, pos)) {
                throw new Malformed();
            }
            pos += token.length();
        }

        private char peek() {
            return pos < src.length() ? src.charAt(pos) : '\0';
        }

        private void skipWs() {
            while (pos < src.length()) {
                char c = src.charAt(pos);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    pos++;
                } else {
                    break;
                }
            }
        }

        /** Signals a structurally invalid frame. */
        private static final class Malformed extends RuntimeException {
            private static final long serialVersionUID = 1L;
        }
    }

    // ------------------------------------------------------------------
    // Small helpers
    // ------------------------------------------------------------------

    private static Thread newThread(Runnable runnable, String name) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(true);
        return thread;
    }

    private static void park(long delayMs) {
        if (delayMs > 0) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(delayMs));
        }
    }

    private static void joinBounded(Thread thread) {
        try {
            thread.join(2_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void safeNotify(Runnable callback) {
        try {
            callback.run();
        } catch (Throwable t) {
            LOGGER.fine("events ws: listener threw: " + sanitizer.sanitize(String.valueOf(t)));
        }
    }

    /** The typed dispatch callbacks. All are invoked on the drain/manager threads. */
    public interface EventsListener {
        /** A parsed, generation-accepted event of a known type. */
        void onEvent(PlayerEvent event);

        /** A parsed frame whose {@code type} is unknown (debug-only sink). */
        default void onUnknownEvent(PlayerEvent event) {}

        /** The quarantine threshold (consecutive failures) was reached. */
        default void onQuarantine() {}

        /** A connection was established (initial connect or reconnect). */
        default void onConnected() {}

        /** An established connection was lost (never fires on clean close). */
        default void onDisconnected() {}
    }
}
