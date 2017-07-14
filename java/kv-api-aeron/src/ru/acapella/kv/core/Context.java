package ru.acapella.kv.core;

import com.sun.management.ThreadMXBean;
import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.exceptions.ActiveDriverException;
import io.aeron.logbuffer.Header;
import kotlin.coroutines.experimental.CoroutineContext;
import kotlinx.coroutines.experimental.CoroutineDispatcher;
import lombok.SneakyThrows;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.DirectBuffer;
import org.agrona.collections.BiInt2ObjectMap;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import ru.acapella.common.ILogger;
import ru.acapella.kv.core.TimerManager.Timer;
import ru.acapella.kv.core.buffers.InputWrapBuffer;
import ru.acapella.kv.core.buffers.OutputWrapBuffer;
import ru.acapella.kv.core.data.Address;
import ru.acapella.kv.core.data.ErrorCodes;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.handlers.RequestHandler;
import ru.acapella.kv.core.handlers.ResponseHandler;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

abstract public class Context extends CoroutineDispatcher implements Closeable {
    public static final short PROTOCOL_VERSION = 0x0003;

    public static final boolean LOG_MEMORY_USAGE = Boolean.getBoolean("kv.context.log.resources");

    // битовые позиции флагов
    public static final byte FLAG_REQUEST = 0;
    public static final byte FLAG_NEED_RESPONSE = 1;
    public static final byte FLAG_ERROR = 2;

    public static final int KV_IPC_STREAM_ID_MARKER = 1 << 16;
    public static final int KV_UDP_STREAM_ID = 12;
    
    private static final int SEND_RETRY_MAX = Integer.getInteger("kv.context.send.retry.max", 100000);

    private static final int MAX_REQUEST_COUNT = Integer.getInteger("kv.context.requests.count", 100000);
    private static final int MAX_REQUEST_PUT_ATTEMPTS = Integer.getInteger("kv.context.requests.attempts", 5);

    private final Map<Address, Publication> pubs;
    private final UnsafeBuffer sendBuffer;
    private final OutputWrapBuffer output;
    private final InputWrapBuffer input;
    private final Int2ObjectHashMap<Supplier<RequestHandler>> handlers;
    private final BiInt2ObjectMap<ResponseHandler> responses;
    public final RequestPool requests;
    protected final IdleStrategy sendingIdleStrategy;
    protected final TimerManager timers;
    protected final Queue<Runnable> tasks;
    protected long requestCounter;
    private long counter;

    protected PublicationFactory publicationFactory;
    private State state;

    public final Address selfAddress;
    public final ILogger logger;

    public enum State {
        RUNNING,
        CLOSING,
        CLOSED
    }

    public Context(Address selfAddress, ILogger logger, PublicationFactory publicationFactory) {
        this(selfAddress, logger, publicationFactory, new BackoffIdleStrategy(
                1000, 200, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(500)));
    }

    public Context(Address selfAddress, ILogger logger, PublicationFactory publicationFactory, IdleStrategy sendingIdleStrategy) {
        this.publicationFactory = publicationFactory;
        this.sendingIdleStrategy = sendingIdleStrategy;
        this.sendBuffer = new UnsafeBuffer(BufferUtil.allocateDirectAligned(1000000, BitUtil.CACHE_LINE_LENGTH));
        this.output = new OutputWrapBuffer(sendBuffer, 0);
        this.input = new InputWrapBuffer();

        this.selfAddress = selfAddress;
        this.pubs = new HashMap<>();
        this.handlers = new Int2ObjectHashMap<>();
        this.responses = new BiInt2ObjectMap<>();
        this.requests = new RequestPool(MAX_REQUEST_COUNT, MAX_REQUEST_PUT_ATTEMPTS);
        this.timers = new TimerManager();
        this.tasks = new ConcurrentLinkedQueue<>();

        this.logger = logger;
        this.logger.activate();
        this.state = State.RUNNING;
    }

    public State getState() { return state; }

    public static int ipcStreamId(Address selfAddress) {
        return selfAddress.port | KV_IPC_STREAM_ID_MARKER;
    }

    @Override
    public void dispatch(CoroutineContext coroutineContext, Runnable runnable) {
        schedule(runnable);
    }

    public interface PublicationFactory extends Closeable {
        Publication createIpcPublication(int streamId);
        Publication createUdpPublication(Address address, int streamId);
    }

    public static final class EventLoopContext extends Context implements Runnable {
        final Aeron aeron;
        final FragmentAssembler asm;
        final Subscription sub_ipc;
        final Subscription sub_udp;
        final IdleStrategy pollingIdleStrategy;

        public EventLoopContext(Address selfAddress, boolean launchDriver, ILogger logger) {
            super(selfAddress, logger, null);
            this.pollingIdleStrategy = new BackoffIdleStrategy(
                    1000, 200, TimeUnit.MICROSECONDS.toNanos(1), TimeUnit.MICROSECONDS.toNanos(500));

            publicationFactory = new PublicationFactory() {
                private List<Publication> pubs = new ArrayList<>();

                @Override
                public Publication createIpcPublication(int streamId) {
                    logger.info("[Aeron] Connecting to ipc (stream: %d)", streamId);
                    return aeron.addPublication("aeron:ipc", streamId);
                }

                @Override
                public Publication createUdpPublication(Address address, int streamId) {
                    logger.info("[Aeron] Connecting to udp (host: %s, port: %d, stream: %d)",
                            address.hostString(), address.port, streamId);
                    return aeron.addPublication("aeron:udp?endpoint=" + address, streamId);
                }

                @Override
                public void close() throws IOException {
                    for (Publication p : pubs) p.close();
                    pubs.clear();
                }
            };

            String aeronDirectoryName = "/dev/shm/aeron-" + System.getProperty("user.name");

            Aeron.Context ctx = new Aeron.Context();
            ctx.aeronDirectoryName(aeronDirectoryName);

            if (launchDriver) {
                try {
                    MediaDriver.launch();
                } catch (ActiveDriverException ex) {
                    logger.info("[Aeron] Active driver detected");
                }
            }

            this.aeron = Aeron.connect(ctx);

            logger.info("[Aeron] Listening to ipc (stream: %d)", ipcStreamId(selfAddress));
            this.sub_ipc = aeron.addSubscription("aeron:ipc", ipcStreamId(selfAddress));

            logger.info("[Aeron] Listening to udp (host: %s, port: %d, stream: %d)",
                    selfAddress.hostString(), selfAddress.port, KV_UDP_STREAM_ID);
            this.sub_udp = aeron.addSubscription("aeron:udp?endpoint=" + selfAddress, KV_UDP_STREAM_ID);

            this.asm = new FragmentAssembler(this::handleMessage);
        }

        private int processTasks(int maxCount) {
            int i;
            for (i = 0; i < maxCount && !tasks.isEmpty(); ++i) {
                try {
                    tasks.poll().run();
                } catch (Throwable ex) {
                    logger.error(ex);
                }
            }
            return i;
        }

        public int process() {
            if (getState() == State.CLOSED) return 0;

            int workCount = 0;

            timers.updateTimersAndTime();
            workCount += processTasks(16);
            workCount += sub_ipc.poll(asm, 16);
            workCount += sub_udp.poll(asm, 16);

            return workCount;
        }

        @SneakyThrows
        @Override
        public void run() {
            ThreadMXBean mx = (ThreadMXBean) java.lang.management.ManagementFactory.getThreadMXBean();
            long tid = Thread.currentThread().getId();
            long alloc1 = mx.getThreadAllocatedBytes(tid);
            long t1 = System.currentTimeMillis();

            while (!Thread.interrupted() && getState() != State.CLOSED) {
                int workCount = process();
                pollingIdleStrategy.idle(workCount);

                if (LOG_MEMORY_USAGE) {
                    long t2 = System.currentTimeMillis();
                    if (t2 - t1 > 5000) {
                        long alloc2 = mx.getThreadAllocatedBytes(tid);

                        if (requestCounter > 0) {
                            logger.info("%6d bytes/req, %8d req/sec",
                                    (alloc2 - alloc1 - 48) / requestCounter, requestCounter * 1000 / (t2 - t1));
                        }
                        requestCounter = 0;
                        alloc1 = alloc2;
                        t1 = t2;
                    }
                }

                if (getState() == State.CLOSING && canClose()) close();
            }
        }

        @Override
        public void close() throws IOException {
            super.close();
            sub_ipc.close();
            sub_udp.close();
            aeron.close();
        }
    }

    public void handleMessage(DirectBuffer buffer, int offset, int length, Header header) {
        input.wrap(buffer, offset);

        short version = input.getShort();
        if (version != PROTOCOL_VERSION)
            throw new UnsupportedOperationException(String.format("Unsupported protocol version %d", version));

        short flags = input.getShort();
        boolean request = (flags & (1 << FLAG_REQUEST)) != 0;

        if (request) {
            handleRequest(flags);
        } else {
            handleResponse(flags);
        }
    }

    private void handleRequest(short flags) {
        ++requestCounter;
        long id = 0;
        Address from = null;
        boolean needResponse = (flags & (1 << FLAG_NEED_RESPONSE)) != 0;
        if (needResponse) {
            id = input.getLong();
            from = new Address();
            from.deserialize(input);
        }
        byte type = input.getByte();
        Supplier<RequestHandler> handlerFactory = handlers.get(type);
        if (handlerFactory != null) {
            RequestHandler handler = handlerFactory.get();
            int index = requests.put(handler);
            handler.handle(index, from, id, input);
        }
    }

    private void handleResponse(short flags) {
        long id = input.getLong();
        boolean error = (flags & (1 << FLAG_ERROR)) != 0;
        if (error) {
            int code = input.getInt();
            handleError(id, code);
        } else {
            handleResult(id);
        }
    }

    private void handleError(long id, int code) {
        ResponseHandler handler = responses.get((int) (id >> 32), (int) id);
        removeResponse(id);
        if (handler != null) {
            handler.handleError(id, code);
        }
    }

    private void handleResult(long id) {
        ResponseHandler handler = responses.get((int) (id >> 32), (int) id);
        removeResponse(id);
        if (handler != null) {
            byte type = input.getByte();
            handler.handleResult(id, type, input);
        }
    }

    private Publication pub(Address address) {
        Publication pub = pubs.get(address);
        if (pub == null) {
            if (Arrays.equals(address.host, selfAddress.host)) {
                pub = publicationFactory.createIpcPublication(address.port | KV_IPC_STREAM_ID_MARKER);
            } else {
                pub = publicationFactory.createUdpPublication(address, KV_UDP_STREAM_ID);
            }
            pubs.put(address, pub);
        }
        return pub;
    }

    private boolean trySend(Publication pub, DirectBuffer sendBuffer, int length) {
        long result = pub.offer(sendBuffer, 0, length);
        return result >= 0L || result == Publication.CLOSED || result == Publication.NOT_CONNECTED;
    }

    private boolean send(Publication pub, DirectBuffer sendBuffer, int length) {
        int retry = 0;
        while (!trySend(pub, sendBuffer, length) && retry < SEND_RETRY_MAX) {
            sendingIdleStrategy.idle();
            ++retry;
        }
        sendingIdleStrategy.reset();

        return retry < SEND_RETRY_MAX;
    }

    // перенаправляет запрос на другую ноду
    // ответ придёт только на ноду, которая изначально послала запрос
    public void redirectRequest(Address to, Address from, long id, RequestBase msg) {
        sendRequestInner(to, from, id, msg);
    }

    // посылает запрос и ждёт ответа
    // после получения ответа, автоматически удаляет обработчик из списка ожидания
    public Resource sendRequest(Address to, RequestBase msg, ResponseHandler handler) {
        long id = ++counter;
        //if (counter % 1000 == 0) System.out.println(counter);
        sendRequestInner(to, selfAddress, id, msg);
        addResponse(id, handler);
        return () -> removeResponse(id);
    }

    public <Response extends ResponseBase, Request extends RequestBase<Response>>
    CompletableFuture<Response> sendRequestFuture(Address to, Request msg, long timeout, TimeUnit unit) {
        CompletableFuture<Response> f = new CompletableFuture<>();
        Supplier<Response> responseAllocator = msg.response();

        Resource r = sendRequest(to, msg, ResponseHandler.create((id, type, input) -> {
            Response resp = responseAllocator.get();
            resp.deserialize(id, type, input);
            f.complete(resp);
        }, (id, code) -> {
            f.completeExceptionally(new CodeException(code));
        }));

        Resource timer = setTimeout(timeout, unit, () -> {
            r.close();
            f.completeExceptionally(new TimeoutException());
        });

        return f.whenComplete((res, ex) -> {
            timer.close();
        });
    }

    public <Response extends ResponseBase, Request extends RequestBase<Response>>
    CompletableFuture<Response> sendRequest(
            Address to, Request msg, ResourceList resources
    ) {
        CompletableFuture<Response> f = new CompletableFuture<>();
        Supplier<Response> responseAllocator = msg.response();

        Resource r = sendRequest(to, msg, ResponseHandler.create((id, type, input) -> {
            Response resp = responseAllocator.get();
            resp.deserialize(id, type, input);
            f.complete(resp);
        }, (id, code) -> {
            f.completeExceptionally(new CodeException(code));
        }));

        return f.whenComplete((res, ex) -> resources.remove(r));
    }

    // посылает запрос без ответа
    public void sendRequest(Address to, RequestBase msg) {
        sendRequestInner(to, null, 0, msg);
    }

    private void sendRequestInner(Address to, Address from, long id, RequestBase msg) {
        boolean needResponse = from != null;

        short flags = 0;
        flags |= (1 << FLAG_REQUEST);
        if (needResponse) flags |= (1 << FLAG_NEED_RESPONSE);

        output.offset(0);
        output.putShort(PROTOCOL_VERSION);
        output.putShort(flags);
        if (needResponse) {
            output.putLong(id);
            from.serialize(output);
        }
        output.putByte(msg.type());
        msg.serialize(output);

        Publication pub = pub(to);
        int length = output.offset();
        if (!send(pub, sendBuffer, length)) {
            schedule(() -> handleError(id, ErrorCodes.TIMEOUT));
        }
    }

    public void sendResponse(Address to, long id, ResponseBase msg) {
        if (to != null) {
            short flags = 0;
            flags |= (0 << FLAG_REQUEST);
            flags |= (0 << FLAG_ERROR);

            output.offset(0);
            output.putShort(PROTOCOL_VERSION);
            output.putShort(flags);
            output.putLong(id);
            output.putByte(msg.type());
            msg.serialize(output);

            Publication pub = pub(to);
            int length = output.offset();
            send(pub, sendBuffer, length);
        }
    }

    public void sendError(Address to, long id, int code) {
        if (to != null) {
            short flags = 0;
            flags |= (0 << FLAG_REQUEST);
            flags |= (1 << FLAG_ERROR);

            output.offset(0);
            output.putShort(PROTOCOL_VERSION);
            output.putShort(flags);
            output.putLong(id);
            output.putInt(code);

            Publication pub = pub(to);
            int length = output.offset();
            send(pub, sendBuffer, length);
        }
    }

    public void addHandler(byte type, Supplier<RequestHandler> handler) {
        if (handlers.get(type) != null)
            throw new RuntimeException(String.format("Handler with type %d already registered", type));
        handlers.put(type, handler);
    }

    private void addResponse(long id, ResponseHandler handler) {
        responses.put((int) (id >> 32), (int) id, handler);
    }

    private void removeResponse(long id) {
        responses.remove((int) (id >> 32), (int) id);
    }

    public Resource setTimeout(long delay, TimeUnit unit, Runnable task) {
        Timer timer = timers.runTimer(unit.toNanos(delay), t -> { task.run(); return true; });
        return timer::stop;
    }

    public Resource schedule(Runnable task) {
        tasks.add(task);
        return () -> {};
    }

    public boolean canClose() {
        return responses.isEmpty() && requests.isEmpty();
    }

    @Override
    public void close() throws IOException {
        if (!canClose()) throw new RuntimeException("Context can not be closed at this time");
        publicationFactory.close();
        pubs.clear();
        state = State.CLOSED;
    }

    public void scheduleClosing() {
        state = State.CLOSING;
    }
}
