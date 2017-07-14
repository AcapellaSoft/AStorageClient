package ru.acapella.kv.core.handlers;

import ru.acapella.common.ExceptionUtils;
import ru.acapella.kv.core.CodeException;
import ru.acapella.kv.core.Context;
import ru.acapella.kv.core.Resource;
import ru.acapella.kv.core.buffers.InputBuffer;
import ru.acapella.kv.core.data.Address;
import ru.acapella.kv.core.data.ErrorCodes;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.ResponseBase;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public abstract class RequestHandler implements Resource {
    public static final int REQUEST_TIMEOUT = 60 * 1000; // 1 min

    protected final Context ctx;
    private int index;
    private Address from;
    private long id;
    private Resource timer;
    protected boolean logging;

    public RequestHandler(Context ctx) {
        this.ctx = ctx;
        this.logging = true;
    }

    public final Address from() { return from; }
    public final long id() { return id; }

    public final void handle(int index, Address from, long id, InputBuffer input) {
        this.index = index;
        this.from = from;
        this.id = id;
        this.timer = ctx.setTimeout(REQUEST_TIMEOUT, TimeUnit.MILLISECONDS, this::cancel);

        try {
            handle(input);
        } catch (IllegalArgumentException ex) {
            if (ExceptionUtils.ASSERTIONS_ENABLED) ex.printStackTrace();
            sendError(ErrorCodes.ILLEGAL_ARGUMENT);
        }
    }

    public final void cancel() {
        log_error("Request cancelled, sending timeout response...");
        ctx.sendError(from, id, ErrorCodes.TIMEOUT);
        close();
    }

    public final void complete() {
        log_debug(() -> "Request completed without response");
        close();
        ctx.requests.complete(index, this);
    }

    public final void sendResponse(ResponseBase msg) {
        log_debug(() -> "Request completed, sending response...");
        ctx.sendResponse(from, id, msg);
        close();
        ctx.requests.complete(index, this);
    }

    public final void sendError(int code) {
        log_debug(() -> String.format("Request failed with code %d, sending error response...", code));
        ctx.sendError(from, id, code);
        close();
        ctx.requests.complete(index, this);
    }

    public final Void sendError(Throwable ex) {
        Throwable cause = ExceptionUtils.getInitialCause(ex);
        if (cause instanceof CodeException) {
            CodeException codeEx = (CodeException) cause;
            sendError(codeEx.code);
        } else {
            log_error(ex);
            sendError(ErrorCodes.UNEXPECTED_ERROR);
        }
        return null;
    }

    public final void redirect(Address to, RequestBase msg) {
        if (logging) log_debug(() -> String.format("Redirecting request to %s...", to));
        ctx.redirectRequest(to, from, id, msg);
        close();
        ctx.requests.complete(index, this);
    }

    public final void log_debug(Supplier<String> logFunc) {
        if (logging)
            ctx.logger.debug(() -> String.format("[%s %d] %s", from, id, logFunc.get()));
    }

    public final void log_error(String msg, Object... args) {
        if (logging)
            ctx.logger.error("[%s %d] %s", from, id, String.format(msg, args));
    }

    public final void log_error(Throwable ex) {
        if (logging) {
            String msg = "[%s %d] " + ex.toString() + ExceptionUtils.getStackTrace(ex);
            ctx.logger.error(msg);
        }
    }

    @Override
    public void close() {
        timer.close();
    }

    /**
     * Метод вызывается для обработки запроса.
     * @param input буфер, содержащий сериализованный запрос
     * @throws IllegalArgumentException если аргументы запроса нарушают заданные ограничения
     */
    protected abstract void handle(InputBuffer input);
}
