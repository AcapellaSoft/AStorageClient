package ru.acapella.kv.core.handlers;

import ru.acapella.kv.core.buffers.InputBuffer;

public interface ResponseHandler {
    void handleResult(long id, byte type, InputBuffer input);
    void handleError(long id, int code);

    @FunctionalInterface
    interface ResultHandler {
        void handle(long id, byte type, InputBuffer input);
    }

    @FunctionalInterface
    interface ErrorHandler {
        void handle(long id, int code);
    }

    static ResponseHandler create(ResultHandler resultHandler, ErrorHandler errorHandler) {
        return new ResponseHandler() {
            @Override
            public void handleResult(long id, byte type, InputBuffer input) {
                resultHandler.handle(id, type, input);
            }
            @Override
            public void handleError(long id, int code) {
                errorHandler.handle(id, code);
            }
        };
    }
}
