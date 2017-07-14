package ru.acapella.kv.core;

import ru.acapella.kv.core.handlers.RequestHandler;

import java.util.Random;

public class RequestPool {
    private final RequestHandler[] pool;
    private final Random rand;
    private final int attempts;

    private int count;

    public RequestPool(int size, int attempts) {
        this.pool = new RequestHandler[size];
        this.rand = new Random();
        this.attempts = attempts;
        this.count = 0;
    }

    public int put(RequestHandler handler) {
        int attempt = attempts;
        RequestHandler old;
        int index;

        do {
            index = rand.nextInt(pool.length);
            old = pool[index];
            --attempt;
        } while (attempt > 0 && old != null);

        try {
            if (old != null) {
                --count;
                old.cancel();
            }
        } catch (Throwable ignored) {}

        pool[index] = handler;
        ++count;
        return index;
    }

    public void complete(int index, RequestHandler handler) {
        if (pool[index] == handler) {
            --count;
            pool[index] = null;
        }
    }

    public boolean isEmpty() {
        return count == 0;
    }
}
