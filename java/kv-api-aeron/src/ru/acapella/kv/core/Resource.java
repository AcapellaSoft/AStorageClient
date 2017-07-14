package ru.acapella.kv.core;

import java.io.Closeable;

@FunctionalInterface
public interface Resource extends Closeable {
    @Override
    void close();
}
