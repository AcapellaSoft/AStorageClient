package ru.acapella.kv.core.buffers;

import java.nio.ByteOrder;

public interface OutputBuffer {
    int offset();
    void offset(int value);

    void putLong(long value);
    void putInt(int value);
    void putShort(short value);
    void putBoolean(boolean value);
    void putByte(byte value);
    void putBytes(byte[] bytes);
    void putBytes(byte[] bytes, int offset, int length);

    void putLong(long value, ByteOrder order);
    void putInt(int value, ByteOrder order);
    void putShort(short value, ByteOrder order);
}
