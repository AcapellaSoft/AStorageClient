package ru.acapella.kv.core.buffers;

import java.nio.ByteOrder;

public interface InputBuffer {
    int offset();
    void offset(int value);

    long getLong();
    int getInt();
    short getShort();
    boolean getBoolean();
    byte getByte();
    void getBytes(byte[] bytes);
    void getBytes(byte[] bytes, int offset, int length);

    long getLong(ByteOrder order);
    int getInt(ByteOrder order);
    short getShort(ByteOrder order);
}
