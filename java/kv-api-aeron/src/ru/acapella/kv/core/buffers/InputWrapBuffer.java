package ru.acapella.kv.core.buffers;

import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

public class InputWrapBuffer implements InputBuffer {
    private DirectBuffer buffer;
    private int index;

    public InputWrapBuffer() {
    }

    public InputWrapBuffer(DirectBuffer buffer, int offset) {
        wrap(buffer, offset);
    }

    public InputWrapBuffer(byte[] buffer, int offset) {
        wrap(new UnsafeBuffer(buffer), offset);
    }

    public void wrap(DirectBuffer buffer, int offset) {
        this.buffer = buffer;
        this.index = offset;
    }

    @Override
    public int offset() {
        return index;
    }

    @Override
    public void offset(int value) {
        index = value;
    }

    @Override
    public long getLong() {
        return getLong(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public int getInt() {
        return getInt(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public short getShort() {
        return getShort(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public boolean getBoolean() {
        boolean value = buffer.getByte(index) != 0;
        index += BitUtil.SIZE_OF_BYTE;
        return value;
    }

    @Override
    public byte getByte() {
        byte value = buffer.getByte(index);
        index += BitUtil.SIZE_OF_BYTE;
        return value;
    }

    @Override
    public void getBytes(byte[] bytes) {
        getBytes(bytes, 0, bytes.length);
    }

    @Override
    public void getBytes(byte[] bytes, int offset, int length) {
        buffer.getBytes(index, bytes, offset, length);
        index += length;
    }

    @Override
    public long getLong(ByteOrder order) {
        long value = buffer.getLong(index, order);
        index += BitUtil.SIZE_OF_LONG;
        return value;
    }

    @Override
    public int getInt(ByteOrder order) {
        int value = buffer.getInt(index, order);
        index += BitUtil.SIZE_OF_INT;
        return value;
    }

    @Override
    public short getShort(ByteOrder order) {
        short value = buffer.getShort(index, order);
        index += BitUtil.SIZE_OF_SHORT;
        return value;
    }
}
