package ru.acapella.kv.core.buffers;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteOrder;

public class OutputWrapBuffer implements OutputBuffer {
    private MutableDirectBuffer buffer;
    private int index;

    public OutputWrapBuffer() {
    }

    public OutputWrapBuffer(MutableDirectBuffer buffer, int offset) {
        wrap(buffer, offset);
    }

    public OutputWrapBuffer(byte[] buffer, int offset) {
        wrap(new UnsafeBuffer(buffer), offset);
    }

    public void wrap(MutableDirectBuffer buffer, int index) {
        this.buffer = buffer;
        this.index = index;
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
    public void putLong(long value) {
        putLong(value, ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public void putInt(int value) {
        putInt(value, ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public void putShort(short value) {
        putShort(value, ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public void putBoolean(boolean value) {
        buffer.putByte(index, (byte) (value ? 1 : 0));
        index += BitUtil.SIZE_OF_BYTE;
    }

    @Override
    public void putByte(byte value) {
        buffer.putByte(index, value);
        index += BitUtil.SIZE_OF_BYTE;
    }

    @Override
    public void putBytes(byte[] bytes) {
        putBytes(bytes, 0, bytes.length);
    }

    @Override
    public void putBytes(byte[] bytes, int offset, int length) {
        buffer.putBytes(index, bytes, offset, length);
        index += length;
    }

    @Override
    public void putLong(long value, ByteOrder order) {
        buffer.putLong(index, value, order);
        index += BitUtil.SIZE_OF_LONG;
    }

    @Override
    public void putInt(int value, ByteOrder order) {
        buffer.putInt(index, value, order);
        index += BitUtil.SIZE_OF_INT;
    }

    @Override
    public void putShort(short value, ByteOrder order) {
        buffer.putShort(index, value, order);
        index += BitUtil.SIZE_OF_SHORT;
    }
}
