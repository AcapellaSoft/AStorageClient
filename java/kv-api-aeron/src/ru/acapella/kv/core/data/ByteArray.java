package ru.acapella.kv.core.data;

import org.agrona.BitUtil;
import org.jetbrains.annotations.NotNull;
import ru.acapella.kv.core.buffers.InputBuffer;
import ru.acapella.kv.core.buffers.InputWrapBuffer;
import ru.acapella.kv.core.buffers.OutputBuffer;
import ru.acapella.kv.core.buffers.OutputWrapBuffer;

import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;

/**
 * Класс-обвёртка над массивом байт.
 */
public class ByteArray implements Serializable, Comparable<ByteArray> {
    public static final ByteArray EMPTY = new ByteArray(new byte[0]);

    private byte[] data;

    /**
     * Создание пустого массива байт.
     */
    public ByteArray() {
        data = null;
    }

    /**
     * Создание массива с заданной длиной.
     * @param length длина массива
     */
    public ByteArray(int length) {
        data = new byte[length];
    }

    /**
     * Создание массива с указанными данными.
     * Данные <b>не копируются</b>, а передаются в объект по ссылке.
     * @param data данные
     */
    public ByteArray(byte[] data) {
        this.data = data;
    }

    /**
     * Создание массива из указанной строки.
     * @param data строка для преобоазования в массив байт
     */
    public ByteArray(String data) {
        this(data.getBytes());
    }

    @Override
    public void serialize(OutputBuffer output) {
        output.putInt(data.length);
        output.putBytes(data);
    }

    @Override
    public ByteArray serialize() {
        return this.copy();
    }

    @Override
    public void deserialize(InputBuffer input) {
        int length = input.getInt();
        data = new byte[length];
        input.getBytes(data);
    }

    @Override
    public void deserialize(ByteArray bytes) {
        this.data = bytes.copy().data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ByteArray byteArray = (ByteArray) o;

        return Arrays.equals(data, byteArray.data);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data);
    }

    public ByteArray copy() {
        return new ByteArray(Arrays.copyOf(data, data.length));
    }

    public ByteArray cut() {
        ByteArray result = new ByteArray(data);
        data = EMPTY.data;
        return result;
    }

    public byte[] data() {
        return data;
    }

    /**
     * Сериализует {@link Long} в {@link ByteArray}.
     * <b>Порядок байт - BIG_ENDIAN!</b> Это нужно для правильного сравнения
     * при использовании в качестве ключа.
     * @param value сериализуемое значение
     * @return сериализванные байты
     */
    public static ByteArray fromLong(long value) {
        ByteArray result = new ByteArray(BitUtil.SIZE_OF_LONG);
        OutputWrapBuffer output = new OutputWrapBuffer(result.data, 0);
        output.putLong(value, ByteOrder.BIG_ENDIAN);
        return result;
    }

    public static ByteArray fromInt(int value) {
        ByteArray result = new ByteArray(BitUtil.SIZE_OF_INT);
        OutputWrapBuffer output = new OutputWrapBuffer(result.data, 0);
        output.putInt(value, ByteOrder.BIG_ENDIAN);
        return result;
    }

    public static ByteArray fromByte(byte value) {
        ByteArray result = new ByteArray(BitUtil.SIZE_OF_BYTE);
        OutputWrapBuffer output = new OutputWrapBuffer(result.data, 0);
        output.putByte(value);
        return result;
    }

    public static ByteArray fromUUID(UUID value) {
        ByteArray result = new ByteArray(BitUtil.SIZE_OF_LONG * 2);
        OutputWrapBuffer output = new OutputWrapBuffer(result.data, 0);
        output.putLong(value.getMostSignificantBits());
        output.putLong(value.getLeastSignificantBits());
        return result;
    }

    public static ByteArray fromObject(Serializable obj) {
        return obj == null
                ? ByteArray.EMPTY
                : obj.serialize();
    }

    /**
     * Десериализует {@link Long} из этого объекта.
     * <b>Порядок байт - BIG_ENDIAN!</b> Это нужно для правильного сравнения
     * при использовании в качестве ключа.
     * @return десериализованное значение
     */
    public long toLong() {
        InputWrapBuffer input = new InputWrapBuffer(data, 0);
        return input.getLong(ByteOrder.BIG_ENDIAN);
    }

    public UUID toUUID() {
        InputWrapBuffer input = new InputWrapBuffer(data, 0);
        long high = input.getLong();
        long low = input.getLong();
        return new UUID(high, low);
    }

    /**
     * Десериализация из {@link ByteArray} в тип, реализующий {@link Serializable}.
     * Пример использования:
     * <pre>
     * {@code
     *  ByteArray bytes = ...;
     *  TransactionInfo info = new TransactionInfo();
     *  bytes.toObject(info);
     * }
     * </pre>
     * Эквивалент {@code null} для {@link ByteArray} - это пустое значение. На него можно проверить
     * вызвав метод {@link ByteArray#isEmpty()}.
     * @param value объект, в который будет произведена десериализация
     * @param <T> Тип десериализуемого объекта
     * @return объект из параметра {@code value}
     */
    public <T extends Serializable> T toObject(T value) {
        value.deserialize(this);
        return value;
    }

    public boolean isEmpty() {
        return data.length == 0;
    }

    @Override
    public int compareTo(@NotNull ByteArray other) {
        int count = Math.min(this.data.length, other.data.length);

        for (int i = 0; i < count; ++i) {
            int r = Byte.toUnsignedInt(this.data[i]) - Byte.toUnsignedInt(other.data[i]);
            if (r != 0) return r;
        }

        return this.data.length - other.data.length;
    }

    @Override
    public String toString() {
        return bytesToHex(data);
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }
}
