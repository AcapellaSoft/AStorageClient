package ru.acapella.kv.core.data;

import org.agrona.BitUtil;
import ru.acapella.kv.core.buffers.InputBuffer;
import ru.acapella.kv.core.buffers.InputWrapBuffer;
import ru.acapella.kv.core.buffers.OutputBuffer;
import ru.acapella.kv.core.buffers.OutputWrapBuffer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public interface DefaultSerializable extends Serializable {

    default int binarySize() {
        throw new UnsupportedOperationException();
    }

    @Override
    default ByteArray serialize() {
        ByteArray result = new ByteArray(this.binarySize());
        OutputWrapBuffer output = new OutputWrapBuffer(result.data(), 0);
        serialize(output);
        return result;
    }

    @Override
    default void deserialize(ByteArray bytes) {
        if (!bytes.isEmpty()) {
            InputWrapBuffer input = new InputWrapBuffer(bytes.data(), 0);
            deserialize(input);
        }
    }

    /**
     * Метод стандартной сериализации списка объектов.
     * @param list список сериализуемых объектов
     * @param output буфер для сериализации
     * @param <T> тип сериализуемых объектов
     */
    static <T extends Serializable> void serializeList(List<T> list, OutputBuffer output) {
        int count = list.size();
        output.putInt(count);
        for (T t: list) t.serialize(output);
    }

    /**
     * Метод стандартной десериализации списка объектов.
     * @param allocator метод для создания новых объектов указанного типа
     * @param input буфер для десериализации
     * @param <T> тип десериализации объектов
     * @return список десериализуемых объектов
     */
    static <T extends Serializable> List<T> deserializeList(Supplier<T> allocator, InputBuffer input) {
        int count = input.getInt();
        List<T> list = new ArrayList<>(count);

        for (int i = 0; i < count; ++i) {
            T t = allocator.get();
            t.deserialize(input);
            list.add(t);
        }

        return list;
    }

    /**
     * Метод стандартного вычисления размера списка объектов.
     * @param list список сериализуемых объектов
     * @param <T> тип сериализуемых объектов
     * @return размер списка в байтах
     */
    static <T extends DefaultSerializable> int binarySizeList(List<T> list) {
        int size = BitUtil.SIZE_OF_INT;
        for (T t : list) size += t.binarySize();
        return size;
    }
}
