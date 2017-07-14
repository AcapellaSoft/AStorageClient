package ru.acapella.kv.core.data;

import ru.acapella.kv.core.buffers.InputBuffer;
import ru.acapella.kv.core.buffers.OutputBuffer;

/**
 * Интерфейс для бинарной сериализации в {@link OutputBuffer} и
 * десериализации из {@link InputBuffer}.
 */
public interface Serializable {
    /**
     * Сериализация в {@link OutputBuffer}.
     * @param output буфер для сериализации
     */
    void serialize(OutputBuffer output);

    /**
     * Десериализация из {@link InputBuffer}.
     * @param input буфер для десериализации
     */
    void deserialize(InputBuffer input);

    /**
     * Сериализация в {@link ByteArray}.
     */
    ByteArray serialize();

    /**
     * Десериализация из {@link ByteArray}.
     * @param bytes массив байт, из которого будет десериализован объект
     */
    void deserialize(ByteArray bytes);
}
