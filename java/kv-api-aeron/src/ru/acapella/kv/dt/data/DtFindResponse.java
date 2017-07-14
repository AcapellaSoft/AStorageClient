package ru.acapella.kv.dt.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArray;
import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.data.Serializable;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

/**
 * Ответ на {@link DtFindRequest}.
 */
public class DtFindResponse implements ResponseBase, ProtostuffSerializable {
    /**
     * Значение в бинарном виде.
     * @see Serializable#deserialize(ByteArray)
     * @see ByteArray#toObject(Serializable)
     */
    @Tag(1) public ByteArray value;

    /**
     * Узел DT, в котором лежит указанный в запросе ключ.
     * Может использоватся в других запросах для ускорения
     * последовательного доступа.
     * Может вернуть {@link ByteArray#EMPTY}, если ключ не найден.
     */
    @Tag(2) public ByteArray node;

    public DtFindResponse() {
        value = new ByteArray();
        node = new ByteArray();
    }

    @Override
    public byte type() {
        return DtFindRequest.TYPE;
    }
}
