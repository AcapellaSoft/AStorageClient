package ru.acapella.kv.dt.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArray;
import ru.acapella.kv.core.data.ByteArrayList;
import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.data.Serializable;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

/**
 * Ответ на {@link DtPrevRequest}.
 */
public class DtPrevResponse implements ResponseBase, ProtostuffSerializable {
    /**
     * Предыдущий за указанным в запросе ключ.
     * Если ключ из запроса был первым в дереве,
     * то в этом поле будет пустой ключ.
     * @see ByteArrayList#isEmpty()
     */
    @Tag(1) public ByteArrayList key;

    /**
     * Значение в бинарном виде.
     * @see Serializable#deserialize(ByteArray)
     * @see ByteArray#toObject(Serializable)
     */
    @Tag(2) public ByteArray value;

    /**
     * Узел DT, в котором лежит найденный ключ.
     * Может использоватся в других запросах для ускорения
     * последовательного доступа.
     * Может вернуть {@link ByteArray#EMPTY}, если ключ не найден.
     */
    @Tag(3) public ByteArray node;

    public DtPrevResponse() {
        key = new ByteArrayList();
        value = new ByteArray();
        node = new ByteArray();
    }

    @Override
    public byte type() {
        return DtPrevRequest.TYPE;
    }
}
