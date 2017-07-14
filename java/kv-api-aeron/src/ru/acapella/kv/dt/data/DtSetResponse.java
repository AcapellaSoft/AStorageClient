package ru.acapella.kv.dt.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArray;
import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

public class DtSetResponse implements ResponseBase, ProtostuffSerializable {
    /**
     * Узел DT, в котором лежит указанный в запросе ключ.
     * Может использоватся в других запросах для ускорения
     * последовательного доступа.
     * Может вернуть {@link ByteArray#EMPTY}, если в ходе выполения запроса
     * не потребовался проход по дереву.
     */
    @Tag(1) public ByteArray node;

    public DtSetResponse() {
        node = new ByteArray();
    }

    @Override
    public byte type() {
        return DtSetRequest.TYPE;
    }
}
