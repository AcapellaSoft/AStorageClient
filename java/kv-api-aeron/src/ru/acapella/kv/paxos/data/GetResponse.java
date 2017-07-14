package ru.acapella.kv.paxos.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArray;
import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.data.Serializable;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

/**
 * Ответ на {@link GetRequest}
 */
public class GetResponse implements ResponseBase, ProtostuffSerializable {
    /**
     * Текущая версия значения.
     */
    @Tag(1) public long version;

    /**
     * Значение в бинарном виде.
     * @see Serializable#deserialize(ByteArray)
     * @see ByteArray#toObject(Serializable)
     */
    @Tag(2) public ByteArray value;

    public GetResponse() {
        version = 0;
        value = new ByteArray();
    }

    @Override
    public byte type() {
        return GetRequest.TYPE;
    }
}
