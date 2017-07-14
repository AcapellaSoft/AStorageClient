package ru.acapella.kv.dt.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArray;
import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.data.Serializable;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

/**
 * Ответ на {@link DtGetRequest}.
 */
public class DtGetResponse implements ResponseBase, ProtostuffSerializable {
    /**
     * Значение в бинарном виде.
     * @see Serializable#deserialize(ByteArray)
     * @see ByteArray#toObject(Serializable)
     */
    @Tag(1) public ByteArray value;

    public DtGetResponse() {
        value = new ByteArray();
    }

    @Override
    public byte type() {
        return DtGetRequest.TYPE;
    }
}
