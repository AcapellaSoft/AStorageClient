package ru.acapella.kv.transaction.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArray;
import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

public class TransactGetResponse implements ResponseBase, ProtostuffSerializable {
    @Tag(1) public long version;
    @Tag(2) public ByteArray value;

    public TransactGetResponse() {
        version = 0;
        value = new ByteArray();
    }

    @Override
    public byte type() {
        return TransactGetRequest.TYPE;
    }
}
