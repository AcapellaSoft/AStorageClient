package ru.acapella.kv.transaction.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

public class TransactSetResponse implements ResponseBase, ProtostuffSerializable {
    @Tag(1) public long version;

    public TransactSetResponse() {
        version = 0;
    }

    @Override
    public byte type() {
        return TransactSetRequest.TYPE;
    }
}
