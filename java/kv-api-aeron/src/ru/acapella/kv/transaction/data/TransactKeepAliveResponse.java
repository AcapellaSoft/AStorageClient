package ru.acapella.kv.transaction.data;

import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

public class TransactKeepAliveResponse implements ResponseBase, ProtostuffSerializable {

    @Override
    public byte type() {
        return TransactKeepAliveRequest.TYPE;
    }
}
