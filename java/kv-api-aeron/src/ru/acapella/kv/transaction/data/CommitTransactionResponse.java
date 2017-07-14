package ru.acapella.kv.transaction.data;

import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

public class CommitTransactionResponse implements ResponseBase, ProtostuffSerializable {

    @Override
    public byte type() {
        return CommitTransactionRequest.TYPE;
    }
}
