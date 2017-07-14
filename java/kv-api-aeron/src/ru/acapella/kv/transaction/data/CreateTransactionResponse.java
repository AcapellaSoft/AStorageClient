package ru.acapella.kv.transaction.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

public class CreateTransactionResponse implements ResponseBase, ProtostuffSerializable {
    @Tag(1) public long index;

    public CreateTransactionResponse() {
        index = 0;
    }

    public CreateTransactionResponse(long index) {
        this.index = index;
    }

    @Override
    public byte type() {
        return CreateTransactionRequest.TYPE;
    }
}
