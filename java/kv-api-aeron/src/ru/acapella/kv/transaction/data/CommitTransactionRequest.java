package ru.acapella.kv.transaction.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.function.Supplier;

public class CommitTransactionRequest implements RequestBase<CommitTransactionResponse>, ProtostuffSerializable {
    public static final byte TYPE = 0x0A;

    @Tag(1) public long index;

    public CommitTransactionRequest() {
        index = 0;
    }

    public CommitTransactionRequest(long index) {
        this.index = index;
    }

    @Override
    public byte type() {
        return TYPE;
    }

    @Override
    public Supplier<CommitTransactionResponse> response() {
        return CommitTransactionResponse::new;
    }
}
