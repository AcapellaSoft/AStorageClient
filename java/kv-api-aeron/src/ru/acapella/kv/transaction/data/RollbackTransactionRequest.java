package ru.acapella.kv.transaction.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.function.Supplier;

public class RollbackTransactionRequest implements RequestBase<RollbackTransactionResponse>, ProtostuffSerializable {
    public static final byte TYPE = 0x0B;

    @Tag(1) public long index;

    public RollbackTransactionRequest() {
        index = 0;
    }

    public RollbackTransactionRequest(long index) {
        this.index = index;
    }

    @Override
    public byte type() {
        return TYPE;
    }

    @Override
    public Supplier<RollbackTransactionResponse> response() {
        return RollbackTransactionResponse::new;
    }
}
