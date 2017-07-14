package ru.acapella.kv.transaction.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.function.Supplier;

public class TransactKeepAliveRequest implements RequestBase<TransactKeepAliveResponse>, ProtostuffSerializable {
    public static final byte TYPE = 0x0C;

    @Tag(1) public long index;

    public TransactKeepAliveRequest() {
        index = 0;
    }

    public TransactKeepAliveRequest(long index) {
        this.index = index;
    }

    @Override
    public byte type() {
        return TYPE;
    }

    @Override
    public Supplier<TransactKeepAliveResponse> response() {
        return TransactKeepAliveResponse::new;
    }
}
