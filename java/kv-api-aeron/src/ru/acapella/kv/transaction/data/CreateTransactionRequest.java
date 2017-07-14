package ru.acapella.kv.transaction.data;

import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.function.Supplier;

public class CreateTransactionRequest implements RequestBase<CreateTransactionResponse>, ProtostuffSerializable {
    public static final byte TYPE = 0x09;

    public CreateTransactionRequest() {
    }

    @Override
    public byte type() {
        return TYPE;
    }

    @Override
    public Supplier<CreateTransactionResponse> response() {
        return CreateTransactionResponse::new;
    }
}
