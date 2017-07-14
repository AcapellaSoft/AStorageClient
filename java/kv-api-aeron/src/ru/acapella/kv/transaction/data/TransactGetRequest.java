package ru.acapella.kv.transaction.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArrayList;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.function.Supplier;

public class TransactGetRequest implements RequestBase<TransactGetResponse>, ProtostuffSerializable {
    public static final byte TYPE = 0x0E;

    @Tag(1) public byte n;
    @Tag(2) public byte r;
    @Tag(3) public byte w;
    @Tag(4) public long transactIndex;
    @Tag(5) public boolean uncommitted;
    @Tag(6) public ByteArrayList key;

    public TransactGetRequest() {
        n = 0;
        r = 0;
        w = 0;
        transactIndex = 0;
        uncommitted = false;
        key = new ByteArrayList();
    }

    public TransactGetRequest(long transactIndex, ByteArrayList key) {
        this.transactIndex = transactIndex;
        this.key = key;
        this.n = 3;
        this.r = 2;
        this.w = 2;
        this.uncommitted = false;
    }

    public TransactGetRequest transaction(long index) {
        this.transactIndex = index;
        return this;
    }

    public TransactGetRequest key(ByteArrayList key) {
        this.key = key;
        return this;
    }

    public TransactGetRequest replicas(byte n, byte r, byte w) {
        this.n = n;
        this.r = r;
        this.w = w;
        return this;
    }

    @Override
    public byte type() {
        return TYPE;
    }

    @Override
    public Supplier<TransactGetResponse> response() {
        return TransactGetResponse::new;
    }
}
