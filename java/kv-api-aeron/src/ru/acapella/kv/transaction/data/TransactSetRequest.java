package ru.acapella.kv.transaction.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArray;
import ru.acapella.kv.core.data.ByteArrayList;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.function.Supplier;

public class TransactSetRequest implements RequestBase<TransactSetResponse>, ProtostuffSerializable {
    public static final byte TYPE = 0x0D;

    @Tag(1) public byte n;
    @Tag(2) public byte r;
    @Tag(3) public byte w;
    @Tag(4) public long transactIndex;
    @Tag(5) public ByteArrayList key;
    @Tag(6) public ByteArray value;
    @Tag(7) public boolean cas;
    @Tag(8) public long version;

    public TransactSetRequest() {
        n = 0;
        r = 0;
        w = 0;
        key = new ByteArrayList();
        value = new ByteArray();
        cas = false;
        version = 0;
    }

    public TransactSetRequest(long transactIndex, ByteArrayList key, ByteArray value) {
        this.transactIndex = transactIndex;
        this.key = key;
        this.value = value;
        this.cas = false;
    }

    public TransactSetRequest replicas(byte n, byte r, byte w) {
        this.n = n;
        this.r = r;
        this.w = w;
        return this;
    }

    public TransactSetRequest condVersion(long version) {
        this.cas = true;
        this.version = version;
        return this;
    }

    @Override
    public byte type() {
        return TYPE;
    }

    @Override
    public Supplier<TransactSetResponse> response() {
        return TransactSetResponse::new;
    }
}
