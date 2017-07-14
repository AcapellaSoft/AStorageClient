package ru.acapella.kv.dt.data;

import ru.acapella.kv.core.data.ByteArray;
import ru.acapella.kv.core.data.ByteArrayList;

public class DtEntry {
    private final ByteArrayList key;
    private final ByteArray value;

    public DtEntry(ByteArrayList key, ByteArray value) {
        this.key = key;
        this.value = value;
    }

    public ByteArrayList key() { return key; }

    public ByteArray value() { return value; }
}
