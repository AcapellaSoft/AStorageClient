package ru.acapella.kv.paxos.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

/**
 * Ответ на {@link GetVersionRequest}
 */
public class GetVersionResponse implements ResponseBase, ProtostuffSerializable {
    /**
     * Текущая версия значения.
     */
    @Tag(1) public long version;

    public GetVersionResponse() {
        version = 0;
    }

    @Override
    public byte type() {
        return GetVersionRequest.TYPE;
    }
}
