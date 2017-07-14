package ru.acapella.kv.dt.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.ArrayList;
import java.util.List;

/**
 *  Ответ на {@link DtRangeRequest}.
 */
public class DtRangeResponse implements ResponseBase, ProtostuffSerializable {
    /**
     * Отсортированный список полученных данных.
     */
    @Tag(1) public List<DtEntry> data;

    public DtRangeResponse() {
        data = new ArrayList<>();
    }

    @Override
    public byte type() {
        return DtRangeRequest.TYPE;
    }
}
