package ru.acapella.kv.paxos.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

/**
 * Ответ на {@link SetRequest}.
 */
public class SetResponse implements ResponseBase, ProtostuffSerializable {
    /**
     * Статус выполения операции. Если были указаны какие-либо условия при запросе,
     * то при их невыполнении в этом поле будет {@code false}.
     */
    @Tag(1) public boolean status;

    /**
     * При успешном выполнении ({@code status == true}) - новая версия значения.
     * При неудачном ({@code status == false}) - текущая версия значения в базе.
     */
    @Tag(2) public long version;

    public SetResponse() {
        status = false;
        version = 0;
    }

    @Override
    public byte type() {
        return SetRequest.TYPE;
    }
}
