package ru.acapella.kv.core.data;

import ru.acapella.kv.core.buffers.InputBuffer;

public interface ResponseBase extends Serializable {
    byte type();

    default void deserialize(long id, byte type, InputBuffer input) {
        if (type != type())
            throw new RuntimeException(String.format("invalid message type %d with id %d", type, id));
        deserialize(input);
    }
}
