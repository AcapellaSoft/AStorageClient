package ru.acapella.kv.core.data.protostuff;

import io.protostuff.runtime.RuntimeSchema;
import ru.acapella.kv.core.buffers.InputBuffer;
import ru.acapella.kv.core.buffers.OutputBuffer;
import ru.acapella.kv.core.data.ByteArray;
import ru.acapella.kv.core.data.Serializable;

public interface ProtostuffSerializable extends Serializable {

    default RuntimeSchema schema() {
        return ProtostuffUtils.schema(this.getClass());
    }

    @Override
    default void serialize(OutputBuffer output) {
        //noinspection unchecked
        ProtostuffUtils.serialize(this, schema(), output);
    }

    @Override
    default void deserialize(InputBuffer input) {
        //noinspection unchecked
        ProtostuffUtils.deserialize(this, schema(), input);
    }

    @Override
    default ByteArray serialize() {
        //noinspection unchecked
        return new ByteArray(ProtostuffUtils.serialize(this, schema()));
    }

    @Override
    default void deserialize(ByteArray bytes) {
        //noinspection unchecked
        ProtostuffUtils.deserialize(this, schema(), bytes.data());
    }
}
