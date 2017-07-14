package ru.acapella.kv.core.data.protostuff;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.runtime.RuntimeSchema;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import ru.acapella.kv.core.buffers.InputBuffer;
import ru.acapella.kv.core.buffers.OutputBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class ProtostuffUtils {
    private static final int BUFFER_SIZE = 1024 * 1024;
    private static final LinkedBuffer LINKED_BUFFER = LinkedBuffer.allocate(BUFFER_SIZE);
    private static final Map<Class<?>, RuntimeSchema<?>> SCHEMA_CACHE = new HashMap<>();

    public static RuntimeSchema schema(Class<?> cls) {
        return SCHEMA_CACHE.computeIfAbsent(cls, k -> RuntimeSchema.createFrom(cls));
    }

    private static class OutputBufferStream extends OutputStream {
        private final OutputBuffer output;

        public OutputBufferStream(OutputBuffer output) {
            this.output = output;
        }

        @Override
        public void write(@NotNull byte[] b) throws IOException {
            output.putBytes(b);
        }

        @Override
        public void write(@NotNull byte[] b, int off, int len) throws IOException {
            output.putBytes(b, off, len);
        }

        @Override
        public void write(int b) throws IOException {
            output.putByte((byte) b);
        }
    }

    private static class InputBufferStream extends InputStream {
        private final InputBuffer input;

        public InputBufferStream(InputBuffer input) {
            this.input = input;
        }

        @Override
        public int read(@NotNull byte[] b) throws IOException {
            input.getBytes(b);
            return b.length;
        }

        @Override
        public int read(@NotNull byte[] b, int off, int len) throws IOException {
            input.getBytes(b, off, len);
            return len;
        }

        @Override
        public int read() throws IOException {
            return Byte.toUnsignedInt(input.getByte());
        }
    }

    @SneakyThrows
    public static <T> void serialize(T object, RuntimeSchema<T> schema, OutputBuffer output) {
        LINKED_BUFFER.clear();
        OutputBufferStream s = new OutputBufferStream(output);
        ProtostuffIOUtil.writeDelimitedTo(s, object, schema, LINKED_BUFFER);
    }

    @SneakyThrows
    public static <T> void deserialize(T object, RuntimeSchema<T> schema, InputBuffer input) {
        LINKED_BUFFER.clear();
        InputBufferStream s = new InputBufferStream(input);
        ProtostuffIOUtil.mergeDelimitedFrom(s, object, schema, LINKED_BUFFER);
    }

    public static <T> byte[] serialize(T object, RuntimeSchema<T> schema) {
        LINKED_BUFFER.clear();
        return ProtostuffIOUtil.toByteArray(object, schema, LINKED_BUFFER);
    }

    public static <T> void deserialize(T object, RuntimeSchema<T> schema, byte[] bytes) {
        ProtostuffIOUtil.mergeFrom(bytes, object, schema);
    }
}
