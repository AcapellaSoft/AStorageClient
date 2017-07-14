package ru.acapella.kv.paxos.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArrayList;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.function.Supplier;

/**
 * Запрос на получение версии указанного ключа.
 */
public class GetVersionRequest implements RequestBase<GetVersionResponse>, ProtostuffSerializable {
    public static final byte TYPE = 0x16;

    @Tag(1) public byte n;
    @Tag(2) public byte r;
    @Tag(3) public byte w;
    @Tag(4) public ByteArrayList key;

    /**
     * Создание пустого запроса. Используется для последующей десериализации.
     */
    public GetVersionRequest() {
        n = 0;
        r = 0;
        w = 0;
        key = new ByteArrayList();
    }

    /**
     * Создание запроса с указанием ключа.
     * Параметры репликации выбираются по умолчанию (n = 3, r = 2, w = 2).
     * @param key ключ, для которого будет получена версия
     */
    public GetVersionRequest(ByteArrayList key) {
        this.key = key;
        this.n = 3;
        this.r = 2;
        this.w = 2;
    }

    /**
     * Установка параметров репликации.
     * @param n количество реплик
     * @param r количество ответов для подтверждения чтения
     * @param w количество ответов для подтверждения записи
     * @return этот объект
     */
    public GetVersionRequest replicas(byte n, byte r, byte w) {
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
    public Supplier<GetVersionResponse> response() {
        return GetVersionResponse::new;
    }
}
