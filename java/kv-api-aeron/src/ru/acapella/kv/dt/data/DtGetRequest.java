package ru.acapella.kv.dt.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArrayList;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.function.Supplier;

/**
 * Запрос на получение значения по ключу из дерева.
 */
public class DtGetRequest implements RequestBase<DtGetResponse>, ProtostuffSerializable {
    public static final byte TYPE = 0x0F;

    @Tag(1) public ByteArrayList treeName;
    @Tag(2) public ByteArrayList key;
    @Tag(3) public long transaction;
    @Tag(4) public byte n;
    @Tag(5) public byte r;
    @Tag(6) public byte w;

    /**
     * Создание пустого запроса. Используется для последующей десериализации.
     */
    public DtGetRequest() {
        treeName = new ByteArrayList();
        key = new ByteArrayList();
        transaction = 0;
        n = 0;
        r = 0;
        w = 0;
    }

    /**
     * Создание запроса с указанием имени дерева и ключа.
     * Остальные параметры устанавливаются по умолчанию.
     * @param treeName имя дерева
     * @param key ключ
     */
    public DtGetRequest(ByteArrayList treeName, ByteArrayList key) {
        this.treeName = treeName;
        this.key = key;
        this.transaction = 0;
        this.n = 3;
        this.n = 2;
        this.n = 2;
    }

    /**
     * Указание транзакции, в которой следует выполнить этот запрос.
     * По умолчанию запрос выполняется в своей транзакции.
     * @param index индекс транзакции
     * @return этот объект
     */
    public DtGetRequest transaction(long index) {
        this.transaction = index;
        return this;
    }

    /**
     * Установка параметров репликации.
     * @param n количество реплик
     * @param r количество ответов для подтверждения чтения
     * @param w количество ответов для подтверждения записи
     * @return этот объект
     */
    public DtGetRequest replicas(byte n, byte r, byte w) {
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
    public Supplier<DtGetResponse> response() {
        return DtGetResponse::new;
    }
}
