package ru.acapella.kv.dt.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArray;
import ru.acapella.kv.core.data.ByteArrayList;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.Serializable;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.function.Supplier;

/**
 * Запрос на установку значения в дереве по ключу.
 */
public class DtSetRequest implements RequestBase<DtSetResponse>, ProtostuffSerializable {
    public static final byte TYPE = 0x10;

    @Tag(1) public ByteArrayList treeName;
    @Tag(2) public ByteArrayList key;
    @Tag(3) public ByteArray value;
    @Tag(4) public long transaction;
    @Tag(5) public byte n;
    @Tag(6) public byte r;
    @Tag(7) public byte w;

    /**
     * Создание пустого запроса. Используется для последующей десериализации.
     */
    public DtSetRequest() {
        treeName = new ByteArrayList();
        key = new ByteArrayList();
        value = new ByteArray();
        n = 0;
        r = 0;
        w = 0;
    }

    /**
     * Создание запроса с указанием имени дерева, ключа и значения.
     * Остальные параметры устанавливаются по умолчанию.
     * @see ByteArray#fromObject(Serializable)
     * @see Serializable#serialize()
     * @param treeName имя дерева
     * @param key ключ
     * @param value значение
     */
    public DtSetRequest(ByteArrayList treeName, ByteArrayList key, ByteArray value) {
        this.treeName = treeName;
        this.key = key;
        this.value = value;
        this.n = 3;
        this.r = 2;
        this.w = 2;
    }

    /**
     * Указание транзакции, в которой следует выполнить этот запрос.
     * По умолчанию запрос выполняется в своей транзакции.
     * @param index индекс транзакции
     * @return этот объект
     */
    public DtSetRequest transaction(long index) {
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
    public DtSetRequest replicas(byte n, byte r, byte w) {
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
    public Supplier<DtSetResponse> response() {
        return DtSetResponse::new;
    }
}
