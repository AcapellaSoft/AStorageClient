package ru.acapella.kv.dt.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArrayList;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.function.Supplier;

/**
 * Запрос на выборку диапазона ключей из дерева.
 * ВАЖНО: выборка не консистентна! То есть пока происходит выполнение запроса
 * в дерево могут добавиться или удалиться данные, о которых запрос не сообщит.
 */
public class DtRangeRequest implements RequestBase<DtRangeResponse>, ProtostuffSerializable {
    public static final byte TYPE = 0x14;

    @Tag(1) public ByteArrayList treeName;
    @Tag(2) public ByteArrayList firstKey;
    @Tag(3) public ByteArrayList lastKey;
    @Tag(4) public int limit;
    @Tag(5) public long transaction;
    @Tag(6) public byte n;
    @Tag(7) public byte r;
    @Tag(8) public byte w;

    /**
     * Создание пустого запроса. Используется для последующей десериализации.
     */
    public DtRangeRequest() {
        treeName = new ByteArrayList();
        firstKey = new ByteArrayList();
        lastKey = new ByteArrayList();
        limit = 0;
        transaction = 0;
        n = 0;
        r = 0;
        w = 0;
    }

    /**
     * Создание запроса с указанием имени дерева.
     * Остальные параметры устанавливаются по умолчанию.
     * Без указания firstKey, lastKey или limit - возвращает все ключи в дереве.
     * @param treeName имя дерева
     */
    public DtRangeRequest(ByteArrayList treeName) {
        this.treeName = treeName;
        this.firstKey = new ByteArrayList();
        this.lastKey = new ByteArrayList();
        this.limit = 0;
        this.transaction = 0;
        this.n = 3;
        this.r = 2;
        this.w = 2;
    }

    /**
     * Установка ключа, с которого начнётся выборка из дерева.
     * Результат не включает этот ключ.
     * Пустой ключ (по умолчанию) - выборка с самого начала.
     * @param firstKey первый ключ
     * @return этот объект
     */
    public DtRangeRequest from(ByteArrayList firstKey) {
        this.firstKey = firstKey;
        return this;
    }

    /**
     * Установка ключа, на котором закончится выборка из дерева.
     * Результат включает этот ключ.
     * Пустой ключ (по умолчанию) - выборка до последнего ключа в дереве.
     * @param lastKey последний ключ
     * @return этот объект
     */
    public DtRangeRequest to(ByteArrayList lastKey) {
        this.lastKey = lastKey;
        return this;
    }

    /**
     * Установка лимита на количество ключей.
     * Если от firstKey до lastKey ключей больше, чем limit,
     * то в ответе будут первые по порядку ключи.
     * Если limit == 0 (по умолчанию) - выборка без ограничения.
     * @param limit предел
     * @return этот объект
     */
    public DtRangeRequest limit(int limit) {
        this.limit = limit;
        return this;
    }

    /**
     * Указание транзакции, в которой следует выполнить этот запрос.
     * По умолчанию запрос выполняется в своей транзакции.
     * @param index индекс транзакции
     * @return этот объект
     */
    public DtRangeRequest transaction(long index) {
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
    public DtRangeRequest replicas(byte n, byte r, byte w) {
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
    public Supplier<DtRangeResponse> response() {
        return DtRangeResponse::new;
    }
}
