package ru.acapella.kv.dt.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArray;
import ru.acapella.kv.core.data.ByteArrayList;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.function.Supplier;

/**
 * Запрос на поиск следующего ключа в дереве.
 */
public class DtNextRequest implements RequestBase<DtNextResponse>, ProtostuffSerializable {
    public static final byte TYPE = 0x12;

    @Tag(1) public ByteArrayList treeName;
    @Tag(2) public ByteArray node;
    @Tag(3) public ByteArrayList key;
    @Tag(4) public long transaction;
    @Tag(5) public byte n;
    @Tag(6) public byte r;
    @Tag(7) public byte w;

    /**
     * Создание пустого запроса. Используется для последующей десериализации.
     */
    public DtNextRequest() {
        treeName = new ByteArrayList();
        node = new ByteArray();
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
    public DtNextRequest(ByteArrayList treeName, ByteArrayList key) {
        this.treeName = treeName;
        this.node = ByteArray.EMPTY.copy();
        this.key = key;
        this.transaction = 0;
        this.n = 3;
        this.n = 2;
        this.n = 2;
    }

    /**
     * Указание узла в дереве, с которого нужно начинать поиск.
     * По умолчанию - корень дервева.
     * Если указанный узел уже не пренадлежит дереву,
     * то поиск начнётся заново с корня.
     * @param node ID начального узла
     * @return этот объект
     */
    public DtNextRequest node(ByteArray node) {
        this.node = node;
        return this;
    }

    /**
     * Указание транзакции, в которой следует выполнить этот запрос.
     * По умолчанию запрос выполняется в своей транзакции.
     * @param index индекс транзакции
     * @return этот объект
     */
    public DtNextRequest transaction(long index) {
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
    public DtNextRequest replicas(byte n, byte r, byte w) {
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
    public Supplier<DtNextResponse> response() {
        return DtNextResponse::new;
    }
}
