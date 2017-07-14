package ru.acapella.kv.paxos.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArray;
import ru.acapella.kv.core.data.ByteArrayList;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.Serializable;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.function.Supplier;

/**
 * Запрос на установку значения по указанному ключу. Пример использования:
 * <pre>
 * {@code
 *  TransactionInfo value = ...;
 *  new SetRequest(key, value.serialize())
 *          .replicas(5, 3, 3)
 *          .condVersion(oldVersion)
 *          .expire(120)
 *          .send(client, onSuccess, onFail)
 * }
 * </pre>
 */
public class SetRequest implements RequestBase<SetResponse>, ProtostuffSerializable {
    public static final byte TYPE = 0x02;

    public static final byte COND_ALWAYS = 0; // применить в любом случае
    public static final byte COND_EXISTS = 1; // применить, если ключ существует
    public static final byte COND_NOT_EXISTS = 2; // применить, если ключ не существует
    public static final byte COND_VERSION = 3; // применить, если указанная версия совпадает с версией ключа

    public static final int EXPIRE_NONE = 0; // снять expire
    public static final int EXPIRE_NOT_SET = -1; // оставить текущий

    @Tag(1) public byte n;
    @Tag(2) public byte r;
    @Tag(3) public byte w;
    @Tag(4) public ByteArrayList key;
    @Tag(5) public ByteArray value;
    @Tag(6) public byte condition;
    @Tag(7) public long version;
    @Tag(8) public int expire; // в секундах, 0 - expire не установлен

    /**
     * Создание пустого запроса. Используется для последующей десериализации.
     */
    public SetRequest() {
        n = 0;
        r = 0;
        w = 0;
        key = new ByteArrayList();
        value = new ByteArray();
        condition = COND_ALWAYS;
        version = 0;
        expire = 0;
    }

    /**
     * Создание запроса с указанием ключа и значения.
     * Остальные параметры устанавливаются по умолчанию.
     * @see ByteArray#fromObject(Serializable)
     * @see Serializable#serialize()
     * @param key ключ, по которому будет установлено значение
     * @param value новое значение в бинарном виде
     */
    public SetRequest(ByteArrayList key, ByteArray value) {
        this.key = key;
        this.value = value;
        this.n = 3;
        this.r = 2;
        this.w = 2;
        this.condition = COND_ALWAYS;
        this.version = 0;
        this.expire = 0;
    }

    /**
     * Установка параметров репликации.
     * @param n количество реплик
     * @param r количество ответов для подтверждения чтения
     * @param w количество ответов для подтверждения записи
     * @return этот объект
     */
    public SetRequest replicas(byte n, byte r, byte w) {
        this.n = n;
        this.r = r;
        this.w = w;
        return this;
    }

    /**
     * Устанавливает проверку на старую версию значения (CAS). Если указанная версия
     * не совпадёт с версией в базе, то в ответе поле {@code status} будет выставлено в {@code false}.
     * @param version старая версия значения
     * @return этот объект
     */
    public SetRequest condVersion(long version) {
        this.version = version;
        this.condition = COND_VERSION;
        return this;
    }

    /**
     * Устанавливает проверку на отсутствие значения. Если в базе уже есть значение
     * по этому ключу, то в ответе поле {@code status} будет выставлено в {@code false}.
     * @return этот объект
     */
    public SetRequest condNotExists() {
        this.condition = COND_NOT_EXISTS;
        return this;
    }

    /**
     * Устанавливает проверку присутствия значения. Если значения не было в базе или
     * оно было удалено, то в ответе поле {@code status} будет выставлено в {@code false}.
     * @return этот объект
     */
    public SetRequest condExists() {
        this.condition = COND_EXISTS;
        return this;
    }

    /**
     * Позволяет задать таймаут, после которого значение будет удалено
     * @param expire таймаут в секундах
     * @return этот объект
     */
    public SetRequest expire(int expire) {
        this.expire = expire;
        return this;
    }

    @Override
    public byte type() {
        return TYPE;
    }

    @Override
    public Supplier<SetResponse> response() {
        return SetResponse::new;
    }
}
