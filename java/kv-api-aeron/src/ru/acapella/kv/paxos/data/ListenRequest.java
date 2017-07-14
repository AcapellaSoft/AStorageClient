package ru.acapella.kv.paxos.data;

import io.protostuff.Tag;
import ru.acapella.kv.core.data.ByteArrayList;
import ru.acapella.kv.core.data.ErrorCodes;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Запрос на ожидание, пока версия по ключу не превысит заданную. Пример использования:
 * <pre>
 * {@code
 *  new ListenRequest(key, version)
 *          .replicas(5, 3)
 *          .timeout(60, TimeUnit.SECONDS)
 *          .send(client, onSuccess, onFail)
 * }
 * </pre>
 */
public class ListenRequest implements RequestBase<ListenResponse>, ProtostuffSerializable {
    public static final byte TYPE = 0x03;

    @Tag(1) public byte n;
    @Tag(2) public byte w;
    @Tag(3) public long version;
    @Tag(4) public int timeout;
    @Tag(5) public ByteArrayList key;

    /**
     * Создание пустого запроса. Используется для последующей десериализации.
     */
    public ListenRequest() {
        n = 0;
        w = 0;
        version = 0;
        timeout = 0;
        key = new ByteArrayList();
    }

    /**
     * Создание запроса с указанием ключа и ожидаемой версии.
     * Параметры репликации и таймаут выставлены по умолчанию
     * @param key ключ, по которому будет происходить ожидание
     * @param version когда версия ключа станет больше указанной, ожидание завершится
     */
    public ListenRequest(ByteArrayList key, long version) {
        this.key = key;
        this.version = version;
        this.n = 3;
        this.w = 2;
        this.timeout = 30000;
    }

    /**
     * Установка параметров репликации.
     * @param n количество реплик
     * @param w количество ответов для подтверждения записи
     * @return этот объект
     */
    public ListenRequest replicas(byte n, byte w) {
        this.n = n;
        this.w = w;
        return this;
    }

    /**
     * Установка максимального времени ожидания, после которого
     * в ответ приходит ошибка {@link ErrorCodes#TIMEOUT}.
     * @param timeout время ожидания
     * @param unit единицы времени для параметра {@code timeout}
     * @return этот объект
     */
    public ListenRequest timeout(int timeout, TimeUnit unit) {
        this.timeout = (int) unit.toMillis(timeout);
        return this;
    }

    @Override
    public byte type() {
        return TYPE;
    }

    @Override
    public Supplier<ListenResponse> response() {
        return ListenResponse::new;
    }
}
