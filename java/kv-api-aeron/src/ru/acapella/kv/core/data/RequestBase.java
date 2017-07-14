package ru.acapella.kv.core.data;

import ru.acapella.kv.core.ContextClient;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Supplier;
import ru.acapella.kv.core.CodeException;

/**
 * Базовый класс для запросов.
 * @param <Response> Тип ответа на этот запрос.
 */
public interface RequestBase<Response extends ResponseBase> extends Serializable {
    /**
     * @return тип запроса для выбора обработчика, должен быть уникальным
     */
    byte type();

    /**
     * @return метод для создания ответа
     */
    Supplier<Response> response();

    /**
     * Метод для отправки запроса сразу же при создании.
     * Пример использования:
     * <pre>
     * {@code
     *  future = new GetRequest(key)
     *          .replicas(5, 3, 3)
     *          .send(client)
     * }
     * </pre>
     * Если запрос завершился с ошибкой, то в ответ придёт {@link CodeException}
     * @param client Клиент контекста, с помощью которого будет отправлен запрос
     * @return {@link CompletableFuture}, который завершится когда придёт ответ
     */
    default CompletableFuture<Response> send(ContextClient client) {
        return client.request(this);
    }

    /**
     * Метод для отправки запроса сразу же при создании.
     * Пример использования:
     * <pre>
     * {@code
     *  new GetRequest(key)
     *          .replicas(5, 3, 3)
     *          .send(client, onSuccess, onFail)
     * }
     * </pre>
     * Если запрос завершился с ошибкой, то в ответ придёт одна из констант {@link ErrorCodes}
     * @param client Клиент контекста, с помощью которого будет отправлен запрос
     * @param success Метод, который будет вызван, если запрос завершился успешно
     * @param fail Метод, который будет вызван, если запрос завершился с ошибкой
     */
    default void send(ContextClient client, Consumer<Response> success, Consumer<Integer> fail) {
        client.request(this, success, fail);
    }
}
