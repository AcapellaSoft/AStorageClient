package ru.acapella.kv.core;

import ru.acapella.kv.core.data.Address;
import ru.acapella.kv.core.data.RequestBase;
import ru.acapella.kv.core.data.ResponseBase;
import ru.acapella.kv.core.handlers.ResponseHandler;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Клиент для {@link Context}.
 * Для чего стоит использовать:
 * <ul>
 *     <li>простейшая балансировка нагрузки между указанными адресами;</li>
 *     <li>отмена всех незавершённых запросов, отправленных через этот клиент;</li>
 *     <li>методы для упрощения отправки запросов.</li>
 * </ul>
 *
 */
public class ContextClient implements Resource {
    private final Context ctx;
    private final EndpointBalancer endpoints;
    private final ResourceList resources;

    /**
     * Создание клиента. Этот объект довольно легковесный,
     * его можно создавать на небольшое количество
     * логически связанных запросов. После завершения обработки или при таймауте
     * нужно вызвать метод {@link ContextClient#close()}, чтобы освободить ресурсы.
     * @param ctx контекст
     * @param endpoints объект для балансировки между адресами
     */
    public ContextClient(Context ctx, EndpointBalancer endpoints) {
        this.ctx = ctx;
        this.endpoints = endpoints;
        this.resources = new ResourceList();
    }

    /**
     * Создание клиента.
     * @param ctx контекст
     * @param endpoints адреса для балансировки нагрузки
     */
    public ContextClient(Context ctx, Address... endpoints) {
        this.ctx = ctx;
        this.endpoints = new EndpointBalancer(endpoints);
        this.resources = new ResourceList();
    }

    /**
     * Отправление запроса и ожидание ответа через {@link CompletableFuture}.
     * @see RequestBase#send(ContextClient)
     * @param request объект запроса
     * @param <Response> тип ответа
     * @param <Request> тип запроса
     * @return {@link CompletableFuture}, который завершится когда придёт ответ
     */
    public <Response extends ResponseBase, Request extends RequestBase<Response>>
    CompletableFuture<Response> request(Request request) {
        Address endpoint = endpoints.next();
        return ctx.sendRequest(endpoint, request, resources);
    }

    /**
     * Отправление запроса и ожидание ответа с помощью колбеков.
     * @see RequestBase#send(ContextClient, Consumer, Consumer)
     * @param request объект запроса
     * @param success вызовется при удачном завершении
     * @param fail вызовется при ошибке
     * @param <Response> тип ответа
     * @param <Request> тип запроса
     */
    public <Response extends ResponseBase, Request extends RequestBase<Response>>
    void request(Request request, Consumer<Response> success, Consumer<Integer> fail) {
        Address endpoint = endpoints.next();
        Supplier<Response> allocator = request.response();

        Resource[] r = new Resource[1];

        r[0] = ctx.sendRequest(endpoint, request, ResponseHandler.create((id, type, input) -> {
            resources.remove(r[0]);
            Response resp = allocator.get();
            resp.deserialize(id, type, input);
            success.accept(resp);

        }, (id, code) -> {
            resources.remove(r[0]);
            fail.accept(code);
        }));

        resources.add(r[0]);
    }

    /**
     * Установка таймера, после истечения которого будет вызван метод task.
     * При вызове метода {@link #close()} этот таймер будет автоматически отменён.
     * @param delay интервал ожидания
     * @param unit единицы изменения интервала
     * @param task метод, который будет вызван
     */
    public void setTimeout(long delay, TimeUnit unit, Runnable task) {
        Resource r = ctx.setTimeout(delay, unit, task);
        resources.add(r);
    }

    /**
     * Освобождение ресурсов.
     */
    @Override
    public void close() {
        resources.close();
    }
}
