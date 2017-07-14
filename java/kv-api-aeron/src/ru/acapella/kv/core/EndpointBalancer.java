package ru.acapella.kv.core;

import ru.acapella.kv.core.data.Address;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Класс для балансировки нагрузки между указанными адресами.
 */
public class EndpointBalancer {
    private final List<Address> endpoints;
    private int lastEndpoint;

    /**
     * Создание балансировщика.
     * @param endpoints адреса для балансировки
     */
    public EndpointBalancer(List<Address> endpoints) {
        this.endpoints = endpoints;
        this.lastEndpoint = 0;
    }

    /**
     * Создание балансировщика.
     * @param endpoints адреса для балансировки
     */
    public EndpointBalancer(Address... endpoints) {
        this(Arrays.asList(endpoints));
    }

    /**
     * Создание балансировщика с единственным адресом.
     * @param endpoint адрес
     */
    public EndpointBalancer(Address endpoint) {
        this(Collections.singletonList(endpoint));
    }

    /**
     * @return следующий адрес из списка
     */
    public Address next() {
        Address res = endpoints.get(lastEndpoint);
        lastEndpoint = (lastEndpoint + 1) % endpoints.size();
        return res;
    }
}
