package ru.acapella.kv.core;

import java.util.ArrayList;

public class ResourceList extends ArrayList<Resource> implements Resource {

    public ResourceList(int initialCapacity) {
        super(initialCapacity);
    }

    public ResourceList() {
    }

    @Override
    public void close() {
        for (Resource d : this)
            d.close();
        clear();
    }
}
