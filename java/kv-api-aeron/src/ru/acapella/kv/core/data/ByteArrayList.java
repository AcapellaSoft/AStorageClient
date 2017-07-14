package ru.acapella.kv.core.data;

import io.protostuff.Tag;
import org.jetbrains.annotations.NotNull;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

public class ByteArrayList extends AbstractList<ByteArray> implements Comparable<ByteArrayList>, ProtostuffSerializable {
    public static final ByteArray KEYSPACE_KV = new ByteArray("KV".getBytes());
    public static final ByteArray KEYSPACE_DATA = new ByteArray("DATA".getBytes());
    public static final ByteArray KEYSPACE_TRANSACTION = new ByteArray("TR".getBytes());
    public static final ByteArray KEYSPACE_LOCKS = new ByteArray("LOCKS".getBytes());
    public static final ByteArray KEYSPACE_USER = new ByteArray("USER".getBytes());
    public static final ByteArray KEYSPACE_DT = new ByteArray("DT".getBytes());
    public static final ByteArray KEYSPACE_TREE = new ByteArray("TREE".getBytes());

    @Tag(1) private final ArrayList<ByteArray> inner;

    //
    // Пространства ключей в KV
    //
    // /user - пользовательское простанство ключей
    // /user/kv - данные сервиса KV
    // /user/kv/{key} - данные пользователя
    // /user/dt - данные сервиса DT
    // /user/dt/{treeName} - информация о дереве
    // /user/dt/{treeName}/{nodeId} - узлы дерева
    // /kv - служебные данные KV
    // /kv/tr - данные о транзакциях
    // /kv/tr/locks/{trIdx} - объекты, описывающие транзакции
    //

    public ByteArrayList(int initialCapacity) {
        inner = new ArrayList<>(initialCapacity);
    }

    public ByteArrayList() {
        inner = new ArrayList<>();
    }

    public ByteArrayList(@NotNull Collection<? extends ByteArray> c) {
        inner = new ArrayList<>(c);
    }

    public ByteArrayList(ByteArray... c) {
        this(Arrays.asList(c));
    }

    public ByteArrayList(String... c) {
        this(c.length);
        for (String s : c) add(new ByteArray(s));
    }

    @Override
    public void add(int index, ByteArray item) {
        inner.add(index, item);
    }

    @Override
    public int size() {
        return inner.size();
    }

    @Override
    public ByteArray get(int index) {
        return inner.get(index);
    }

    @Override
    public boolean isEmpty() {
        return inner.isEmpty();
    }

    public void addAll(ByteArrayList items) {
        inner.addAll(items.inner);
    }

    @NotNull
    @Override
    public Iterator<ByteArray> iterator() {
        return inner.iterator();
    }

    @Override
    public int compareTo(@NotNull ByteArrayList other) {
        int count = Math.min(this.size(), other.size());

        for (int i = 0; i < count; ++i) {
            int r = this.get(i).compareTo(other.get(i));
            if (r != 0) return r;
        }

        return this.size() - other.size();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < size(); ++i) {
            sb.append(get(i));
            if (i < size() - 1) sb.append(',');
        }
        sb.append(']');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ByteArrayList that = (ByteArrayList) o;

        return inner.equals(that.inner);
    }

    @Override
    public int hashCode() {
        return inner.hashCode();
    }
}
