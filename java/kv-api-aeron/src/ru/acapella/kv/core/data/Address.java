package ru.acapella.kv.core.data;

import io.protostuff.Tag;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import ru.acapella.kv.core.data.protostuff.ProtostuffSerializable;

import java.net.Inet4Address;
import java.util.Arrays;

public class Address implements Serializable, Comparable<Address>, ProtostuffSerializable {
    @Tag(1) public byte[] host;
    @Tag(2) public int port;

    public Address() {
        host = new byte[4];
        port = 0;
    }

    @SneakyThrows
    public Address(String host, int port) {
        this.host = Inet4Address.getByName(host).getAddress();
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Address address = (Address) o;

        return port == address.port && Arrays.equals(host, address.host);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(host);
        result = 31 * result + port;
        return result;
    }

    @Override
    public String toString() {
        return String.format("%d.%d.%d.%d:%d",
                Byte.toUnsignedInt(host[0]),
                Byte.toUnsignedInt(host[1]),
                Byte.toUnsignedInt(host[2]),
                Byte.toUnsignedInt(host[3]),
                port);
    }

    public String hostString() {
        return String.format("%d.%d.%d.%d",
                Byte.toUnsignedInt(host[0]),
                Byte.toUnsignedInt(host[1]),
                Byte.toUnsignedInt(host[2]),
                Byte.toUnsignedInt(host[3]));
    }

    @Override
    public int compareTo(@NotNull Address o) {
        int r = 0;
        for (int i = 0; i < 4; ++i) {
            if (r == 0) r = Byte.compare(this.host[i], o.host[i]);
        }
        if (r == 0) r = Integer.compare(this.port, o.port);
        return r;
    }

    public Address copy() {
        Address copy = new Address();
        copy.host = Arrays.copyOf(host, host.length);
        copy.port = port;
        return copy;
    }

    public static Address parse(String s) {
        String[] parts = s.split(":");
        return new Address(parts[0], Integer.parseInt(parts[1]));
    }
}
