package ru.acapella.kv.paxos

import kotlinx.coroutines.experimental.future.await
import ru.acapella.kv.core.ContextClient
import ru.acapella.kv.core.data.ByteArray
import ru.acapella.kv.core.data.ByteArrayList
import ru.acapella.kv.core.data.Serializable
import ru.acapella.kv.paxos.data.GetRequest
import ru.acapella.kv.paxos.data.GetVersionRequest
import ru.acapella.kv.paxos.data.ListenRequest
import ru.acapella.kv.paxos.data.SetRequest
import java.util.concurrent.TimeUnit


fun <T: Serializable> valueOrNull(bytes: ByteArray, allocator: () -> T) =
        if (bytes.isEmpty) null
        else bytes.toObject(allocator())


suspend inline fun <reified T: Serializable> ContextClient.get(
        key: ByteArrayList,
        n: Byte = 3, r: Byte = 2, w: Byte = 2): SimpleEntry<T> {
    val resp = request(GetRequest(key)
            .replicas(n, r, w))
            .await()
    val value = valueOrNull(resp.value, T::class.java::newInstance)
    return SimpleEntry(this, T::class.java::newInstance, key, resp.version, value, n, r, w)
}


suspend fun ContextClient.getVersion(
        key: ByteArrayList,
        n: Byte = 3, r: Byte = 2, w: Byte = 2
): Long {
    val response = request(GetVersionRequest(key)
            .replicas(n, r, w))
            .await()
    return response.version
}


inline fun <reified T: Serializable> ContextClient.entry(key: ByteArrayList, n: Byte = 3, r: Byte = 2, w: Byte = 2) =
    SimpleEntry<T>(this, T::class.java::newInstance, key, 0, null, n, r, w)


class SimpleEntry<T: Serializable> (
        val client: ContextClient,
        val allocator: () -> T,
        val key: ByteArrayList,
        var version: Long,
        var value: T?,
        val n: Byte,
        val r: Byte,
        val w: Byte) {

    suspend fun set(newValue: T?, expire: Int = SetRequest.EXPIRE_NONE) {
        val resp = SetRequest(key, ByteArray.fromObject(newValue))
                .replicas(n, r, w)
                .expire(expire)
                .send(client)
                .await()

        value = newValue
        version = resp.version
    }

    suspend fun cas(newValue: T?, expire: Int = SetRequest.EXPIRE_NONE): Boolean {
        val resp = SetRequest(key, ByteArray.fromObject(newValue))
                .replicas(n, r, w)
                .expire(expire)
                .condVersion(version)
                .send(client)
                .await()

        if (resp.status) {
            value = newValue
            version = resp.version
        }
        return resp.status
    }

    suspend fun listen() {
        val resp = ListenRequest(key, version)
                .replicas(n, w)
                .send(client)
                .await()

        version = resp.version
        value = valueOrNull(resp.value, allocator)
    }

    suspend fun listen(forVersion: Long) {
        val resp = ListenRequest(key, forVersion)
                .replicas(n, w)
                .send(client)
                .await()

        version = resp.version
        value = valueOrNull(resp.value, allocator)
    }

    suspend fun listen(forVersion: Long, timeout: Int, unit: TimeUnit) {
        val resp = ListenRequest(key, forVersion)
                .replicas(n, w)
                .timeout(timeout, unit)
                .send(client)
                .await()

        version = resp.version
        value = valueOrNull(resp.value, allocator)
    }
}
