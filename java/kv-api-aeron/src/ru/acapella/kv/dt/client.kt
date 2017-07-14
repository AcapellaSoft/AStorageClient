package ru.acapella.kv.dt

import kotlinx.coroutines.experimental.future.await
import ru.acapella.kv.core.ContextClient
import ru.acapella.kv.core.data.ByteArray
import ru.acapella.kv.core.data.ByteArrayList
import ru.acapella.kv.core.data.Serializable
import ru.acapella.kv.dt.data.*
import ru.acapella.kv.paxos.valueOrNull
import ru.acapella.kv.transaction.Transaction


fun ContextClient.tree(name: ByteArrayList, n: Byte, r: Byte, w: Byte) =
        DtTree(this, name, n, r, w)


class DtTree(
        val client: ContextClient,
        val name: ByteArrayList,
        val n: Byte,
        val r: Byte,
        val w: Byte
) {
    inline suspend fun <reified T: Serializable> get(key: ByteArrayList, tr: Transaction? = null): DtCursor<T> {
        val resp = DtGetRequest(name, key)
                .transaction(tr?.index ?: 0)
                .replicas(n, r, w)
                .send(client)
                .await()

        val value = valueOrNull(resp.value, T::class.java::newInstance)
        return DtCursor(this, T::class.java::newInstance, key, tr, value, ByteArray.EMPTY.copy())
    }

    inline suspend fun <reified T: Serializable> find(key: ByteArrayList, tr: Transaction? = null): DtCursor<T> {
        val resp = DtFindRequest(name, key)
                .transaction(tr?.index ?: 0)
                .replicas(n, r, w)
                .send(client)
                .await()

        val value = valueOrNull(resp.value, T::class.java::newInstance)
        return DtCursor(this, T::class.java::newInstance, key, tr, value, resp.node)
    }

    inline suspend fun <reified T: Serializable> cursor(key: ByteArrayList, tr: Transaction? = null): DtCursor<T> {
        return DtCursor(this, T::class.java::newInstance, key, tr, null, ByteArray.EMPTY.copy())
    }

    inline suspend fun <reified T: Serializable> range(
            firstKey: ByteArrayList? = ByteArrayList(),
            lastKey: ByteArrayList? = ByteArrayList(),
            limit: Int = 0,
            tr: Transaction? = null
    ): List<DtCursor<T>> {
        val resp = DtRangeRequest(name)
                .from(firstKey)
                .to(lastKey)
                .limit(limit)
                .transaction(tr?.index ?: 0)
                .replicas(n, r, w)
                .send(client)
                .await()

        return resp.data.map { e ->
            val value = valueOrNull(e.value(), T::class.java::newInstance)
            DtCursor(this, T::class.java::newInstance, e.key(), tr, value, ByteArray.EMPTY.copy())
        }
    }
}


class DtCursor<T: Serializable>(
        val tree: DtTree,
        val allocator: () -> T,
        key: ByteArrayList,
        val tr: Transaction?,
        value: T?,
        node: ByteArray
) {
    var key = key
        private set

    var value = value
        private set

    var node = node
        private set

    suspend fun set(newValue: T?) {
        val resp = DtSetRequest(tree.name, key, ByteArray.fromObject(newValue))
                .transaction(tr?.index ?: 0)
                .replicas(tree.n, tree.r, tree.w)
                .send(tree.client)
                .await()

        this.value = newValue
        this.node = resp.node
    }

    suspend fun next(): Boolean {
        val resp = DtNextRequest(tree.name, key)
                .node(node)
                .transaction(tr?.index ?: 0)
                .replicas(tree.n, tree.r, tree.w)
                .send(tree.client)
                .await()

        if (resp.key.isEmpty()) return false

        key = resp.key
        value = allocator().apply { deserialize(resp.value) }
        node = resp.node
        return true
    }

    suspend fun prev(): Boolean {
        val resp = DtPrevRequest(tree.name, key)
                .node(node)
                .transaction(tr?.index ?: 0)
                .replicas(tree.n, tree.r, tree.w)
                .send(tree.client)
                .await()

        if (resp.key.isEmpty()) return false

        key = resp.key
        value = allocator().apply { deserialize(resp.value) }
        node = resp.node
        return true
    }
}