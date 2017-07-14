package ru.acapella.kv.transaction

import kotlinx.coroutines.experimental.future.await
import ru.acapella.kv.common.AsyncCloseable
import ru.acapella.kv.core.ContextClient
import ru.acapella.kv.core.data.ByteArray
import ru.acapella.kv.core.data.ByteArrayList
import ru.acapella.kv.core.data.Serializable
import ru.acapella.kv.transaction.data.*


suspend fun ContextClient.transaction(): Transaction {
    val resp = request(CreateTransactionRequest()).await()
    return Transaction(this, resp.index)
}


suspend fun ContextClient.transaction(index: Long): Transaction {
    return Transaction(this, index)
}


class Transaction(val client: ContextClient, val index: Long): AsyncCloseable {
    private var completed = false

    suspend override fun close(ex: Throwable?) {
        if (!completed) {
            if (ex == null) {
                commit()
            } else {
                rollback()
            }
        }
    }

    suspend fun keepAlive() {
        TransactKeepAliveRequest(index)
                .send(client)
                .await()
    }

    suspend fun commit() {
        CommitTransactionRequest(index)
                .send(client)
                .await()
        completed = true
    }

    suspend fun rollback() {
        RollbackTransactionRequest(index)
                .send(client)
                .await()
        completed = true
    }

    inline suspend fun <reified T: Serializable> get(key: ByteArrayList, n: Byte, r: Byte, w: Byte): TransactionEntry<T> {
        val resp = TransactGetRequest(index, key)
                .replicas(n, r, w)
                .send(client)
                .await()

        val value =
                if (resp.value.isEmpty) null
                else resp.value.toObject(T::class.java.newInstance())

        return TransactionEntry(client, index, key, resp.version, value, n, r, w)
    }

    fun <T: Serializable> entry(key: ByteArrayList, n: Byte, r: Byte, w: Byte) =
            TransactionEntry<T>(client, index, key, 0, null, n, r, w)
}


fun Transaction.withNrw(n: Byte, r: Byte, w: Byte) =
        TransactionNrw(this, n, r, w)


class TransactionNrw(
        val transaction: Transaction,
        val n: Byte,
        val r: Byte,
        val w: Byte
): AsyncCloseable {
    suspend override fun close(ex: Throwable?) = transaction.close(ex)
    suspend fun keepAlive() = transaction.keepAlive()
    suspend fun commit() = transaction.commit()
    suspend fun rollback() = transaction.rollback()

    inline suspend fun <reified T: Serializable> get(key: ByteArrayList) =
            transaction.get<T>(key, n, r, w)

    fun <T: Serializable> entry(key: ByteArrayList) =
            transaction.entry<T>(key, n, r, w)
}


class TransactionEntry<T: Serializable>(
        val client: ContextClient,
        val index: Long,
        val key: ByteArrayList,
        var version: Long,
        var value: T?,
        val n: Byte,
        val r: Byte,
        val w: Byte
) {
    suspend fun set(newValue: T?) {
        val resp = TransactSetRequest(index, key, ByteArray.fromObject(newValue))
                .replicas(n, r, w)
                .send(client)
                .await()

        value = newValue
        version = resp.version
    }

    suspend fun cas(newValue: T?) {
        val resp = TransactSetRequest(index, key, ByteArray.fromObject(newValue))
                .replicas(n, r, w)
                .condVersion(version)
                .send(client)
                .await()

        value = newValue
        version = resp.version
    }
}