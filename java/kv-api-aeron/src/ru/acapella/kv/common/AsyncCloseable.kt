package ru.acapella.kv.common

import kotlinx.coroutines.experimental.Deferred

interface AsyncCloseable {
    suspend fun close(ex: Throwable?): Unit
}

suspend fun <T: AsyncCloseable, R: Any> T.use(action: suspend (T) -> R): R {
    try {
        val res = action(this)
        this.close(null)
        return res
    } catch (ex: Throwable) {
        this.close(ex)
        throw ex
    }
}

suspend fun <T: AsyncCloseable, R: Any> Deferred<T>.use(action: suspend (T) -> R): R {
    return this.await().use(action)
}