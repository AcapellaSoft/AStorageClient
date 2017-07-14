package ru.acapella.kv.common

import kotlinx.coroutines.experimental.async
import ru.acapella.common.ILogger
import ru.acapella.kv.core.CodeException
import ru.acapella.kv.core.Context
import ru.acapella.kv.core.data.Address
import ru.acapella.kv.core.data.RequestBase
import ru.acapella.kv.core.data.ResponseBase
import ru.acapella.kv.core.handlers.ResponseHandler
import java.io.Closeable
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import kotlin.coroutines.experimental.CoroutineContext
import kotlin.coroutines.experimental.suspendCoroutine

suspend fun Context.sleep(delay: Long, unit: TimeUnit = TimeUnit.MILLISECONDS) = suspendCoroutine<Unit> { c ->
    setTimeout(delay, unit) { c.resume(Unit) }
}

fun asyncLoop(context: CoroutineContext, exceptionLogger: ILogger? = null, block: suspend () -> Unit): Closeable {
    var running = true
    async(context) {
        while (running) {
            try {
                block()
            } catch (ex: Throwable) {
                exceptionLogger?.error(ex)
            }
        }
    }
    return Closeable { running = false }
}


suspend fun <T: RequestBase<R>, R: ResponseBase>
        Context.sendRequest(to: Address, msg: T, timeout: Long, unit: TimeUnit) = suspendCoroutine<R> { c ->
    val responseAllocator = msg.response()

    var timer: Closeable? = null

    val r = sendRequest(to, msg, ResponseHandler.create({ id, type, input ->
        val resp = responseAllocator.get()
        resp.deserialize(id, type, input)
        timer?.close()
        c.resume(resp)
    }) { _, code ->
        timer?.close()
        c.resumeWithException(CodeException(code))
    })

    timer = setTimeout(timeout, unit) {
        r.close()
        c.resumeWithException(TimeoutException())
    }
}