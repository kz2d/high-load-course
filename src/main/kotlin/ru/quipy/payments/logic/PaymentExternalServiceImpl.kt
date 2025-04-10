package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resumeWithException

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private val parallelWindow = OngoingWindow(parallelRequests)
    private val slidingWindowRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))

    private val client = OkHttpClient.Builder().callTimeout(30_000, TimeUnit.MILLISECONDS)
        .protocols(listOf(Protocol.HTTP_1_1))
        .connectionPool(ConnectionPool(300, 10, TimeUnit.MINUTES))
        .dispatcher(Dispatcher().apply {
            maxRequests = 1000
            maxRequestsPerHost = 1000
        })
        .build()

    private val paymentScope = CoroutineScope(Dispatchers.Default)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
//        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()
//        logger.info("[$accountName] Submit for $paymentId, txId: $transactionId")

        paymentScope.launch {
            try {
                makePayment(paymentId, amount, paymentStartedAt, deadline, transactionId)
            } catch (e: Exception) {
                logger.error("[$accountName] Unexpected error processing payment $paymentId", e)
                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Unexpected error: ${e.message}")
                }
            }
        }
    }

    private suspend fun makePayment(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        transactionId: UUID
    ) {
        // Mark submission regardless of outcome
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        var wait_time: Long = 1500
        var retries = 3
        val speed_factor = 2
        while (retries > 0) {
            retries -= 1
            wait_time*=2

            if (!parallelWindow.tryAcquire(deadline - now() - requestAverageProcessingTime.toMillis()/ speed_factor)){
                logger.warn("too many, droped")

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Cannot acquire lock")
                }
                return
            }

            try {
                val response = withTimeoutOrNull(deadline - now()) {


                client.awaitResponse(request)
            }

if (response == null) {
    parallelWindow.release()
  continue
}
                    response.use { resp ->
                        val body = try {
                            resp.body?.string()?.let {
                                mapper.readValue(it, ExternalSysResponse::class.java)
                            } ?: throw IllegalStateException("Empty response body")
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${resp.code}, reason: ${resp.body?.string()}")
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }

                        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                        if (body.result) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }
                            parallelWindow.release()
                            return
                        }

                }
            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    }
                }
            }

            parallelWindow.release()

            delay(wait_time)
        }

        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = "Payment expired")
        }
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = properties.accountName
}

// Extension function to convert OkHttp call to suspend function
private suspend fun OkHttpClient.awaitResponse(request: Request) = suspendCancellableCoroutine { continuation ->
    val call = newCall(request)
    continuation.invokeOnCancellation { call.cancel() }
    call.enqueue(object : okhttp3.Callback {
        override fun onFailure(call: Call, e: IOException) {
            continuation.resumeWithException(e)
        }

        override fun onResponse(call: okhttp3.Call, response: okhttp3.Response) {
            continuation.resume(response) {  }
        }
    })
}

public fun now() = System.currentTimeMillis()