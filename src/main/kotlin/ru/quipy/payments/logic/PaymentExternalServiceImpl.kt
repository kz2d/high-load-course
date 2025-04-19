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
import java.lang.reflect.Proxy
import java.net.InetSocketAddress
import java.net.SocketTimeoutException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.EventListener
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


    private val client = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(requestAverageProcessingTime + Duration.ofSeconds(2))
        .build()

   private val max_retries = 2

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
//        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()
//        logger.info("[$accountName] Submit for $paymentId, txId: $transactionId")

    }

    private fun makePayment(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        transactionId: UUID,
        retriest: Int
    ) {
        if (retriest == max_retries) {
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
            if (!parallelWindow.tryAcquire(deadline - now() - requestAverageProcessingTime.toMillis()*2)){
                logger.warn("too many, droped")

                paymentESService.update(paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = "Cannot acquire lock")
                }
                return
            }
        }
        if (retriest == 0) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Payment expired")
            }
            parallelWindow.release()
            return
        }

        val request = HttpRequest.newBuilder().
            uri(URI.create("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            ).POST(BodyPublishers.noBody())
            .version(HttpClient.Version.HTTP_2)
            .timeout(requestAverageProcessingTime + Duration.ofSeconds(2))
        .build()

            try {
                val resp = client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenAcceptAsync{ resp ->


                    val body = try {
                        resp.body()?.let {
                            mapper.readValue(it, ExternalSysResponse::class.java)
                        } ?: throw IllegalStateException("Empty response body")
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${resp.statusCode()}, reason: ${resp.body()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    if (body.result) {
                        parallelWindow.release()
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }

                    }else {
                        makePayment(paymentId, amount, paymentStartedAt, deadline, transactionId, retriest - 1)
                    }
                }


            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        makePayment(paymentId, amount, paymentStartedAt, deadline, transactionId, retriest - 1)
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    }

                    else -> {
                        makePayment(paymentId, amount, paymentStartedAt, deadline, transactionId, retriest - 1)
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    }
                }
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