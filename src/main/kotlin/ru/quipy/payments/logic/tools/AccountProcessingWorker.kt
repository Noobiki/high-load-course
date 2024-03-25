package ru.quipy.payments.logic.tools

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.launch
import okhttp3.Call
import okhttp3.Callback
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.ExternalServiceProperties
import ru.quipy.payments.logic.ExternalSysResponse
import ru.quipy.payments.logic.PaymentAggregateState
import ru.quipy.payments.logic.PaymentExternalServiceImpl
import ru.quipy.payments.logic.logProcessing
import ru.quipy.payments.logic.logSubmission
import ru.quipy.payments.logic.now
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import kotlin.math.min


class AccountProcessingWorker(
    private val client: OkHttpClient,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    val accountProcessingInfo: AccountProcessingInfo
) {
    @OptIn(ExperimentalCoroutinesApi::class)
    private val requestScope =
        CoroutineScope(Dispatchers.IO.limitedParallelism(accountProcessingInfo.maxParallelRequests))

    @OptIn(ExperimentalCoroutinesApi::class)
    private val queueProcessingScope =
        CoroutineScope(Dispatchers.IO.limitedParallelism(accountProcessingInfo.maxParallelRequests))

    private fun sendRequest(
        transactionId: UUID,
        paymentId: UUID
    ) = requestScope.launch {
        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${accountProcessingInfo.serviceName}&accountName=${accountProcessingInfo.accountName}&transactionId=$transactionId")
            post(PaymentExternalServiceImpl.emptyBody)
        }.build()

        try {
            client.newCall(request).enqueue(object : Callback {
                override fun onFailure(call: Call, e: IOException) {
                    PaymentExternalServiceImpl.logger.error("[${accountProcessingInfo.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, error message: ${e.message}")
                }

                override fun onResponse(call: Call, response: Response) {
                    val body = try {
                        PaymentExternalServiceImpl.mapper.readValue(
                            response.body?.string(),
                            ExternalSysResponse::class.java
                        )
                    } catch (e: Exception) {
                        PaymentExternalServiceImpl.logger.error("[${accountProcessingInfo.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(false, e.message)
                    }

                    PaymentExternalServiceImpl.logger.warn("[${accountProcessingInfo.accountName}] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }

            })
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    PaymentExternalServiceImpl.logger.error(
                        "[${accountProcessingInfo.accountName}] Payment failed for txId: $transactionId, payment: $paymentId",
                        e
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        } finally {
            accountProcessingInfo.requestCounter.releaseWindow()
        }
    }

    public fun enqueuePayment(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long
    ) = queueProcessingScope.launch {
        accountProcessingInfo.paymentQueue.enqueue(PaymentInfo(paymentId, amount, paymentStartedAt))
        val windowResult = accountProcessingInfo.requestCounter.putIntoWindow()
        if (windowResult is NonBlockingOngoingWindow.WindowResponse.Success) {
            while (!accountProcessingInfo.rateLimiter.tick()) {
                PaymentExternalServiceImpl.logger.warn("[${accountProcessingInfo.accountName}] Payment $paymentId waiting for tick. Already passed: ${now() - paymentStartedAt} ms")
                continue
            }
        }
        PaymentExternalServiceImpl.logger.warn("[${accountProcessingInfo.accountName}] Added payment $paymentId in queue. Current number ${accountProcessingInfo.paymentQueue.length()}. Already passed: ${now() - paymentStartedAt} ms")
    }

    private val processQueue = queueProcessingScope.launch {
        while (true) {
            val payment = accountProcessingInfo.paymentQueue.dequeue()
            if (payment != null) {
                PaymentExternalServiceImpl.logger.warn("[${accountProcessingInfo.accountName}] Submitting payment request for payment ${payment.id}. Already passed: ${now() - payment.startedAt} ms")

                val transactionId = UUID.randomUUID()
                PaymentExternalServiceImpl.logger.info("[${accountProcessingInfo.accountName}] Submit for ${payment.id} , txId: $transactionId")

                paymentESService.update(payment.id) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - payment.startedAt))
                }

                if (Duration.ofMillis(now() - payment.startedAt) + accountProcessingInfo.request95thPercentileProcessingTime > PaymentExternalServiceImpl.paymentOperationTimeout) {
                    accountProcessingInfo.requestCounter.releaseWindow()
                    paymentESService.update(payment.id) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                    continue
                }

                sendRequest(transactionId, payment.id)
            }
        }
    }
}

class AccountProcessingInfo(
    properties: ExternalServiceProperties
) {
    val serviceName = properties.serviceName
    val accountName = properties.accountName
    val maxParallelRequests = properties.parallelRequests
    val rateLimitPerSec = properties.rateLimitPerSec
    val paymentQueue: Queue<PaymentInfo> = FAAQueue()
    val request95thPercentileProcessingTime = properties.request95thPercentileProcessingTime
    val requestCounter = NonBlockingOngoingWindow(maxParallelRequests)
    val rateLimiter = RateLimiter(rateLimitPerSec)
    val speedPerMillisecond = min(
        maxParallelRequests.toDouble() / (request95thPercentileProcessingTime.toMillis()),
        rateLimitPerSec.toDouble() / 1000
    )
}

data class PaymentInfo(
    val id: UUID,
    val amount: Int,
    val startedAt: Long
)