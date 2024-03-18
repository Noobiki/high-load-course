package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.delay
import okhttp3.Dispatcher
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties: List<ExternalServiceProperties>,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val accountProcessingInfos = properties
        .map {
            it.toAccountProcessingInfo()
        }

    private val waitDuration = Duration.ofSeconds(1)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newSingleThreadExecutor()

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        build()
    }

    private fun getAccountProcessingInfo(paymentStartedAt: Long): AccountProcessingInfo {
        while (true) {
            for (accountProcessingInfo in accountProcessingInfos) {
                if (accountProcessingInfo.queueLength.get() == 0 && accountProcessingInfo.rateLimiter.tick()) {
                    logger.warn("[${accountProcessingInfo.accountName}] ${accountProcessingInfo.queueLength.get()}. Already passed: ${now() - paymentStartedAt} ms")
                    accountProcessingInfo.requestCounter.putIntoWindow()
                    return accountProcessingInfo
                }
                val waitStartTime = now()
                if (
                    Duration
                        .ofMillis(
                            paymentOperationTimeout.toMillis() - (waitStartTime - paymentStartedAt)
                        ) - accountProcessingInfo.request95thPercentileProcessingTime >
                    Duration
                        .ofSeconds((accountProcessingInfo.speed() * accountProcessingInfo.queueLength.get()).toLong())
                ) {
                    val number = accountProcessingInfo.queueLength.getAndIncrement()
                    logger.warn("[${accountProcessingInfo.accountName}] Added payment for queue. Current number $number. Already passed: ${now() - paymentStartedAt} ms")
                } else {
                    continue
                }
                do {
                    if (accountProcessingInfo.rateLimiter.tick()) {
                        accountProcessingInfo.queueLength.decrementAndGet()
                        accountProcessingInfo.requestCounter.putIntoWindow()
                        return accountProcessingInfo
                    }
                } while (true)
            }
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val accountProcessingInfo = getAccountProcessingInfo(paymentStartedAt)
        logger.warn("[${accountProcessingInfo.accountName}] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[${accountProcessingInfo.accountName}] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        if (Duration.ofMillis(now() - paymentStartedAt) > paymentOperationTimeout) {
            accountProcessingInfo.requestCounter.releaseWindow()
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
            return
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${accountProcessingInfo.serviceName}&accountName=${accountProcessingInfo.accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        try {
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[${accountProcessingInfo.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[${accountProcessingInfo.accountName}] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error(
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
}

fun ExternalServiceProperties.toAccountProcessingInfo(): AccountProcessingInfo = AccountProcessingInfo(this)

data class AccountProcessingInfo(
    private val properties: ExternalServiceProperties
) {
    val serviceName = properties.serviceName
    val accountName = properties.accountName
    val maxParallelRequests = properties.parallelRequests
    val rateLimitPerSec = properties.rateLimitPerSec
    val request95thPercentileProcessingTime = properties.request95thPercentileProcessingTime
    val requestCounter = NonBlockingOngoingWindow(maxParallelRequests)
    val rateLimiter = RateLimiter(rateLimitPerSec)
    val queueLength = AtomicInteger(0)
}

fun AccountProcessingInfo.speed(): Double {
    return min(
        maxParallelRequests.toDouble() / (request95thPercentileProcessingTime.toMillis().toDouble() / 1000),
        rateLimitPerSec.toDouble()
    )
}

public fun now() = System.currentTimeMillis()