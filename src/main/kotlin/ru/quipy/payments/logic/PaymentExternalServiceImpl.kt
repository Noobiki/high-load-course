package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.tools.AccountProcessingInfo
import ru.quipy.payments.logic.tools.AccountProcessingWorker
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.UUID


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

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val accountProcessingWorkers = properties
        .map {
            it.toAccountProcessingWorker()
        }

    private val client = OkHttpClient()

    private fun pickAccountProcessingWorker(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long
    ) {
        for (accountProcessingWorker in accountProcessingWorkers) {
            if (
                (Duration
                    .ofMillis(
                        paymentOperationTimeout.toMillis() - (now() - paymentStartedAt)
                    ) - accountProcessingWorker.accountProcessingInfo.request95thPercentileProcessingTime).toMillis()
                * accountProcessingWorker.accountProcessingInfo.speedPerMillisecond >
                accountProcessingWorker.accountProcessingInfo.paymentQueue.length()
                - accountProcessingWorker.accountProcessingInfo.maxParallelRequests

            ) {
                accountProcessingWorker.enqueuePayment(paymentId, amount, paymentStartedAt)
            } else {
                continue
            }
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        logger.warn("Payment $paymentId started choosing account. Already passed: ${now() - paymentStartedAt} ms")
        pickAccountProcessingWorker(paymentId, amount, paymentStartedAt)
    }

    fun ExternalServiceProperties.toAccountProcessingWorker(): AccountProcessingWorker =
        AccountProcessingWorker(client, paymentESService, AccountProcessingInfo(this))
}

public fun now() = System.currentTimeMillis()