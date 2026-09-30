package me.jaehyeon

import me.jaehyeon.kafka.verifyKafkaConnection
import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import java.time.Duration
import java.util.Properties
import kotlin.use

object ConsumerApp {
    private val bootstrapAddress = System.getenv("BOOTSTRAP") ?: "127.0.0.1:9092"
    private val topicName = System.getenv("TOPIC") ?: "orders-avro"
    private val registryUrl = System.getenv("REGISTRY_URL") ?: "http://127.0.0.1:8081"
    private val logger = KotlinLogging.logger { }
    private const val MAX_RETRIES = 3
    private const val ERROR_THRESHOLD = -1

    @Volatile
    private var keepConsuming = true

    fun run() {
        // Verify kafka connection
        verifyKafkaConnection(bootstrapAddress)

        val props =
            Properties().apply {
                put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
                put(ConsumerConfig.GROUP_ID_CONFIG, "$topicName-group")
                put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
                put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer")
                put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
                put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                put("specific.avro.reader", false)
                put("schema.registry.url", registryUrl)
                put("basic.auth.credentials.source", "USER_INFO")
                put("basic.auth.user.info", "admin:admin")
                put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000")
                put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000")
            }

        val consumer = KafkaConsumer<String, GenericRecord>(props)

        Runtime.getRuntime().addShutdownHook(
            Thread {
                logger.info { "Shutdown detected. Waking up Kafka consumer..." }
                keepConsuming = false
                consumer.wakeup()
            },
        )

        try {
            consumer.use { c ->
                c.subscribe(listOf(topicName))
                while (keepConsuming) {
                    val records = pollSafely(c)
                    for (record in records) {
                        processRecordWithRetry(record)
                    }
                    consumer.commitSync()
                }
            }
        } catch (e: Exception) {
            RuntimeException("Unrecoverable error while consuming record.", e)
        }
    }

    private fun pollSafely(consumer: KafkaConsumer<String, GenericRecord>) =
        runCatching { consumer.poll(Duration.ofMillis(1000)) }
            .getOrElse { e ->
                when (e) {
                    is WakeupException -> {
                        if (keepConsuming) throw e
                        logger.info { "ConsumerApp wakeup for shutdown." }
                        emptyList()
                    }
                    else -> {
                        logger.error(e) { "Unexpected error while polling records" }
                        emptyList()
                    }
                }
            }

    private fun processRecordWithRetry(record: ConsumerRecord<String, GenericRecord>) {
        var attempt = 0
        while (attempt < MAX_RETRIES) {
            try {
                attempt++
                if ((0..99).random() < ERROR_THRESHOLD) {
                    throw RuntimeException(
                        "Simulated error for ${record.value()} from partition ${record.partition()}, offset ${record.offset()}",
                    )
                }
                logger.info { "Received ${record.value()} from partition ${record.partition()}, offset ${record.offset()}" }
                return
            } catch (e: Exception) {
                logger.warn(e) { "Error processing record (attempt $attempt of $MAX_RETRIES)" }
                if (attempt == MAX_RETRIES) {
                    logger.error(e) { "Failed to process record after $MAX_RETRIES attempts, skipping..." }
                    return
                }
                Thread.sleep(500L * attempt.toLong()) // exponential backoff
            }
        }
    }
}
