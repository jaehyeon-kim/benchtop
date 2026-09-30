package me.jaehyeon

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import me.jaehyeon.avro.SupplierStats
import me.jaehyeon.kafka.createTopicIfNotExists
import me.jaehyeon.streams.extractor.BidTimeTimestampExtractor
import me.jaehyeon.streams.processor.LateRecordProcessor
import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Branched
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Named
import org.apache.kafka.streams.kstream.Produced
import org.apache.kafka.streams.kstream.TimeWindows
import org.apache.kafka.streams.kstream.Windowed
import org.apache.kafka.streams.processor.api.ProcessorSupplier
import java.time.Duration
import java.util.Properties

object StreamsApp {
    private val bootstrapAddress = System.getenv("BOOTSTRAP") ?: "127.0.0.1:9092"
    private val inputTopicName = System.getenv("TOPIC") ?: "orders-avro"
    private val registryUrl = System.getenv("REGISTRY_URL") ?: "http://127.0.0.1:8081"
    private val registryConfig =
        mapOf(
            "schema.registry.url" to registryUrl,
            "basic.auth.credentials.source" to "USER_INFO",
            "basic.auth.user.info" to "admin:admin",
        )
    private val windowSize = Duration.ofSeconds(5)
    private val gracePeriod = Duration.ofSeconds(5)
    private const val NUM_PARTITIONS = 3
    private const val REPLICATION_FACTOR: Short = 1 // odctl kafka-lite has one broker
    private val logger = KotlinLogging.logger {}

    // ObjectMapper for converting late source to JSON
    private val objectMapper: ObjectMapper by lazy {
        ObjectMapper().registerKotlinModule()
    }

    fun run() {
        // Create output topics if not existing
        val outputTopicName = "$inputTopicName-stats"
        val skippedTopicName = "$inputTopicName-skipped"
        listOf(outputTopicName, skippedTopicName).forEach { name ->
            createTopicIfNotExists(
                name,
                bootstrapAddress,
                NUM_PARTITIONS,
                REPLICATION_FACTOR,
            )
        }

        val props =
            Properties().apply {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "$outputTopicName-kafka-streams")
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress)
                put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String()::class.java.name)
                put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest")
                put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, BidTimeTimestampExtractor::class.java.name)
                put("schema.registry.url", registryUrl)
                put("basic.auth.credentials.source", "USER_INFO")
                put("basic.auth.user.info", "admin:admin")
            }

        val streams = KafkaStreams(buildTopology(inputTopicName, registryConfig), props)
        try {
            streams.start()
            logger.info { "Kafka Streams started successfully." }

            Runtime.getRuntime().addShutdownHook(
                Thread {
                    logger.info { "Shutting down Kafka Streams..." }
                    streams.close()
                },
            )
        } catch (e: Exception) {
            streams.close(Duration.ofSeconds(5))
            throw RuntimeException("Error while running Kafka Streams", e)
        }
    }

    // Builds the stream processing: late records go to the skipped topic as JSON, and the
    // rest are aggregated per supplier in 5-second windows into the stats topic.
    fun buildTopology(
        inputTopicName: String,
        registryConfig: Map<String, String>,
    ): Topology {
        val outputTopicName = "$inputTopicName-stats"
        val skippedTopicName = "$inputTopicName-skipped"
        val keySerde = Serdes.String()
        val valueSerde =
            GenericAvroSerde().apply {
                configure(registryConfig, false)
            }
        val supplierStatsSerde =
            SpecificAvroSerde<SupplierStats>().apply {
                configure(registryConfig, false)
            }

        val builder = StreamsBuilder()
        val source: KStream<String, GenericRecord> = builder.stream(inputTopicName, Consumed.with(keySerde, valueSerde))

        val taggedStream: KStream<String, Pair<GenericRecord, Boolean>> =
            source.process(
                ProcessorSupplier {
                    LateRecordProcessor(windowSize, gracePeriod)
                },
                Named.`as`("process-late-records"),
            )

        val branches: Map<String, KStream<String, Pair<GenericRecord, Boolean>>> =
            taggedStream
                .split(Named.`as`("branch-"))
                .branch({ _, value -> !value.second }, Branched.`as`("valid"))
                .branch({ _, value -> value.second }, Branched.`as`("late"))
                .noDefaultBranch()

        val validSource: KStream<String, GenericRecord> =
            branches["branch-valid"]!!
                .mapValues { _, pair -> pair.first }

        val lateSource: KStream<String, GenericRecord> =
            branches["branch-late"]!!
                .mapValues { _, pair -> pair.first }

        lateSource
            .mapValues { _, genericRecord ->
                val map = mutableMapOf<String, Any?>()
                genericRecord.schema.fields.forEach { field ->
                    val value = genericRecord.get(field.name())
                    map[field.name()] = if (value is org.apache.avro.util.Utf8) value.toString() else value
                }
                map["late"] = true
                map
            }.peek { key, mapValue ->
                logger.warn { "Potentially late record - key=$key, value=$mapValue" }
            }.mapValues { _, mapValue ->
                objectMapper.writeValueAsString(mapValue)
            }.to(skippedTopicName, Produced.with(keySerde, Serdes.String()))

        val aggregated: KTable<Windowed<String>, SupplierStats> =
            validSource
                .map { _, value ->
                    val supplier = value["supplier"]?.toString() ?: "UNKNOWN"
                    val price = value["price"] as? Double ?: 0.0
                    KeyValue(supplier, price)
                }.groupByKey(Grouped.with(keySerde, Serdes.Double()))
                .windowedBy(TimeWindows.ofSizeAndGrace(windowSize, gracePeriod))
                .aggregate(
                    {
                        SupplierStats
                            .newBuilder()
                            .setWindowStart("")
                            .setWindowEnd("")
                            .setSupplier("")
                            .setTotalPrice(0.0)
                            .setCount(0L)
                            .build()
                    },
                    { key, value, aggregate ->
                        val updated =
                            SupplierStats
                                .newBuilder(aggregate)
                                .setSupplier(key)
                                .setTotalPrice(aggregate.totalPrice + value)
                                .setCount(aggregate.count + 1)
                        updated.build()
                    },
                    Materialized.with(keySerde, supplierStatsSerde),
                )
        aggregated
            .toStream()
            .map { key, value ->
                val windowStart = key.window().startTime().toString()
                val windowEnd = key.window().endTime().toString()
                val updatedValue =
                    SupplierStats
                        .newBuilder(value)
                        .setWindowStart(windowStart)
                        .setWindowEnd(windowEnd)
                        .build()
                KeyValue(key.key(), updatedValue)
            }.peek { _, value ->
                logger.info { "Supplier Stats: $value" }
            }.to(outputTopicName, Produced.with(keySerde, supplierStatsSerde))

        return builder.build()
    }
}
