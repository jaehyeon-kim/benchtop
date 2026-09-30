package me.jaehyeon.infrastructure.kafka

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import me.jaehyeon.config.AppConfig
import me.jaehyeon.domain.model.Feedback
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import java.util.Properties

/**
 * Factory for creating a KafkaSource that reads live feedback events.
 *
 * It handles the deserialization from Avro (GenericRecord) directly into the
 * [Feedback] domain object to ensure type consistency with the FileSource.
 */
class KafkaSourceFactory(
    private val config: AppConfig,
) {
    private val logger = LoggerFactory.getLogger(KafkaSourceFactory::class.java)
    private val feedbackSchemaString = """
        {
          "namespace": "me.jaehyeon",
          "type": "record",
          "name": "FeedbackEvent",
          "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "product_id", "type": "string"},
            {"name": "reward", "type": "int"},
            {"name": "context_vector", "type": {"type": "array", "items": "double"}},
            {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"}
          ]
        }
    """

    /**
     * Builds the KafkaSource.
     *
     * @param topic The Kafka topic to consume.
     * @param startingOffsets Offset strategy (Earliest/Latest/Timestamp).
     */
    fun createSource(
        topic: String,
        startingOffsets: OffsetsInitializer,
    ): KafkaSource<Feedback> {
        logger.info("Initializing Kafka Source for topic: '{}'", topic)
        logger.info("Using Schema Registry at: '{}'.", config.registryUrl)

        val schema =
            try {
                getLatestSchema(topic, config.registryUrl)
            } catch (e: Exception) {
                logger.warn("Failed to fetch schema from registry (Topic might be new). Using Fallback Schema. Error: ${e.message}")
                Schema.Parser().parse(feedbackSchemaString)
            }

        val avroDeserializer =
            ConfluentRegistryAvroDeserializationSchema.forGeneric(
                schema,
                config.registryUrl,
            )

        return KafkaSource
            .builder<Feedback>()
            .setBootstrapServers(config.bootstrapAddress)
            .setTopics(topic)
            .setGroupId("$topic-flink-group")
            .setStartingOffsets(startingOffsets)
            .setProperty("commit.offsets.on.checkpoint", "true")
            // Use custom deserializer to map GenericRecord -> Feedback
            .setDeserializer(
                FeedbackKafkaDeserializationSchema(avroDeserializer),
            ).setProperties(
                Properties().apply {
                    // Optimization: Do not wait too long to fetch data
                    put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "500")
                    put("partition.discovery.interval.ms", "10000")
                },
            ).build()
    }

    /**
     * Custom Deserializer adapter.
     * Wraps the Confluent Avro deserializer and maps the result to our POJO.
     */
    private class FeedbackKafkaDeserializationSchema(
        private val avroDeserializer: ConfluentRegistryAvroDeserializationSchema<GenericRecord>,
    ) : KafkaRecordDeserializationSchema<Feedback> {
        private val logger = LoggerFactory.getLogger(FeedbackKafkaDeserializationSchema::class.java)

        override fun deserialize(
            record: ConsumerRecord<ByteArray, ByteArray>,
            collector: Collector<Feedback>,
        ) {
            try {
                // Deserialize raw bytes to Avro GenericRecord
                val genericRecord = avroDeserializer.deserialize(record.value())

                // Avro arrays implement the List interface.
                val rawList = genericRecord.get("context_vector") as? List<*>
                if (rawList == null) {
                    logger.warn("context_vector is null or not a List. Record: $genericRecord")
                    return
                }

                // Map content safely
                val contextArr =
                    rawList.map {
                        // Avro numbers might come in as Float, Double, or Int
                        (it as Number).toDouble()
                    }

                // Emit domain object
                collector.collect(
                    Feedback(
                        eventId = genericRecord.get("event_id").toString(),
                        productId = genericRecord.get("product_id").toString(),
                        reward = genericRecord.get("reward") as Int,
                        contextVector = contextArr,
                        timestamp = genericRecord.get("timestamp") as Long,
                    ),
                )
            } catch (e: Exception) {
                logger.error("Error while parsing avro deserialization", e)
            }
        }

        override fun getProducedType(): TypeInformation<Feedback> = TypeInformation.of(Feedback::class.java)
    }

    /**
     * Fetches the latest Avro schema definition from the Registry.
     * This is useful if we need to inspect the schema before creating the source,
     * though Flink's deserializer usually handles this automatically.
     */
    fun getLatestSchema(
        topic: String,
        url: String,
    ): Schema {
        logger.info("Fetching latest schema for subject '$topic-value' from $url")
        val client = CachedSchemaRegistryClient(url, 100)
        val meta = client.getLatestSchemaMetadata("$topic-value")
        return Schema.Parser().parse(meta.schema)
    }
}
