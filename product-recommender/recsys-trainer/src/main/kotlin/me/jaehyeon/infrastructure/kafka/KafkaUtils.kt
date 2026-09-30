package me.jaehyeon.infrastructure.kafka

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import me.jaehyeon.config.AppConfig
import org.apache.avro.Schema
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.slf4j.LoggerFactory
import java.util.Properties

object KafkaUtils {
    private val logger = LoggerFactory.getLogger(KafkaUtils::class.java)

    fun ensureTopicExists(
        config: AppConfig,
        partitions: Int = 3,
        replication: Short = System.getenv("TOPIC_REPLICATION")?.toShortOrNull() ?: 1,
    ) {
        val props = Properties()
        props[AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG] = config.bootstrapAddress

        try {
            AdminClient.create(props).use { client ->
                val topics = client.listTopics().names().get()
                if (topics.contains(config.feedbackTopic)) {
                    logger.info("Kafka topic '${config.feedbackTopic}' already exists.")
                    return
                }

                logger.info("Creating Kafka topic '${config.feedbackTopic}' with $partitions partitions...")
                val newTopic = NewTopic(config.feedbackTopic, partitions, replication)
                client.createTopics(listOf(newTopic)).all().get()
                logger.info("Successfully created topic '${config.feedbackTopic}'.")
            }
        } catch (e: Exception) {
            logger.error("Failed to ensure Kafka topic exists: ${e.message}", e)
            throw RuntimeException(e)
        }
    }
}
