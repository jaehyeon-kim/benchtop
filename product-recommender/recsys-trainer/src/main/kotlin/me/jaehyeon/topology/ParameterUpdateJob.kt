package me.jaehyeon.topology

import me.jaehyeon.config.AppConfig
import me.jaehyeon.domain.model.Feedback
import me.jaehyeon.infrastructure.file.FileSourceFactory
import me.jaehyeon.infrastructure.kafka.KafkaSourceFactory
import me.jaehyeon.infrastructure.redis.RedisSink
import me.jaehyeon.topology.processing.LinUCBUpdater
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.SplitEnumerator
import org.apache.flink.connector.base.source.hybrid.HybridSource
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * Defines the main Flink Job Topology.
 *
 * Workflow:
 * 1. Hybrid Source: Reads historical CSV first (Bootstrap), then switches to live Kafka stream.
 *    Both sources output normalized [Feedback] objects.
 * 2. KeyBy: Partitions the stream by Product ID.
 * 3. Process: [LinUCBUpdater] updates the Matrix A and Vector b statefully.
 * 4. Sink: Pushes updated model parameters to Redis.
 */
class ParameterUpdateJob(
    private val config: AppConfig,
) {
    fun createTopology(env: StreamExecutionEnvironment) {
        // Define File Source (Bounded / History)
        val fileFactory = FileSourceFactory(config)
        val fileSource = fileFactory.createSource()

        // Define Kafka Source (Unbounded / Live)
        val kafkaFactory = KafkaSourceFactory(config)
        val kafkaSource =
            kafkaFactory.createSource(
                topic = config.feedbackTopic,
                startingOffsets = OffsetsInitializer.earliest(),
            )

        // Define Hybrid Source
        // Since both sources produce 'Feedback', we combine them seamlessly.
        // We specify SplitEnumerator<*,*> because File and Kafka use different enumerator implementations.
        val hybridSource =
            HybridSource
                .builder<Feedback, SplitEnumerator<*, *>>(fileSource)
                .addSource<SplitEnumerator<*, *>, KafkaSource<Feedback>>(kafkaSource)
                .build()

        // Create Stream & Map Data
        // We use 'noWatermarks' because our update logic relies on processing order/time,
        // effectively treating every update as immediate.
        val feedbackStream =
            env.fromSource(
                hybridSource,
                WatermarkStrategy.noWatermarks(),
                "Hybrid-Source-Stream",
                TypeInformation.of(Feedback::class.java),
            )

        // Process (Stateful Training)
        val updateStream =
            feedbackStream
                .keyBy { it.productId }
                .process(LinUCBUpdater())
                .name("LinUCB-Updater")

        // Sink (Parameter Updates)
        updateStream
            .sinkTo(RedisSink(config))
            .name("Redis-Sink")
    }
}
