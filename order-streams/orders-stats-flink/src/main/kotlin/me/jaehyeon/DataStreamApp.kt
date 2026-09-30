package me.jaehyeon

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import me.jaehyeon.avro.SupplierStats
import me.jaehyeon.flink.processing.RecordMap
import me.jaehyeon.flink.processing.SupplierStatsAggregator
import me.jaehyeon.flink.processing.SupplierStatsFunction
import me.jaehyeon.flink.watermark.SupplierWatermarkStrategy
import me.jaehyeon.kafka.createOrdersSource
import me.jaehyeon.kafka.createSkippedSink
import me.jaehyeon.kafka.createStatsSink
import me.jaehyeon.kafka.createTopicIfNotExists
import me.jaehyeon.kafka.getLatestSchema
import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.util.OutputTag
import java.time.Duration

object DataStreamApp {
    private val toSkipPrint = System.getenv("TO_SKIP_PRINT")?.toBoolean() ?: true
    private val bootstrapAddress = System.getenv("BOOTSTRAP") ?: "127.0.0.1:9092"
    private val inputTopicName = System.getenv("TOPIC") ?: "orders-avro"
    private val registryUrl = System.getenv("REGISTRY_URL") ?: "http://127.0.0.1:8081"
    private val registryConfig =
        mapOf(
            "basic.auth.credentials.source" to "USER_INFO",
            "basic.auth.user.info" to "admin:admin",
        )
    private const val INPUT_SCHEMA_SUBJECT = "orders-avro-value"
    private const val NUM_PARTITIONS = 3
    private const val REPLICATION_FACTOR: Short = 1 // odctl kafka-lite has one broker
    private val logger = KotlinLogging.logger {}

    // ObjectMapper for converting late data Map to JSON
    private val objectMapper: ObjectMapper by lazy {
        ObjectMapper().registerKotlinModule()
    }

    fun run() {
        // Create output topics if not existing
        val outputTopicName = "$inputTopicName-kds-stats"
        val skippedTopicName = "$inputTopicName-kds-skipped"
        listOf(outputTopicName, skippedTopicName).forEach { name ->
            createTopicIfNotExists(
                name,
                bootstrapAddress,
                NUM_PARTITIONS,
                REPLICATION_FACTOR,
            )
        }

        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = 3

        val inputAvroSchema = getLatestSchema(INPUT_SCHEMA_SUBJECT, registryUrl, registryConfig)
        val ordersGenericRecordSource =
            createOrdersSource(
                topic = inputTopicName,
                groupId = "$inputTopicName-flink-ds",
                bootstrapAddress = bootstrapAddress,
                registryUrl = registryUrl,
                registryConfig = registryConfig,
                schema = inputAvroSchema,
            )

        // 1. Stream of GenericRecords from Kafka
        val genericRecordStream: DataStream<GenericRecord> =
            env
                .fromSource(ordersGenericRecordSource, WatermarkStrategy.noWatermarks(), "KafkaGenericRecordSource")

        // 2. Convert GenericRecord to Map<String, Any?> (RecordMap)
        val recordMapStream: DataStream<RecordMap> =
            genericRecordStream
                .map { genericRecord ->
                    val map = mutableMapOf<String, Any?>()
                    genericRecord.schema.fields.forEach { field ->
                        val value = genericRecord.get(field.name())
                        map[field.name()] = if (value is org.apache.avro.util.Utf8) value.toString() else value
                    }
                    map as RecordMap // Cast to type alias
                }.name("GenericRecordToMapConverter")
                .returns(TypeInformation.of(object : TypeHint<RecordMap>() {}))

        // 3. Define OutputTag for late data (now carrying RecordMap)
        val lateMapOutputTag =
            OutputTag(
                "late-order-records",
                TypeInformation.of(object : TypeHint<RecordMap>() {}),
            )

        // 4. Process the RecordMap stream
        val statsStreamOperator: SingleOutputStreamOperator<SupplierStats> =
            recordMapStream
                .assignTimestampsAndWatermarks(SupplierWatermarkStrategy.strategy)
                .keyBy { recordMap -> recordMap["supplier"].toString() }
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .allowedLateness(Duration.ofSeconds(5))
                .sideOutputLateData(lateMapOutputTag)
                .aggregate(SupplierStatsAggregator(), SupplierStatsFunction())
        val statsStream: DataStream<SupplierStats> = statsStreamOperator

        // 5. Handle late data as a pair of key and value
        val lateDataMapStream: DataStream<RecordMap> = statsStreamOperator.getSideOutput(lateMapOutputTag)
        val lateKeyPairStream: DataStream<Pair<String?, String>> =
            lateDataMapStream
                .map { recordMap ->
                    val mutableMap = recordMap.toMutableMap()
                    mutableMap["late"] = true
                    val orderId = mutableMap["order_id"] as? String
                    try {
                        val value = objectMapper.writeValueAsString(mutableMap)
                        Pair(orderId, value)
                    } catch (e: Exception) {
                        logger.error(e) { "Error serializing late RecordMap to JSON: $mutableMap" }
                        val errorJson = "{ \"error\": \"json_serialization_failed\", \"data_keys\": \"${
                            mutableMap.keys.joinToString(
                                ",",
                            )}\" }"
                        Pair(orderId, errorJson)
                    }
                }.returns(TypeInformation.of(object : TypeHint<Pair<String?, String>>() {}))

        if (!toSkipPrint) {
            statsStream
                .print()
                .name("SupplierStatsPrint")
            lateKeyPairStream
                .map { it.second }
                .print()
                .name("LateDataPrint")
        }

        val statsSink =
            createStatsSink(
                topic = outputTopicName,
                bootstrapAddress = bootstrapAddress,
                registryUrl = registryUrl,
                registryConfig = registryConfig,
                outputSubject = "$outputTopicName-value",
            )

        val skippedSink =
            createSkippedSink(
                topic = skippedTopicName,
                bootstrapAddress = bootstrapAddress,
            )

        statsStream.sinkTo(statsSink).name("SupplierStatsSink")
        lateKeyPairStream.sinkTo(skippedSink).name("LateDataSink")
        env.execute("SupplierStats")
    }
}
