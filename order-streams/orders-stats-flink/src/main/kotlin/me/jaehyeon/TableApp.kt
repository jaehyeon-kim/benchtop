@file:Suppress("ktlint:standard:no-wildcard-imports")

package me.jaehyeon

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import me.jaehyeon.flink.processing.LateDataRouter
import me.jaehyeon.flink.processing.RecordMap
import me.jaehyeon.flink.watermark.RowWatermarkStrategy
import me.jaehyeon.flink.watermark.SupplierWatermarkStrategy
import me.jaehyeon.kafka.createOrdersSource
import me.jaehyeon.kafka.createSkippedSink
import me.jaehyeon.kafka.createTopicIfNotExists
import me.jaehyeon.kafka.getLatestSchema
import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.table.KafkaConnectorOptions
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.api.Expressions.*
import org.apache.flink.table.api.Expressions.lit
import org.apache.flink.table.api.FormatDescriptor
import org.apache.flink.table.api.Schema
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.TableDescriptor
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.util.OutputTag
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

object TableApp {
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
        val outputTopicName = "$inputTopicName-ktl-stats"
        val skippedTopicName = "$inputTopicName-ktl-skipped"
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
        val tEnv = StreamTableEnvironment.create(env)

        val inputAvroSchema = getLatestSchema(INPUT_SCHEMA_SUBJECT, registryUrl, registryConfig)
        val ordersGenericRecordSource =
            createOrdersSource(
                topic = inputTopicName,
                groupId = "$inputTopicName-flink-tl",
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

        // 4. Split late records from on-time ones
        val statsStreamOperator: SingleOutputStreamOperator<RecordMap> =
            recordMapStream
                .assignTimestampsAndWatermarks(SupplierWatermarkStrategy.strategy)
                .process(LateDataRouter(lateMapOutputTag, allowedLatenessMillis = 5000))
                .name("LateDataRouter")

        // 5. Create source table (statsTable)
        val statsStream: DataStream<RecordMap> = statsStreamOperator
        val rowStatsStream: DataStream<Row> =
            statsStream
                .map { recordMap ->
                    val orderId = recordMap["order_id"] as? String
                    val price = recordMap["price"] as? Double
                    val item = recordMap["item"] as? String
                    val supplier = recordMap["supplier"] as? String

                    val bidTimeString = recordMap["bid_time"] as? String
                    var bidTimeInstant: Instant? = null // Changed from bidTimeLong to bidTimeInstant

                    if (bidTimeString != null) {
                        try {
                            val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                            val localDateTime = LocalDateTime.parse(bidTimeString, formatter)
                            // Convert to Instant
                            bidTimeInstant =
                                localDateTime
                                    .atZone(ZoneId.systemDefault()) // Or ZoneOffset.UTC
                                    .toInstant()
                        } catch (e: DateTimeParseException) {
                            logger.error(e) { "Failed to parse bid_time string '$bidTimeString'. RecordMap: $recordMap" }
                        } catch (e: Exception) {
                            logger.error(e) { "Unexpected error parsing bid_time string '$bidTimeString'. RecordMap: $recordMap" }
                        }
                    } else {
                        logger.warn { "bid_time string is null in RecordMap: $recordMap" }
                    }

                    Row.of(
                        orderId,
                        bidTimeInstant,
                        price,
                        item,
                        supplier,
                    )
                }.returns(
                    RowTypeInfo(
                        arrayOf<TypeInformation<*>>(
                            TypeInformation.of(String::class.java),
                            TypeInformation.of(Instant::class.java), // bid_time (as Long milliseconds for TIMESTAMP_LTZ)
                            TypeInformation.of(Double::class.java),
                            TypeInformation.of(String::class.java),
                            TypeInformation.of(String::class.java),
                        ),
                        arrayOf("order_id", "bid_time", "price", "item", "supplier"),
                    ),
                ).assignTimestampsAndWatermarks(RowWatermarkStrategy.strategy)
                .name("MapToRowConverter")
        val tableSchema =
            Schema
                .newBuilder()
                .column("order_id", DataTypes.STRING())
                .column("bid_time", DataTypes.TIMESTAMP_LTZ(3)) // Event time attribute
                .column("price", DataTypes.DOUBLE())
                .column("item", DataTypes.STRING())
                .column("supplier", DataTypes.STRING())
                .watermark("bid_time", "SOURCE_WATERMARK()") // Use watermarks from DataStream
                .build()
        tEnv.createTemporaryView("orders", rowStatsStream, tableSchema)

        val statsTable: Table =
            tEnv
                .from("orders")
                .window(Tumble.over(lit(5).seconds()).on(col("bid_time")).`as`("w"))
                .groupBy(col("supplier"), col("w"))
                .select(
                    col("supplier"),
                    col("w").start().`as`("window_start"),
                    col("w").end().`as`("window_end"),
                    col("price").sum().round(2).`as`("total_price"),
                    col("order_id").count().`as`("count"),
                )

        // 6. Create sink table
        val sinkSchema =
            Schema
                .newBuilder()
                .column("supplier", DataTypes.STRING())
                .column("window_start", DataTypes.TIMESTAMP(3))
                .column("window_end", DataTypes.TIMESTAMP(3))
                .column("total_price", DataTypes.DOUBLE())
                .column("count", DataTypes.BIGINT())
                .build()
        val kafkaSinkDescriptor: TableDescriptor =
            TableDescriptor
                .forConnector("kafka")
                .schema(sinkSchema) // Set the schema for the sink
                .option(KafkaConnectorOptions.TOPIC, listOf(outputTopicName))
                .option(KafkaConnectorOptions.PROPS_BOOTSTRAP_SERVERS, bootstrapAddress)
                .format(
                    FormatDescriptor
                        .forFormat("avro-confluent")
                        .option("url", registryUrl)
                        .option("basic-auth.credentials-source", "USER_INFO")
                        .option("basic-auth.user-info", "admin:admin")
                        .option("subject", "$outputTopicName-value")
                        .build(),
                ).build()

        // 7. Handle late data as a pair of key and value
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
        val skippedSink =
            createSkippedSink(
                topic = skippedTopicName,
                bootstrapAddress = bootstrapAddress,
            )

        if (!toSkipPrint) {
            tEnv
                .toDataStream(statsTable)
                .print()
                .name("SupplierStatsPrint")
            lateKeyPairStream
                .map { it.second }
                .print()
                .name("LateDataPrint")
        }

        statsTable.executeInsert(kafkaSinkDescriptor)
        lateKeyPairStream.sinkTo(skippedSink).name("LateDataSink")
        env.execute("SupplierStats")
    }
}
