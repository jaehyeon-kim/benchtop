package me.jaehyeon

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import me.jaehyeon.avro.SupplierStats
import me.jaehyeon.streams.extractor.BidTimeTimestampExtractor
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.TopologyTestDriver
import java.util.Properties
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class StreamsTopologyTest {
    private val registryConfig = mapOf("schema.registry.url" to "mock://orders-stats-streams-test")
    private val orderSchema =
        Schema.Parser().parse(
            """
            {"type": "record", "name": "Order", "namespace": "me.jaehyeon.avro", "fields": [
              {"name": "order_id", "type": "string"}, {"name": "bid_time", "type": "string"},
              {"name": "price", "type": "double"}, {"name": "item", "type": "string"},
              {"name": "supplier", "type": "string"}]}
            """.trimIndent(),
        )
    private val driver =
        TopologyTestDriver(
            StreamsApp.buildTopology("orders-avro", registryConfig),
            Properties().apply {
                put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-stats-streams-test")
                put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092")
                put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, BidTimeTimestampExtractor::class.java.name)
            },
        )
    private val orders =
        driver.createInputTopic(
            "orders-avro",
            Serdes.String().serializer(),
            GenericAvroSerde().apply { configure(registryConfig, false) }.serializer(),
        )
    private val stats =
        driver.createOutputTopic(
            "orders-avro-stats",
            Serdes.String().deserializer(),
            SpecificAvroSerde<SupplierStats>().apply { configure(registryConfig, false) }.deserializer(),
        )
    private val skipped =
        driver.createOutputTopic("orders-avro-skipped", Serdes.String().deserializer(), Serdes.String().deserializer())

    private fun order(
        id: String,
        bidTime: String,
        price: Double,
    ): GenericRecord =
        GenericData.Record(orderSchema).apply {
            put("order_id", id)
            put("bid_time", bidTime)
            put("price", price)
            put("item", "Chair")
            put("supplier", "Alice")
        }

    @AfterTest
    fun close() = driver.close()

    @Test
    fun `orders in one window are summed per supplier`() {
        orders.pipeInput("a", order("a", "2026-09-30 12:00:01", 10.0))
        orders.pipeInput("b", order("b", "2026-09-30 12:00:02", 5.0))

        val latest = stats.readValuesToList().last()
        assertEquals("Alice", latest.supplier)
        assertEquals(15.0, latest.totalPrice)
        assertEquals(2L, latest.count)
    }

    @Test
    fun `an order older than its window and grace period goes to the skipped topic`() {
        orders.pipeInput("a", order("a", "2026-09-30 12:00:01", 10.0))
        // Moves stream time past the first window's end (12:00:05) plus its grace (5 seconds).
        orders.pipeInput("b", order("b", "2026-09-30 12:00:20", 5.0))
        orders.pipeInput("late", order("late", "2026-09-30 12:00:02", 7.0))

        val late = skipped.readKeyValuesToList()
        assertEquals(listOf("late"), late.map { it.key })
        assertTrue("\"late\":true" in late.single().value)
        assertTrue(stats.readValuesToList().none { it.totalPrice == 17.0 })
    }
}
