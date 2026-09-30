package me.jaehyeon

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroSerializer
import me.jaehyeon.avro.Order
import org.apache.avro.generic.GenericRecord
import kotlin.test.Test
import kotlin.test.assertEquals

class OrderAvroSerdeTest {
    // "mock://" gives the Confluent serializers an in-memory schema registry.
    private val config = mapOf("schema.registry.url" to "mock://orders-avro-test")

    @Test
    fun `an order is written with the schema and read back as a generic record`() {
        val order =
            Order().apply {
                orderId = "order-1"
                bidTime = "2026-09-30 12:00:01"
                price = 12.5
                item = "Chair"
                supplier = "Alice"
            }
        val serializer = KafkaAvroSerializer().apply { configure(config, false) }
        val deserializer = KafkaAvroDeserializer().apply { configure(config, false) }

        val bytes = serializer.serialize("orders-avro", order)
        val record = deserializer.deserialize("orders-avro", bytes) as GenericRecord

        assertEquals("order-1", record["order_id"].toString())
        assertEquals("2026-09-30 12:00:01", record["bid_time"].toString())
        assertEquals(12.5, record["price"])
        assertEquals("Alice", record["supplier"].toString())
    }
}
