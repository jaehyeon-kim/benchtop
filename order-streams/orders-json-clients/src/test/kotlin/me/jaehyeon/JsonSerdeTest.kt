package me.jaehyeon

import me.jaehyeon.model.Order
import me.jaehyeon.serializer.JsonDeserializer
import me.jaehyeon.serializer.JsonSerializer
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class JsonSerdeTest {
    private val order = Order("order-1", "2026-09-30 12:00:01", 12.5, "Chair", "Alice")

    @Test
    fun `an order survives a round trip`() {
        val bytes = JsonSerializer<Order>().serialize("orders-json", order)
        assertEquals(order, JsonDeserializer(Order::class.java).deserialize("orders-json", bytes))
    }

    @Test
    fun `fields are written in snake case`() {
        val json = String(JsonSerializer<Order>().serialize("orders-json", order)!!)
        assertTrue("\"order_id\":\"order-1\"" in json)
        assertTrue("\"bid_time\":\"2026-09-30 12:00:01\"" in json)
    }

    @Test
    fun `null passes through both ways`() {
        assertNull(JsonSerializer<Order>().serialize("orders-json", null))
        assertNull(JsonDeserializer(Order::class.java).deserialize("orders-json", null))
    }
}
