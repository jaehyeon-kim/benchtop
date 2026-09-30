package me.jaehyeon

import me.jaehyeon.avro.SupplierStats
import me.jaehyeon.flink.processing.LateDataRouter
import me.jaehyeon.flink.processing.RecordMap
import me.jaehyeon.flink.processing.SupplierStatsAccumulator
import me.jaehyeon.flink.processing.SupplierStatsAggregator
import me.jaehyeon.flink.processing.SupplierStatsFunction
import org.apache.flink.api.common.functions.util.ListCollector
import org.apache.flink.api.common.typeinfo.TypeHint
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses
import org.apache.flink.util.OutputTag
import kotlin.test.Test
import kotlin.test.assertEquals

class FlinkFunctionsTest {
    private fun order(price: Double): RecordMap = mapOf("order_id" to "o", "price" to price, "supplier" to "Alice")

    @Test
    fun `the aggregator sums prices and counts orders`() {
        val aggregator = SupplierStatsAggregator()
        var acc = aggregator.createAccumulator()
        acc = aggregator.add(order(10.0), acc)
        acc = aggregator.add(order(5.5), acc)
        assertEquals(SupplierStatsAccumulator(15.5, 2), aggregator.getResult(acc))
        assertEquals(
            SupplierStatsAccumulator(16.5, 3),
            aggregator.merge(acc, SupplierStatsAccumulator(1.0, 1)),
        )
    }

    @Test
    fun `the window function writes one stats record per supplier window`() {
        val out = mutableListOf<SupplierStats>()
        SupplierStatsFunction().apply("Alice", TimeWindow(0, 5000), listOf(SupplierStatsAccumulator(15.555, 2)), ListCollector(out))
        val stats = out.single()
        assertEquals("Alice", stats.supplier)
        assertEquals(15.56, stats.totalPrice)
        assertEquals(2L, stats.count)
    }

    @Test
    fun `the router sends records older than the watermark minus the allowed lateness to the side output`() {
        val lateTag = OutputTag("late", TypeInformation.of(object : TypeHint<RecordMap>() {}))
        val harness = ProcessFunctionTestHarnesses.forProcessFunction(LateDataRouter(lateTag, allowedLatenessMillis = 5000))

        harness.processWatermark(20_000)
        harness.processElement(order(1.0), 16_000) // within 5 seconds of the watermark
        harness.processElement(order(2.0), 14_000) // more than 5 seconds behind

        assertEquals(listOf(1.0), harness.extractOutputValues().map { it["price"] })
        assertEquals(listOf(2.0), harness.getSideOutput(lateTag).map { it.value["price"] })
    }
}
