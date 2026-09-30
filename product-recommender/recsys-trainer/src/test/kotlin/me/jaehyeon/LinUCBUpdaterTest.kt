package me.jaehyeon

import me.jaehyeon.domain.model.Feedback
import me.jaehyeon.domain.model.LinUCBModel
import me.jaehyeon.topology.processing.LinUCBUpdater
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class LinUCBUpdaterTest {
    private fun harness(): KeyedOneInputStreamOperatorTestHarness<String, Feedback, LinUCBModel> =
        KeyedOneInputStreamOperatorTestHarness(
            KeyedProcessOperator(LinUCBUpdater()),
            { it.productId },
            Types.STRING,
        ).also { it.open() }

    private fun feedback(
        product: String,
        reward: Int,
        x: List<Double>,
    ) = Feedback("e", product, reward, x, 0L)

    private fun emitted(h: KeyedOneInputStreamOperatorTestHarness<String, Feedback, LinUCBModel>): List<LinUCBModel> =
        h.extractOutputValues()

    @Test
    fun `nothing is emitted until the batch timer fires`() {
        val h = harness()
        h.processElement(feedback("1", 1, listOf(1.0, 0.0)), 0L)
        assertTrue(emitted(h).isEmpty())
        h.setProcessingTime(5_000L)
        assertEquals(1, emitted(h).size)
    }

    @Test
    fun `one click updates A and b as LinUCB does`() {
        val h = harness()
        h.processElement(feedback("1", 1, listOf(1.0, 0.0)), 0L)
        h.setProcessingTime(5_000L)

        val model = emitted(h).single()
        // A = I + x x^T = [[2, 0], [0, 1]], so A_inv = [[0.5, 0], [0, 1]]; b = reward * x = [1, 0]
        assertEquals("1", model.productId)
        assertEquals(listOf(listOf(0.5, 0.0), listOf(0.0, 1.0)), model.inverseA)
        assertEquals(listOf(1.0, 0.0), model.b)
    }

    @Test
    fun `no click adds to A but not to b`() {
        val h = harness()
        h.processElement(feedback("1", 0, listOf(0.0, 1.0)), 0L)
        h.setProcessingTime(5_000L)

        val model = emitted(h).single()
        assertEquals(listOf(listOf(1.0, 0.0), listOf(0.0, 0.5)), model.inverseA)
        assertEquals(listOf(0.0, 0.0), model.b)
    }

    @Test
    fun `each product keeps its own model`() {
        val h = harness()
        h.processElement(feedback("1", 1, listOf(1.0, 0.0)), 0L)
        h.processElement(feedback("2", 1, listOf(0.0, 1.0)), 0L)
        h.setProcessingTime(5_000L)

        val byProduct = emitted(h).associateBy { it.productId }
        assertEquals(listOf(1.0, 0.0), byProduct.getValue("1").b)
        assertEquals(listOf(0.0, 1.0), byProduct.getValue("2").b)
    }
}
