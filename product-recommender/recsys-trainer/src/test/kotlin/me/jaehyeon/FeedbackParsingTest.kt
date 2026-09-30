package me.jaehyeon

import me.jaehyeon.infrastructure.file.FileSourceFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class FeedbackParsingTest {
    private val features = (1..15).map { (it / 10.0).toString() }

    @Test
    fun `a training log line becomes a feedback event`() {
        val line = (listOf("42") + features + listOf("7", "1")).joinToString(",")

        val feedback = FileSourceFactory.parseCsvLine(line)!!

        assertEquals("42", feedback.eventId)
        assertEquals("7", feedback.productId)
        assertEquals(1, feedback.reward)
        assertEquals(15, feedback.contextVector.size)
        assertEquals(0.1, feedback.contextVector.first())
    }

    @Test
    fun `the header and short lines are skipped`() {
        assertNull(FileSourceFactory.parseCsvLine((listOf("event_id") + features + listOf("product_id", "response")).joinToString(",")))
        assertNull(FileSourceFactory.parseCsvLine("1,2,3"))
    }

    @Test
    fun `a malformed number is skipped rather than failing the job`() {
        val line = (listOf("42") + features.drop(1) + listOf("abc", "7", "1")).joinToString(",")
        assertNull(FileSourceFactory.parseCsvLine(line))
    }
}
