package me.jaehyeon.flink.processing

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.util.OutputTag

class LateDataRouter(
    private val lateOutputTag: OutputTag<RecordMap>,
    private val allowedLatenessMillis: Long,
) : ProcessFunction<RecordMap, RecordMap>() { // Extends the generic ProcessFunction from streaming API

    init {
        require(allowedLatenessMillis >= 0) {
            "allowedLatenessMillis cannot be negative. Got: $allowedLatenessMillis"
        }
    }

    @Throws(Exception::class)
    override fun processElement(
        value: RecordMap,
        ctx: ProcessFunction<RecordMap, RecordMap>.Context,
        out: Collector<RecordMap>,
    ) {
        val elementTimestamp: Long? = ctx.timestamp()
        val currentWatermark: Long = ctx.timerService().currentWatermark()

        // Element has no timestamp or watermark is still at its initial value
        if (elementTimestamp == null || currentWatermark == Long.MIN_VALUE) {
            out.collect(value)
            return
        }

        // Element has a timestamp and watermark is active.
        // An element is "too late" if its timestamp is older than current watermark - allowed lateness.
        if (elementTimestamp < currentWatermark - allowedLatenessMillis) {
            ctx.output(lateOutputTag, value)
        } else {
            out.collect(value)
        }
    }
}
