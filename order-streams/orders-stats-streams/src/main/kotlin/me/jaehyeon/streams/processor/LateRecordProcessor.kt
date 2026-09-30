package me.jaehyeon.streams.processor

import mu.KotlinLogging
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.processor.api.Processor
import org.apache.kafka.streams.processor.api.ProcessorContext
import org.apache.kafka.streams.processor.api.Record
import java.time.Duration

class LateRecordProcessor(
    private val windowSize: Duration,
    private val gracePeriod: Duration,
) : Processor<String, GenericRecord, String, Pair<GenericRecord, Boolean>> {
    private lateinit var context: ProcessorContext<String, Pair<GenericRecord, Boolean>>
    private val windowSizeMs = windowSize.toMillis()
    private val gracePeriodMs = gracePeriod.toMillis()
    private val logger = KotlinLogging.logger {}

    override fun init(context: ProcessorContext<String, Pair<GenericRecord, Boolean>>) {
        this.context = context
    }

    // The main processing method for the Processor API
    override fun process(record: Record<String, GenericRecord>) {
        val key = record.key()
        val value = record.value()

        // 1. Get the timestamp assigned to this specific record.
        //    This comes from your BidTimeTimestampExtractor.
        val recordTimestamp = record.timestamp()

        // Handle cases where timestamp extraction might have failed.
        // These records can't be placed in a window correctly anyway.
        if (recordTimestamp < 0) {
            logger.warn { "Record has invalid timestamp $recordTimestamp. Cannot determine window. Forwarding as NOT LATE. Key=$key" }
            // Explicitly forward the result using the context
            context.forward(Record(key, Pair(value, false), recordTimestamp))
            return
        }

        // 2. Determine the time window this record *should* belong to based on its timestamp.
        //    Calculate the END time of that window.
        //    Example: If window size is 5s and recordTimestamp is 12s, it belongs to
        //             window [10s, 15s). The windowEnd is 15s (15000ms).
        //             Calculation: ((12000 / 5000) + 1) * 5000 = (2 + 1) * 5000 = 15000
        val windowEnd = ((recordTimestamp / windowSizeMs) + 1) * windowSizeMs

        // 3. Calculate when this specific window "closes" for accepting late records.
        //    This is the window's end time plus the allowed grace period.
        //    Example: If windowEnd is 15s and gracePeriod is 0s, windowCloseTime is 15s.
        //             If windowEnd is 15s and gracePeriod is 2s, windowCloseTime is 17s.
        val windowCloseTime = windowEnd + gracePeriodMs

        // 4. Get the current "Stream Time".
        //    This represents the maximum record timestamp seen *so far* by this stream task.
        //    It indicates how far along the stream processing has progressed in event time.
        val streamTime = context.currentStreamTimeMs()

        // 5. THE CORE CHECK: Is the stream's progress (streamTime) already past
        //    the point where this record's window closed (windowCloseTime)?
        //    If yes, the record is considered "late" because the stream has moved on
        //    past the time it could have been included in its window (+ grace period).
        //    This mimics the logic the downstream aggregate operator uses to drop late records.
        val isLate = streamTime > windowCloseTime

        if (isLate) {
            logger.debug {
                "Tagging record as LATE: RecordTime=$recordTimestamp belongs to window ending at $windowEnd (closes at $windowCloseTime), but StreamTime is already $streamTime. Key=$key"
            }
        } else {
            logger.trace {
                "Tagging record as NOT LATE: RecordTime=$recordTimestamp, WindowCloseTime=$windowCloseTime, StreamTime=$streamTime. Key=$key"
            }
        }

        // 6. Explicitly forward the result (key, tagged value, timestamp) using the context
        // Ensure you preserve the original timestamp if needed downstream
        context.forward(Record(key, Pair(value, isLate), recordTimestamp))
    }

    override fun close() {
        // No resources to close
    }
}
