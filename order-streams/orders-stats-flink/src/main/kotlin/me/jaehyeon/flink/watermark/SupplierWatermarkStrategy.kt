package me.jaehyeon.flink.watermark

import me.jaehyeon.flink.processing.RecordMap
import mu.KotlinLogging
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.format.DateTimeFormatter

private val logger = KotlinLogging.logger {}

object SupplierWatermarkStrategy {
    val strategy: WatermarkStrategy<RecordMap> =
        WatermarkStrategy
            .forBoundedOutOfOrderness<RecordMap>(Duration.ofSeconds(5)) // Operates on RecordMap
            .withTimestampAssigner { recordMap, _ ->
                val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                try {
                    val bidTimeString = recordMap["bid_time"]?.toString()
                    if (bidTimeString != null) {
                        val ldt = LocalDateTime.parse(bidTimeString, formatter)
                        ldt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()
                    } else {
                        logger.warn { "Missing 'bid_time' field in RecordMap: $recordMap. Using processing time." }
                        System.currentTimeMillis()
                    }
                } catch (e: Exception) {
                    logger.error(e) { "Error parsing 'bid_time' from RecordMap: $recordMap. Using processing time." }
                    System.currentTimeMillis()
                }
            }.withIdleness(Duration.ofSeconds(10)) // Optional: if partitions can be idle
}
