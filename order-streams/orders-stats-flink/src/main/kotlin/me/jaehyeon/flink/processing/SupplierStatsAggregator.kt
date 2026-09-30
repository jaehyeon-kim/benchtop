package me.jaehyeon.flink.processing

import org.apache.flink.api.common.functions.AggregateFunction

typealias RecordMap = Map<String, Any?>

data class SupplierStatsAccumulator(
    var totalPrice: Double = 0.0,
    var count: Long = 0L,
)

class SupplierStatsAggregator : AggregateFunction<RecordMap, SupplierStatsAccumulator, SupplierStatsAccumulator> {
    override fun createAccumulator(): SupplierStatsAccumulator = SupplierStatsAccumulator()

    override fun add(
        value: RecordMap,
        accumulator: SupplierStatsAccumulator,
    ): SupplierStatsAccumulator =
        SupplierStatsAccumulator(
            accumulator.totalPrice + value["price"] as Double,
            accumulator.count + 1,
        )

    override fun getResult(accumulator: SupplierStatsAccumulator): SupplierStatsAccumulator = accumulator

    override fun merge(
        a: SupplierStatsAccumulator,
        b: SupplierStatsAccumulator,
    ): SupplierStatsAccumulator =
        SupplierStatsAccumulator(
            totalPrice = a.totalPrice + b.totalPrice,
            count = a.count + b.count,
        )
}
