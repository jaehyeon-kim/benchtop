package me.jaehyeon.topology.processing

import me.jaehyeon.domain.model.Feedback
import me.jaehyeon.domain.model.LinUCBModel
import org.apache.commons.math3.linear.MatrixUtils
import org.apache.commons.math3.linear.RealMatrix
import org.apache.commons.math3.linear.RealVector
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * Stateful function implementing the Disjoint LinUCB training update.
 *
 * State:
 *  - Matrix A (Covariance)
 *  - Vector b (Reward weighted features)
 *
 * Logic:
 *  1. Restore A and b from RocksDB state (or initialize with Identity/Zeros).
 *  2. Update A += x * x^T
 *  3. Update b += reward * x
 *  4. Calculate Inverse(A)
 *  5. Emit updated parameters.
 */
class LinUCBUpdater : KeyedProcessFunction<String, Feedback, LinUCBModel>() {
    // State handle for Matrix A (stored as Array of DoubleArrays for serialization)
    private lateinit var stateA: ValueState<Array<DoubleArray>>

    // State handle for Vector b
    private lateinit var stateB: ValueState<DoubleArray>

    // State to track if a timer is already running to avoid duplicate registrations
    private lateinit var timerState: ValueState<Long>

    // Regularization parameter (Scale of the Identity Matrix for initialization)
    private val lambda = 1.0
    private val batchIntervalMs = 5000L

    override fun open(openContext: OpenContext) {
        stateA = runtimeContext.getState(ValueStateDescriptor("state-A", Array<DoubleArray>::class.java))
        stateB = runtimeContext.getState(ValueStateDescriptor("state-b", DoubleArray::class.java))
        timerState = runtimeContext.getState(ValueStateDescriptor("timer-state", Long::class.javaObjectType))
    }

    override fun processElement(
        event: Feedback,
        ctx: Context,
        out: Collector<LinUCBModel>,
    ) {
        val dim = event.contextVector.size

        // Load or Initialize State
        val currentAArr = stateA.value()
        var matrixA: RealMatrix

        if (currentAArr == null) {
            // Cold Start: A = Identity Matrix * lambda
            matrixA = MatrixUtils.createRealIdentityMatrix(dim).scalarMultiply(lambda)
        } else {
            matrixA = MatrixUtils.createRealMatrix(currentAArr)
        }

        val currentBArr = stateB.value()
        var vectorB: RealVector

        if (currentBArr == null) {
            // Cold Start: b = Zero Vector
            vectorB = MatrixUtils.createRealVector(DoubleArray(dim))
        } else {
            vectorB = MatrixUtils.createRealVector(currentBArr)
        }

        // Update Math
        // Convert input list to RealVector
        val xArray = event.contextVector.toDoubleArray()
        val x = MatrixUtils.createRealVector(xArray)

        // Update A: A_new = A_old + (x * x^T)
        // outerProduct creates the d*d matrix
        val outerProduct = x.outerProduct(x)
        matrixA = matrixA.add(outerProduct)

        // Update b: b_new = b_old + (reward * x)
        val rewardScaled = x.mapMultiply(event.reward.toDouble())
        vectorB = vectorB.add(rewardScaled)

        // Save State
        stateA.update(matrixA.data)
        stateB.update(vectorB.toArray())

        // Register Timer if not exists
        if (timerState.value() == null) {
            val timerTime = ctx.timerService().currentProcessingTime() + batchIntervalMs
            ctx.timerService().registerProcessingTimeTimer(timerTime)
            timerState.update(timerTime)
        }
    }

    override fun onTimer(
        timestamp: Long,
        ctx: OnTimerContext,
        out: Collector<LinUCBModel>,
    ) {
        // Load State
        val arrA = stateA.value() ?: return
        val arrB = stateB.value() ?: return

        val matrixA = MatrixUtils.createRealMatrix(arrA)

        // Inverse the matrix
        val solver =
            org.apache.commons.math3.linear
                .LUDecomposition(matrixA)
                .solver
        val matrixAInv = solver.inverse

        // Emit to Redis
        out.collect(
            LinUCBModel(
                productId = ctx.currentKey, // Get key from context
                inverseA = matrixAInv.data.map { it.toList() },
                b = arrB.toList(),
            ),
        )

        // Clear Timer State for the next update
        timerState.clear()
    }
}
