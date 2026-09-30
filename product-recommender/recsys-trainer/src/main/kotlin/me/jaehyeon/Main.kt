package me.jaehyeon

import me.jaehyeon.config.AppConfig
import me.jaehyeon.infrastructure.kafka.KafkaUtils
import me.jaehyeon.topology.ParameterUpdateJob
import org.apache.flink.configuration.ExternalizedCheckpointRetention
import org.apache.flink.core.execution.CheckpointingMode
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

/**
 * Main Entry Point for the Flink Application.
 *
 * This job reads feedback events from Kafka, updates LinUCB model parameters
 * (Matrix A and Vector b) statefully, and pushes the updated inverse matrices
 * to Redis for the serving layer.
 */
fun main() {
    val logger = LoggerFactory.getLogger("Main")
    val config = AppConfig()

    logger.info("Starting Online Recommender Training Job...")
    logger.info("Config: $config")

    try {
        // Infrastructure Check
        KafkaUtils.ensureTopicExists(config)

        // Setup Environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        configureEnvironment(env, config)
        env.parallelism = config.parallelism

        // Build Topology
        // Wires: Kafka Source -> KeyBy(Product) -> LinUCB Updater -> Redis Sink
        ParameterUpdateJob(config).createTopology(env)

        // Execute
        logger.info("Executing Flink Job: ${config.jobName}")
        env.execute(config.jobName)
    } catch (e: Exception) {
        logger.error("Critical error in Flink job execution", e)
        exitProcess(1)
    }
}

/**
 * Configures Checkpointing settings.
 * Checkpointing is critical for this job to ensure model state (matrices)
 * is preserved and recovered accurately in case of failure.
 */
fun configureEnvironment(
    env: StreamExecutionEnvironment,
    config: AppConfig,
) {
    // Enable Checkpointing every X ms.
    // EXACTLY_ONCE ensures we don't process a reward twice (double counting) upon recovery.
    env.enableCheckpointing(config.checkpointInterval, CheckpointingMode.EXACTLY_ONCE)

    val checkpointConfig = env.checkpointConfig

    // Timeout if checkpoint takes too long (prevents backpressure)
    checkpointConfig.checkpointTimeout = config.checkPointTimeout

    // Allow job to fail a few times before hard stopping
    checkpointConfig.tolerableCheckpointFailureNumber = config.tolerableCheckpointFailureNumber

    // Ensure a gap between checkpoints to allow processing to happen
    checkpointConfig.minPauseBetweenCheckpoints = config.minPauseBetweenCheckpoints

    // Only 1 checkpoint happening at a time to reduce overhead
    checkpointConfig.maxConcurrentCheckpoints = config.maxConcurrentCheckpoints

    // Retain checkpoint data on cancellation for easier debugging/redeployment
    checkpointConfig.externalizedCheckpointRetention =
        ExternalizedCheckpointRetention.RETAIN_ON_CANCELLATION
}
