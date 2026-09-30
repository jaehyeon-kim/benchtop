package me.jaehyeon.config

import java.io.Serializable

/**
 * centralized Configuration Management.
 * Loads settings from Environment Variables or defaults to local dev settings.
 */
data class AppConfig(
    // Kafka Configuration
    val bootstrapAddress: String = System.getenv("BOOTSTRAP") ?: "broker-1:19092",
    val registryUrl: String = System.getenv("REGISTRY_URL") ?: "http://karapace:8081",
    val feedbackTopic: String = "feedback-events",
    // Flink Configuration
    val jobName: String = "RecommenderParameterUpdate",
    val parallelism: Int = 6,
    val checkpointInterval: Long = 10_000,
    val checkPointTimeout: Long = 60_000,
    val minPauseBetweenCheckpoints: Long = 500,
    val maxConcurrentCheckpoints: Int = 1,
    val tolerableCheckpointFailureNumber: Int = 3,
    // Redis Configuration
    val redisHost: String = System.getenv("REDIS_HOST") ?: "valkey",
    val redisPort: Int = 6379,
    val redisUser: String = System.getenv("REDIS_USER") ?: "user",
    val redisPass: String = System.getenv("REDIS_PASS") ?: "password",
    // File Source (Bootstrap)
    // Object storage, so the split enumerator on the JobManager and the readers on
    // every TaskManager all see the same file without per-container copies.
    val eventLog: String = System.getenv("EVENT_LOG") ?: "s3://odctl-dev/recsys/training_log.csv",
) : Serializable
