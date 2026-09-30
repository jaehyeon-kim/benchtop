package me.jaehyeon.infrastructure.redis

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import me.jaehyeon.config.AppConfig
import me.jaehyeon.domain.model.LinUCBModel
import org.apache.flink.api.connector.sink2.SinkWriter
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisPool
import redis.clients.jedis.JedisPoolConfig

/**
 * Writes LinUCB models to Redis using Jedis.
 *
 * Architecture:
 * - Initializes a JedisPool on creation.
 * - serializes the model to JSON.
 * - Performs a SET command: "linucb:{productId}" -> JSON.
 */
class RedisSinkWriter(
    config: AppConfig,
    private val subtaskId: Int,
) : SinkWriter<LinUCBModel> {
    private val log = LoggerFactory.getLogger(RedisSinkWriter::class.java)
    private val jedisPool: JedisPool
    private val mapper = jacksonObjectMapper()

    init {
        log.info("Sink(Task $subtaskId): Initializing Redis Pool at ${config.redisHost}:${config.redisPort}")

        val poolConfig =
            JedisPoolConfig().apply {
                maxTotal = 20
                maxIdle = 5
                minIdle = 1
            }

        // Valkey disables the default user, so a username is mandatory alongside the password
        jedisPool =
            JedisPool(
                poolConfig,
                config.redisHost,
                config.redisPort,
                2000,
                config.redisUser,
                config.redisPass,
            )
    }

    override fun write(
        element: LinUCBModel,
        context: SinkWriter.Context,
    ) {
        try {
            jedisPool.resource.use { jedis ->
                // Serialize model object to JSON
                val json = mapper.writeValueAsString(element)
                // Construct Key
                val key = "linucb:${element.productId}"
                // Upsert to Redis
                jedis.set(key, json)
                log.info("Updated model for Product ${element.productId} in batch")
            }
        } catch (e: Exception) {
            // Log error and throw to trigger Flink retry/failure handling
            log.error("Sink(Task $subtaskId): Failed to write to Redis for ${element.productId}", e)
            throw e
        }
    }

    override fun flush(endOfInput: Boolean) {
        // No buffering logic implemented; writes are immediate.
        log.debug("Sink(Task $subtaskId): Flushing (No-op for sync writer)")
    }

    override fun close() {
        log.info("Sink(Task $subtaskId): Closing Redis Pool.")
        if (!jedisPool.isClosed) {
            jedisPool.close()
        }
    }
}
