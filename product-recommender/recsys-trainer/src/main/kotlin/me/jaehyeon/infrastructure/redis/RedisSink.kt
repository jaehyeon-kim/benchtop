package me.jaehyeon.infrastructure.redis

import me.jaehyeon.config.AppConfig
import me.jaehyeon.domain.model.LinUCBModel
import org.apache.flink.api.connector.sink2.Sink
import org.apache.flink.api.connector.sink2.SinkWriter
import org.apache.flink.api.connector.sink2.WriterInitContext

/**
 * Flink Sink V2 implementation for Redis.
 * This sink does not require a Committer because Redis SET operations are idempotent.
 */
class RedisSink(
    private val config: AppConfig,
) : Sink<LinUCBModel> {
    override fun createWriter(context: WriterInitContext): SinkWriter<LinUCBModel> {
        // Use taskInfo.indexOfThisSubtask instead of context.subtaskId to fix the warning
        val subtaskId = context.taskInfo.indexOfThisSubtask
        return RedisSinkWriter(config, subtaskId)
    }
}
