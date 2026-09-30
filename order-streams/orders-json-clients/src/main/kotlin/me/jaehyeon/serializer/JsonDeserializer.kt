package me.jaehyeon.serializer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import org.apache.kafka.common.serialization.Deserializer

class JsonDeserializer<T>(
    private val targetClass: Class<T>,
) : Deserializer<T> {
    private val objectMapper =
        ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)

    override fun deserialize(
        topic: String?,
        data: ByteArray?,
    ): T? = data?.let { objectMapper.readValue(it, targetClass) }
}
