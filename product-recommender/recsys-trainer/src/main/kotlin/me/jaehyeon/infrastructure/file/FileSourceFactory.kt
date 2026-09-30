package me.jaehyeon.infrastructure.file

import me.jaehyeon.config.AppConfig
import me.jaehyeon.domain.model.Feedback
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.file.src.FileSource
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat
import org.apache.flink.connector.file.src.reader.StreamFormat
import org.apache.flink.core.fs.FSDataInputStream
import org.apache.flink.core.fs.Path
import org.slf4j.LoggerFactory
import java.io.BufferedReader
import java.io.InputStreamReader

/**
 * Factory for creating a FileSource that reads historical data for Bootstrapping.
 *
 * It parses a CSV file line-by-line directly into [Feedback] objects, allowing
 * the Flink job to pre-train the model before switching to the live Kafka stream.
 */
class FileSourceFactory(
    private val config: AppConfig,
) {
    private val logger = LoggerFactory.getLogger(FileSourceFactory::class.java)

    /**
     * Builds the FileSource using a custom CSV format.
     */
    fun createSource(): FileSource<Feedback> {
        logger.info("Initializing File Source for file: '{}'", config.eventLog)

        val path = Path(config.eventLog)

        return FileSource
            .forRecordStreamFormat(FeedbackCsvFormat(), path)
            .build()
    }

    /**
     * Custom StreamFormat that wraps a standard text reader.
     * It decodes CSV lines into Feedback domain objects on the fly.
     */
    private class FeedbackCsvFormat : SimpleStreamFormat<Feedback>() {
        override fun createReader(
            config: Configuration,
            stream: FSDataInputStream,
        ): StreamFormat.Reader<Feedback> {
            // Use standard Java BufferedReader for efficient line reading
            val reader = BufferedReader(InputStreamReader(stream))

            return object : StreamFormat.Reader<Feedback> {
                override fun read(): Feedback? {
                    while (true) {
                        val line = reader.readLine() ?: return null // End of File

                        // Parse logic (skip bad lines internally)
                        val parsed = parseCsvLine(line)
                        if (parsed != null) {
                            return parsed
                        }
                        // If null (e.g. header), loop continues to next line
                    }
                }

                override fun close() {
                    reader.close()
                }
            }
        }

        override fun getProducedType(): TypeInformation<Feedback> = TypeInformation.of(Feedback::class.java)
    }

    companion object {
        /**
         * Parses a single CSV line into a Feedback object.
         * Expected columns matches the Python simulation output.
         */
        fun parseCsvLine(line: String): Feedback? {
            val tokens = line.split(",")

            // Basic validation: Check column count and skip Header row
            if (tokens.size < 18 || tokens[0] == "event_id") {
                return null
            }

            return try {
                val eventId = tokens[0]
                // Context features are indices 1 to 16 (15 features)
                val contextVector = tokens.subList(1, 16).map { it.toDouble() }
                val productId = tokens[16]
                val reward = tokens[17].toInt()

                Feedback(
                    eventId = eventId,
                    productId = productId,
                    reward = reward,
                    contextVector = contextVector,
                    timestamp = 0L, // Historical data is treated as time 0
                )
            } catch (e: Exception) {
                e.printStackTrace()
                null // Skip malformed lines
            }
        }
    }
}
