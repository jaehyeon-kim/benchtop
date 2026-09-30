plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.8.0"
}

rootProject.name = "order-streams"

include("orders-json-clients", "orders-avro-clients", "orders-stats-streams", "orders-stats-flink")
