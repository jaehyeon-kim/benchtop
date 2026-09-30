// The plugin versions are set once here, and each application applies what it needs.
plugins {
    kotlin("jvm") version "2.1.20" apply false
    id("com.github.davidmc24.gradle.plugin.avro") version "1.9.1" apply false
    id("com.github.johnrengelman.shadow") version "8.1.1" apply false
    id("org.jlleitschuh.gradle.ktlint") version "14.2.0" apply false
}
