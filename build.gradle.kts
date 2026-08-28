plugins {
    java
    // Lavalink Gradle plugin: generates the 4.2.x plugin manifest
    // (lavalink-plugins/<name>.properties) and registers the test-server
    // task `runLavaLink`. Verified on the Gradle Plugin Portal as
    // dev.arbjerg.lavalink.gradle-plugin:1.1.2.
    id("dev.arbjerg.lavalink.gradle-plugin") version "1.1.2"
    // CycloneDX SBOM generation (single owner of this config: T2).
    id("org.cyclonedx.bom") version "2.4.1"
}

group = "dev.lavalinkplugins.golibrespot"
version = "1.0.0"

repositories {
    maven("https://maven.lavalink.dev/releases")
    mavenCentral()
}

lavalinkPlugin {
    name = "golibrespot"
    // plugin-api + Lavalink-Server (test server) 4.2.2 from maven.lavalink.dev/releases.
    // `version` defaults to project.version (1.0.0), `path` defaults to group
    // (dev.lavalinkplugins.golibrespot) — the 4.2.x server loads plugins from the
    // generated lavalink-plugins/golibrespot.properties descriptor.
    apiVersion = "4.2.2"
    serverVersion = "4.2.2"
}

tasks {
    compileJava {
        options.encoding = "UTF-8"
        // Target Java 17 bytecode; no toolchain download (compile with the
        // running JDK's --release flag instead).
        options.release = 17
    }

    withType<Jar>().configureEach {
        // Reproducible JARs: stable timestamps + stable entry order.
        isPreserveFileTimestamps = false
        isReproducibleFileOrder = true
    }

    test {
        useJUnitPlatform()
    }
}

dependencies {
    // Lavalink plugin API 4.2.2 (compileOnly — the server provides it at runtime).
    // It transitively exposes lavaplayer (dev.arbjerg:lavaplayer:2.2.6) and
    // Spring Boot 3.3.0 via `api`, so no explicit lavaplayer-fork dependency is
    // needed. Zero runtime dependencies (java.net.http covers REST + WS later).
    compileOnly("dev.arbjerg.lavalink:plugin-api:4.2.2")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core:3.26.3")
    testImplementation("org.awaitility:awaitility:4.2.2")
    // T5 deviation: lavaplayer is exposed to MAIN code via plugin-api's
    // compileOnly api() exposure, but compileOnly is NOT on the test compile
    // classpath — tests that assert on AudioTrackInfo need this test-scope
    // declaration (matches the version plugin-api 4.2.2 already resolves).
    testImplementation("dev.arbjerg:lavaplayer:2.2.6")
    // Test-only WebSocket server for the fake go-librespot daemon fixture (T6+).
    testImplementation("org.java-websocket:Java-WebSocket:1.5.7")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
