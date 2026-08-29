import org.gradle.api.attributes.Bundling

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
version = "1.2.2"

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

// T23 reproducibility: the Lavalink gradle plugin writes the descriptor via
// java.util.Properties.store(), which stamps a live "#<date>" comment line
// into every build — that makes byte-identical rebuilds impossible. Strip the
// comment line right after generation (the file keeps its plugin-generated
// name/path/version keys; processResources then carries it into the jar).
tasks.named("generatePluginProperties") {
    doLast {
        // The plugin writes build/generated/lavalink/main/resources/... and
        // processResources copies it to build/resources/main/...; sanitize
        // both (idempotent) so whichever copy reaches the jar is clean.
        val candidates =
            listOf(
                layout.buildDirectory.file("generated/lavalink/main/resources/lavalink-plugins/golibrespot.properties"),
                layout.buildDirectory.file("resources/main/lavalink-plugins/golibrespot.properties"),
            )
        for (descriptor in candidates) {
            val file = descriptor.get().asFile
            if (file.exists()) {
                val lines = file.readLines().filterNot { it.startsWith("#") }
                file.writeText(lines.joinToString("\n") + "\n")
            }
        }
    }
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
        // T23: ship the license + third-party notices inside the release jar
        // (the release-artifact test asserts their presence; a release jar
        // without its license texts would be legally deficient).
        from("LICENSE")
        from("THIRD_PARTY_NOTICES")
    }

    test {
        useJUnitPlatform()
        // T23: the release-artifact test reads build/libs/<name>.jar. `jar`
        // is NOT a transitive dependency of `test`, so without this the
        // artifact test could run before (or in parallel with) the jar task
        // in a clean build. Order it explicitly (lazy name form: inside the
        // `tasks {}` block `tasks.jar` does not resolve).
        dependsOn("jar")
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
    testImplementation("org.awaitility:awaitility:4.3.0")
    // T5 deviation: lavaplayer is exposed to MAIN code via plugin-api's
    // compileOnly api() exposure, but compileOnly is NOT on the test compile
    // classpath — tests that assert on AudioTrackInfo need this test-scope
    // declaration (matches the version plugin-api 4.2.2 already resolves).
    testImplementation("dev.arbjerg:lavaplayer:2.2.6")
    // Test-only WebSocket server for the fake go-librespot daemon fixture (T6+).
    testImplementation("org.java-websocket:Java-WebSocket:1.5.7")
    // T19 integration smoke: the real Lavalink test server. The bare GAV resolves
    // the plain (non-boot) jar whose classes are directly classpath-loadable,
    // plus its real transitive deps (Spring Boot 3.3.0, lavaplayer, koe, ...).
    testImplementation("dev.arbjerg.lavalink:Lavalink-Server:4.2.2")
    // The server POM scopes its Spring Boot deps as runtime, so the test code
    // that boots the server in-JVM (SpringApplication / context classes) needs
    // an explicit test-scope Boot declaration matching the server's 3.3.0.
    testImplementation("org.springframework.boot:spring-boot:3.3.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// The server's dependency graph contains dev.arbjerg:lavadsp, which publishes
// both a normal and a shadowed variant; the test configurations must declare
// which one to consume or Gradle cannot resolve it (T19).
configurations {
    testCompileClasspath {
        attributes {
            attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
        }
    }
    testRuntimeClasspath {
        attributes {
            attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.EXTERNAL))
        }
    }
}
