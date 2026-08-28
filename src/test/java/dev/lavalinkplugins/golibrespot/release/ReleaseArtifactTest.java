package dev.lavalinkplugins.golibrespot.release;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Release-artifact verification (T23).
 *
 * <p>Runs in the normal {@code clean build} suite and pins the RELEASE
 * contract of the produced jar:
 *
 * <ul>
 *   <li>the jar ships the Lavalink descriptors, the license texts and the main
 *       class ({@link #requiredReleaseEntriesPresent()});</li>
 *   <li>the jar is a single self-contained unit: zero third-party classes, no
 *       nested jars, no runtime dependencies ({@link
 *       #jarContainsNoThirdPartyContent()});</li>
 *   <li>the jar is reproducible: constant entry timestamps and sorted entry
 *       order, so two {@code clean build} runs are byte-identical ({@link
 *       #reproducibleJarProperties()}). The full two-build SHA-256 comparison
 *       itself runs as a shell gate in .github/workflows/release.yml and in the
 *       T23 evidence.</li>
 * </ul>
 *
 * <p>The test task declares {@code dependsOn(tasks.jar)} in build.gradle.kts so
 * this suite always sees a finished jar even in a {@code clean build}.
 */
@Tag("release")
class ReleaseArtifactTest {

  /** Pinned release version (group/version live in build.gradle.kts: 1.0.0). */
  private static final String JAR_NAME = "lavalink-go-librespot-1.0.0.jar";

  private static final Path JAR_PATH =
      Path.of("build", "libs", JAR_NAME).toAbsolutePath();

  /** Entries the release jar MUST contain (descriptors, licenses, main class). */
  private static final List<String> REQUIRED_ENTRIES =
      List.of(
          "lavalink-plugin.yml",
          "lavalink-plugins/golibrespot.properties",
          "LICENSE",
          "THIRD_PARTY_NOTICES",
          "dev/lavalinkplugins/golibrespot/GoLibrespotPlugin.class");

  @Test
  void requiredReleaseEntriesPresent() throws IOException {
    try (ZipFile jar = openJar()) {
      List<String> names = entryNames(jar);

      // (a) The release jar must ship the Lavalink descriptors, the license
      // texts and the main class.
      assertThat(names).containsAll(REQUIRED_ENTRIES);

      // The 4.2.x descriptor must identify the plugin exactly.
      String pluginYml = readEntry(jar, "lavalink-plugin.yml");
      assertThat(pluginYml)
          .contains("name: golibrespot")
          .contains("main: dev.lavalinkplugins.golibrespot.GoLibrespotPlugin")
          .contains("version: 1.0.0");
    }
  }

  @Test
  void jarContainsNoThirdPartyContent() throws IOException {
    try (ZipFile jar = openJar()) {
      List<String> names = entryNames(jar);

      // (b) Zero runtime dependencies: every non-directory entry must be our
      // own compiled classes, our descriptors, the license texts, or the
      // manifest. No third-party packages, no nested jars, no module-info,
      // no Multi-Release versioned classes.
      for (String name : names) {
        if (name.endsWith("/")) {
          continue; // directory entry
        }
        assertThat(whitelisted(name))
            .as("entry %s must be our own content, not a third-party class", name)
            .isTrue();
      }

      // Explicit examples from the release contract: no Spring, no Jackson.
      assertThat(names).noneMatch(n -> n.contains("springframework"));
      assertThat(names).noneMatch(n -> n.contains("jackson"));
      // No nested jars and no module descriptor.
      assertThat(names).noneMatch(n -> n.endsWith(".jar"));
      assertThat(names).doesNotContain("module-info.class");
    }
  }

  @Test
  void reproducibleJarProperties() throws IOException {
    try (ZipFile jar = openJar()) {
      List<ZipEntry> entries = jar.stream().collect(Collectors.toList());
      List<String> names = entryNames(jar);

      // isPreserveFileTimestamps=false stamps every entry with the same
      // constant time (ZipEntryConstants.CONSTANT_TIME_FOR_ZIP_ENTRIES), so
      // build timestamps can never leak into the jar bytes.
      List<Instant> times = new ArrayList<>();
      for (ZipEntry entry : entries) {
        times.add(entry.getLastModifiedTime().toInstant());
      }
      assertThat(times).as("every zip entry must carry the identical timestamp").containsOnly(times.get(0));

      // Deterministic entry order (Gradle 8.14.5, isReproducibleFileOrder):
      // the jar task walks each source (manifest, classes, resources, license
      // texts) in declaration order with a sorted walker — NOT a global string
      // sort (META-INF precedes dev/, LICENSE trails the resources). Assert
      // the two properties that make output independent of filesystem
      // enumeration: (i) top-level groups are contiguous, (ii) within a group
      // entries are in String natural order. Together with the constant
      // timestamps these are the non-byte-level preconditions of the
      // byte-identical two-build SHA-256 gate in release.yml.
      Map<String, List<String>> byGroup = new LinkedHashMap<>();
      for (String name : names) {
        byGroup.computeIfAbsent(topLevelGroup(name), k -> new ArrayList<>()).add(name);
      }
      for (Map.Entry<String, List<String>> group : byGroup.entrySet()) {
        List<String> groupEntries = group.getValue();
        List<String> sortedGroup = new ArrayList<>(groupEntries);
        sortedGroup.sort(Comparator.naturalOrder());
        assertThat(groupEntries)
            .as("entries of top-level group %s must be sorted", group.getKey())
            .isEqualTo(sortedGroup);
      }
      // Contiguity: walking the flat listing, a group that already appeared
      // must not appear again (no interleaving).
      List<String> seenGroups = new ArrayList<>();
      String current = null;
      for (String name : names) {
        String group = topLevelGroup(name);
        if (!group.equals(current)) {
          assertThat(seenGroups)
              .as("top-level group %s must be contiguous (no interleaving)", group)
              .doesNotContain(group);
          seenGroups.add(group);
          current = group;
        }
      }
    }
  }

  private static String topLevelGroup(String name) {
    int slash = name.indexOf('/');
    return slash < 0 ? name : name.substring(0, slash);
  }

  private static ZipFile openJar() throws IOException {
    assertThat(JAR_PATH).as("release jar %s must exist", JAR_PATH).isRegularFile();
    return new ZipFile(JAR_PATH.toFile(), StandardCharsets.UTF_8);
  }

  private static List<String> entryNames(ZipFile jar) {
    return jar.stream().map(ZipEntry::getName).collect(Collectors.toList());
  }

  private static String readEntry(ZipFile jar, String name) throws IOException {
    return new String(jar.getInputStream(jar.getEntry(name)).readAllBytes(), StandardCharsets.UTF_8);
  }

  private static boolean whitelisted(String name) {
    if (name.startsWith("META-INF/")) {
      return true; // MANIFEST.MF (and any future META-INF license texts)
    }
    if (name.startsWith("dev/lavalinkplugins/golibrespot/")) {
      return true; // our compiled classes only
    }
    if (name.startsWith("lavalink-plugins/")) {
      return true; // dev-server descriptor generated by the Lavalink plugin
    }
    return switch (name) {
      case "lavalink-plugin.yml", "LICENSE", "THIRD_PARTY_NOTICES" -> true;
      default -> false;
    };
  }
}
