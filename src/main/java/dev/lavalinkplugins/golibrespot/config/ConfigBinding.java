package dev.lavalinkplugins.golibrespot.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;

/**
 * Rebuilds a config subtree as a nested {@link Map} from the Spring
 * {@link Environment}'s flattened property sources (command-line args, YAML,
 * system properties) — the exact shape {@link GoLibrespotConfig#from(Map)}
 * binds strictly.
 *
 * <p>Spring Boot's {@code Binder} cannot do this for a {@code Map<String,
 * Object>} target: it has no type information telling it that {@code backends}
 * is a list, so indexed keys ({@code backends[0].name}) collapse into an
 * index-keyed map instead of a list. Walking the enumerable property sources
 * and reconstructing the tree explicitly is deterministic for every source
 * kind and preserves per-element scalar values as they arrive (String for
 * command-line args, native types for YAML).</p>
 *
 * <p>Precedence is honored: sources are walked lowest-precedence first, so a
 * higher-precedence source's value for a leaf overwrites the lower one, while
 * leaves only present in lower-precedence sources survive. Non-enumerable
 * sources (whose contents cannot be enumerated) are skipped.</p>
 */
public final class ConfigBinding {

  private ConfigBinding() {
  }

  /**
   * Binds the subtree under {@code prefix} (e.g. {@code plugins.golibrespot})
   * into a nested map. Returns an empty map when the environment is not a
   * {@link ConfigurableEnvironment} or no matching properties exist.
   */
  public static Map<String, Object> subtree(Environment environment, String prefix) {
    Map<String, Object> root = new LinkedHashMap<>();
    if (!(environment instanceof ConfigurableEnvironment configurable)) {
      return root;
    }
    String dotPrefix = prefix + ".";
    List<PropertySource<?>> sources = new ArrayList<>();
    configurable.getPropertySources().forEach(sources::add);
    Collections.reverse(sources); // lowest precedence first → higher wins on conflict
    for (PropertySource<?> source : sources) {
      if (!(source instanceof EnumerablePropertySource<?> enumerable)) {
        continue;
      }
      for (String name : enumerable.getPropertyNames()) {
        if (name == null || !name.startsWith(dotPrefix)) {
          continue;
        }
        Object value = source.getProperty(name);
        if (value == null) {
          continue;
        }
        putAt(root, parseSegments(name.substring(dotPrefix.length())), value);
      }
    }
    return root;
  }

  // ------------------------------------------------------------ tree building

  private static final class Segment {
    final String key;
    final Integer index;

    Segment(String key, Integer index) {
      this.key = key;
      this.index = index;
    }
  }

  /** Splits {@code backends[0].name} into [backends[0], name]. */
  private static List<Segment> parseSegments(String path) {
    List<Segment> segments = new ArrayList<>();
    for (String part : path.split("\\.")) {
      int bracket = part.indexOf('[');
      if (bracket < 0) {
        segments.add(new Segment(part, null));
      } else {
        String key = part.substring(0, bracket);
        String index = part.substring(bracket + 1, part.length() - 1);
        segments.add(new Segment(key, Integer.valueOf(index)));
      }
    }
    return segments;
  }

  private static void putAt(Map<String, Object> root, List<Segment> segments, Object value) {
    Map<String, Object> current = root;
    for (int i = 0; i < segments.size() - 1; i++) {
      Segment segment = segments.get(i);
      current = segment.index == null
          ? ensureMapAt(current, segment.key)
          : ensureMapAtListElement(current, segment.key, segment.index);
    }
    Segment last = segments.get(segments.size() - 1);
    if (last.index == null) {
      current.put(last.key, value);
    } else {
      List<Object> list = ensureList(current, last.key);
      while (list.size() <= last.index) {
        list.add(null);
      }
      list.set(last.index, value);
    }
  }

  private static Map<String, Object> ensureMapAt(Map<String, Object> map, String key) {
    Object existing = map.get(key);
    if (!(existing instanceof Map<?, ?>)) {
      existing = new LinkedHashMap<String, Object>();
      map.put(key, existing);
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) existing;
    return result;
  }

  private static Map<String, Object> ensureMapAtListElement(
      Map<String, Object> map, String key, int index) {
    List<Object> list = ensureList(map, key);
    while (list.size() <= index) {
      list.add(null);
    }
    Object element = list.get(index);
    if (!(element instanceof Map<?, ?>)) {
      element = new LinkedHashMap<String, Object>();
      list.set(index, element);
    }
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) element;
    return result;
  }

  private static List<Object> ensureList(Map<String, Object> map, String key) {
    Object existing = map.get(key);
    if (!(existing instanceof List<?>)) {
      existing = new ArrayList<Object>();
      map.put(key, existing);
    }
    @SuppressWarnings("unchecked")
    List<Object> result = (List<Object>) existing;
    return result;
  }
}
