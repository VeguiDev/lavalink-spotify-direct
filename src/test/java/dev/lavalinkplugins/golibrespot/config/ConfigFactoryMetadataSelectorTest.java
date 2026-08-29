package dev.lavalinkplugins.golibrespot.config;

import static org.assertj.core.api.Assertions.assertThat;

import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver.ReadyBackend;
import dev.lavalinkplugins.golibrespot.metadata.MetadataResolver.ReadyBackendSelector;
import dev.lavalinkplugins.golibrespot.pool.ExclusivePool;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

class ConfigFactoryMetadataSelectorTest {

  @Test
  void singleBackendProducesOneFiniteCandidatePerResolve() {
    try (ExclusivePool pool = new ExclusivePool(backends("alpha"))) {
      ReadyBackendSelector selector = ConfigFactory.poolReadyBackendSelector(pool);

      assertThat(selector.readyBackends()).extracting(ReadyBackend::restBaseUrl)
          .containsExactly("http://127.0.0.1:20000");
      assertThat(selector.readyBackends()).extracting(ReadyBackend::restBaseUrl)
          .containsExactly("http://127.0.0.1:20000");
    }
  }

  @Test
  void snapshotsRotateAndNeverContainDuplicates() {
    try (ExclusivePool pool = new ExclusivePool(backends("alpha", "beta", "gamma"))) {
      ReadyBackendSelector selector = ConfigFactory.poolReadyBackendSelector(pool);

      List<String> first = urls(selector.readyBackends());
      List<String> second = urls(selector.readyBackends());
      List<String> third = urls(selector.readyBackends());

      assertThat(first).containsExactly(
          "http://127.0.0.1:20000", "http://127.0.0.1:20001", "http://127.0.0.1:20002");
      assertThat(second).containsExactly(
          "http://127.0.0.1:20001", "http://127.0.0.1:20002", "http://127.0.0.1:20000");
      assertThat(third).containsExactly(
          "http://127.0.0.1:20002", "http://127.0.0.1:20000", "http://127.0.0.1:20001");
      assertThat(new HashSet<>(first)).hasSize(first.size());
    }
  }

  @Test
  void concurrentResolvesReceiveIndependentFiniteSnapshots() throws Exception {
    try (ExclusivePool pool = new ExclusivePool(backends("alpha", "beta", "gamma"))) {
      ReadyBackendSelector selector = ConfigFactory.poolReadyBackendSelector(pool);
      ExecutorService executor = Executors.newFixedThreadPool(8);
      try {
        List<Callable<List<ReadyBackend>>> calls = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
          calls.add(selector::readyBackends);
        }
        List<Future<List<ReadyBackend>>> results = executor.invokeAll(calls);
        for (Future<List<ReadyBackend>> result : results) {
          List<ReadyBackend> snapshot = result.get();
          assertThat(snapshot).hasSize(3);
          assertThat(new HashSet<>(snapshot)).hasSize(3);
        }
      } finally {
        executor.shutdownNow();
      }
    }
  }

  private static List<String> urls(List<ReadyBackend> backends) {
    return backends.stream().map(ReadyBackend::restBaseUrl).toList();
  }

  private static List<BackendConfig> backends(String... names) {
    List<Map<String, Object>> raw = new ArrayList<>();
    for (int i = 0; i < names.length; i++) {
      raw.add(Map.of(
          "name", names[i],
          "restBaseUrl", "http://127.0.0.1:" + (20_000 + i),
          "fifoPath", Path.of(System.getProperty("java.io.tmpdir"), names[i] + ".fifo").toString()));
    }
    return GoLibrespotConfig.from(Map.of("enabled", true, "backends", raw)).getBackends();
  }
}
