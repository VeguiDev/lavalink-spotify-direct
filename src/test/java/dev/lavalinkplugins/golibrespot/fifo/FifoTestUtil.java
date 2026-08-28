package dev.lavalinkplugins.golibrespot.fifo;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;

/**
 * Shared helper for FIFO tests (owned by T11; reused by FifoReaderTest/T12).
 *
 * <p>FIFOs cannot be created with {@code Files.createFile} — the FIFO inode
 * must come from the {@code mkfifo} binary. All helpers degrade gracefully on
 * platforms without mkfifo (e.g. Windows): {@link #mkfifoAvailable()} returns
 * {@code false} and {@link #createTempFifo()} throws. Tests gate themselves
 * with {@link #requireMkfifo()}; on Windows the suite then reports skipped and
 * the build stays green, while CI (ubuntu) executes the real FIFO tests.
 *
 * <p>Temporary FIFOs created here live in a dedicated temp directory and may be
 * unlinked by {@link #deleteTempFifo(Path)} — this is test-only cleanup, not
 * the runtime "never unlink the FIFO" rule.
 */
public final class FifoTestUtil {

  private static final long MKFIFO_TIMEOUT_SECONDS = 10;

  private FifoTestUtil() {}

  /**
   * JUnit gate: skips the calling test unless mkfifo can actually create a
   * FIFO on this machine. Use together with {@code @EnabledOnOs(OS.LINUX)}.
   */
  public static void requireMkfifo() {
    Assumptions.assumeTrue(mkfifoAvailable(), "mkfifo unavailable — skipping FIFO test");
  }

  /**
   * True iff {@code mkfifo} exists and can create a FIFO in the system temp
   * dir. The probe FIFO (and its temp dir) is deleted before returning.
   */
  public static boolean mkfifoAvailable() {
    Path dir = null;
    try {
      dir = Files.createTempDirectory("golibrespot-mkfifo-probe");
      Path probe = dir.resolve("probe");
      Process p = new ProcessBuilder("mkfifo", probe.toString())
          .redirectErrorStream(true)
          .start();
      boolean finished = p.waitFor(MKFIFO_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      if (!finished) {
        p.destroyForcibly();
        return false;
      }
      boolean ok = p.exitValue() == 0 && Files.exists(probe);
      if (ok) {
        Files.delete(probe);
      }
      return ok;
    } catch (IOException e) {
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } finally {
      if (dir != null) {
        try {
          Files.deleteIfExists(dir);
        } catch (IOException ignored) {
          // best-effort cleanup
        }
      }
    }
  }

  /**
   * Creates a unique FIFO inside a fresh temp directory via {@code mkfifo}.
   *
   * @return the FIFO path (parent dir is a dedicated temp dir)
   * @throws IOException if mkfifo is missing, fails, or times out
   * @throws InterruptedException if the calling thread is interrupted while waiting
   */
  public static Path createTempFifo() throws IOException, InterruptedException {
    Path dir = Files.createTempDirectory("golibrespot-fifo");
    Path fifo = dir.resolve("fifo-" + UUID.randomUUID());
    Process p = new ProcessBuilder("mkfifo", fifo.toString())
        .redirectErrorStream(true)
        .start();
    if (!p.waitFor(MKFIFO_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      p.destroyForcibly();
      throw new IOException("mkfifo timed out creating " + fifo);
    }
    if (p.exitValue() != 0) {
      throw new IOException(
          "mkfifo failed for " + fifo + ": "
              + new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
    }
    return fifo;
  }

  /** Deletes a FIFO created by {@link #createTempFifo()} plus its temp dir. */
  public static void deleteTempFifo(Path fifo) {
    if (fifo == null) {
      return;
    }
    try {
      Files.deleteIfExists(fifo);
    } catch (IOException ignored) {
      // best-effort cleanup
    }
    Path dir = fifo.getParent();
    if (dir != null) {
      try {
        Files.deleteIfExists(dir);
      } catch (IOException ignored) {
        // best-effort cleanup
      }
    }
  }
}
