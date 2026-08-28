package dev.lavalinkplugins.golibrespot.lifecycle;

import dev.lavalinkplugins.golibrespot.fifo.FifoReader;
import java.io.InputStream;

/**
 * Injectable seam for constructing the {@link FifoReader} over an opened FIFO
 * stream. Production uses {@code FifoReader::new}; tests inject a scripted
 * reader (or a real reader over a scripted stream) so the coordinator logic
 * runs on every platform without a real FIFO.
 */
@FunctionalInterface
public interface FifoReaderFactory {

  /** Creates a reader over the opened FIFO read-end stream. */
  FifoReader create(InputStream in);
}
