package app.orderloop.platform.outbox;

import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Runs the relay on its own thread, so a slow broker never delays other scheduled work such as
 * deadline scans. Keeps going while batches come back full; waits when idle, locked out or short.
 */
final class OutboxRelayPoller implements SmartLifecycle {

  private static final Logger log = LoggerFactory.getLogger(OutboxRelayPoller.class);

  private final Supplier<OptionalInt> batch;
  private final int batchSize;
  private final Duration interval;
  private ScheduledExecutorService executor;

  OutboxRelayPoller(Supplier<OptionalInt> batch, int batchSize, Duration interval) {
    this.batch = batch;
    this.batchSize = batchSize;
    this.interval = interval;
  }

  @Override
  public synchronized void start() {
    executor =
        Executors.newSingleThreadScheduledExecutor(
            runnable -> {
              Thread thread = new Thread(runnable, "outbox-relay");
              thread.setDaemon(true);
              return thread;
            });
    executor.scheduleWithFixedDelay(this::poll, 0, interval.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void poll() {
    try {
      while (batch.get().orElse(0) == batchSize) {
        // The batch was full, so more rows may be waiting.
      }
    } catch (RuntimeException e) {
      // Unpublished rows stay in the outbox, and the next poll retries them.
      log.warn("Outbox relay batch failed; it will be retried", e);
    }
  }

  @Override
  public synchronized void stop() {
    if (executor != null) {
      executor.shutdownNow();
      executor = null;
    }
  }

  @Override
  public synchronized boolean isRunning() {
    return executor != null;
  }
}
