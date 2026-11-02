package app.orderloop.platform.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class OutboxRelayPollerTest {

  @Test
  void drainsFullBatchesThenWaitsOnItsOwnThread() {
    AtomicInteger calls = new AtomicInteger();
    List<String> threads = new CopyOnWriteArrayList<>();
    var poller =
        new OutboxRelayPoller(
            () -> {
              threads.add(Thread.currentThread().getName());
              // Two full batches, then nothing left.
              return OptionalInt.of(calls.incrementAndGet() <= 2 ? 10 : 0);
            },
            10,
            Duration.ofHours(1));

    poller.start();
    try {
      await().atMost(Duration.ofSeconds(5)).until(() -> calls.get() >= 3);
      assertThat(poller.isRunning()).isTrue();
    } finally {
      poller.stop();
    }

    // The interval is an hour, so all three calls came from the first poll, one after another.
    assertThat(calls.get()).isEqualTo(3);
    assertThat(threads).allMatch(name -> name.equals("outbox-relay"));
    assertThat(poller.isRunning()).isFalse();
  }

  @Test
  void aFailingBatchDoesNotStopLaterPolls() {
    AtomicInteger calls = new AtomicInteger();
    var poller =
        new OutboxRelayPoller(
            () -> {
              if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("broker unavailable");
              }
              return OptionalInt.of(0);
            },
            10,
            Duration.ofMillis(10));

    poller.start();
    try {
      await().atMost(Duration.ofSeconds(5)).until(() -> calls.get() >= 2);
    } finally {
      poller.stop();
    }
  }
}
