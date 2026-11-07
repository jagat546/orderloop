package app.orderloop.fulfilment.provider;

import app.orderloop.dto.DemoOrder;
import app.orderloop.dto.Envelope;
import app.orderloop.dto.SagaEvent;
import app.orderloop.dto.SagaEvent.DeliveredEvt;
import app.orderloop.dto.SagaEvent.DeliveryFailedEvt;
import app.orderloop.dto.SagaEvent.RtoReceivedEvt;
import app.orderloop.dto.SagaEvent.RtoStartedEvt;
import app.orderloop.dto.Topics;
import app.orderloop.platform.outbox.OutboxWriter;
import app.orderloop.platform.tenant.TenantContext;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The default carrier: books every shipment except one whose order ID ends in {@value
 * #NO_COURIER_SUFFIX}. Tests and the demo then report what the carrier saw with {@link #deliver},
 * {@link #failDelivery}, {@link #startRto} and {@link #receiveRto}, as a carrier's webhooks would.
 * Each event goes out through the service's outbox in the tenant's scope, and only for a shipment
 * that tenant booked. Events are reported as given, in any order, because real carriers send them
 * late and out of order. It does no I/O, which is why booking may run inside the inbox transaction
 * (invariant 6).
 *
 * <p>A {@link DemoOrder} moves on by itself, {@code orderloop.simulator.demo-step} apart: a
 * delivered one is delivered; an RTO one starts its return, then is received back.
 *
 * <p>shortcut: bookings and the demo's pending reports are in memory and lost on restart; persist
 * them if the demo needs them to survive one.
 */
@Component
@ConditionalOnProperty(
    name = "orderloop.fulfilment.provider",
    havingValue = "simulator",
    matchIfMissing = true)
public class SimulatedCarrier implements FulfilmentProvider {

  static final String NO_COURIER_SUFFIX = "-no-courier";

  private record Key(String tenantId, String orderId) {}

  private final Set<Key> booked = new HashSet<>();
  private final OutboxWriter outbox;
  private final TransactionTemplate tx;
  private final TaskScheduler scheduler;
  private final Duration demoStep;

  SimulatedCarrier(
      OutboxWriter outbox,
      TransactionTemplate tx,
      TaskScheduler scheduler,
      @Value("${orderloop.simulator.demo-step:5s}") Duration demoStep) {
    this.outbox = outbox;
    this.tx = tx;
    this.scheduler = scheduler;
    this.demoStep = demoStep;
  }

  @Override
  public synchronized BookingResult book(String tenantId, String orderId) {
    if (orderId.endsWith(NO_COURIER_SUFFIX)) {
      return new BookingResult.Failed("NO_COURIER");
    }
    // A repeat books the same shipment, so a demo order plays out once.
    if (booked.add(new Key(tenantId, orderId))) {
      DemoOrder.of(orderId).ifPresent(demo -> playOut(tenantId, orderId, demo.outcome()));
    }
    return new BookingResult.Booked();
  }

  // Reported later on the scheduler's thread, as a carrier's webhooks arrive. One that lands before
  // the booking reply is an early carrier event, which the saga accepts.
  private void playOut(String tenantId, String orderId, DemoOrder.Outcome outcome) {
    switch (outcome) {
      case DELIVERED -> later(1, () -> deliver(tenantId, orderId));
      case RTO -> {
        later(1, () -> startRto(tenantId, orderId));
        later(2, () -> receiveRto(tenantId, orderId));
      }
    }
  }

  private void later(int steps, Runnable report) {
    scheduler.schedule(report, scheduler.getClock().instant().plus(demoStep.multipliedBy(steps)));
  }

  @Override
  public void requestReattempt(String tenantId, String orderId) {
    requireBooked(tenantId, orderId); // The outcome arrives later, as deliver or failDelivery.
  }

  public void deliver(String tenantId, String orderId) {
    report(tenantId, new DeliveredEvt(orderId));
  }

  /** {@code reason} is a code such as {@code CUSTOMER_UNAVAILABLE}, never free text. */
  public void failDelivery(String tenantId, String orderId, String reason) {
    report(tenantId, new DeliveryFailedEvt(orderId, reason));
  }

  public void startRto(String tenantId, String orderId) {
    report(tenantId, new RtoStartedEvt(orderId));
  }

  public void receiveRto(String tenantId, String orderId) {
    report(tenantId, new RtoReceivedEvt(orderId));
  }

  private void report(String tenantId, SagaEvent event) {
    requireBooked(tenantId, event.orderId());
    TenantContext.callAs(
        tenantId,
        () ->
            tx.execute(
                status -> {
                  outbox.append(
                      Topics.FULFILMENT_EVENTS,
                      new Envelope(UUID.randomUUID(), tenantId, null, event));
                  return null;
                }));
  }

  private synchronized void requireBooked(String tenantId, String orderId) {
    if (!booked.contains(new Key(tenantId, orderId))) {
      throw new IllegalStateException("No shipment booked for order " + orderId);
    }
  }
}
