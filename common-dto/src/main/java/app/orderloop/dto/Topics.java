package app.orderloop.dto;

import java.util.List;

/**
 * Kafka topics, all keyed by {@code orderId}. Each has a dead-letter topic named {@code
 * <topic>.dlt}. Commands go to the service that acts on them; every reply goes back on its
 * service's events topic, which only the orchestrator reads.
 */
public final class Topics {

  public static final String INVENTORY_COMMANDS = "orderloop.inventory-commands";
  public static final String PAYMENT_COMMANDS = "orderloop.payment-commands";
  public static final String FULFILMENT_COMMANDS = "orderloop.fulfilment-commands";
  public static final String INVENTORY_EVENTS = "orderloop.inventory-events";
  public static final String PAYMENT_EVENTS = "orderloop.payment-events";
  public static final String FULFILMENT_EVENTS = "orderloop.fulfilment-events";

  public static final List<String> ALL =
      List.of(
          INVENTORY_COMMANDS,
          PAYMENT_COMMANDS,
          FULFILMENT_COMMANDS,
          INVENTORY_EVENTS,
          PAYMENT_EVENTS,
          FULFILMENT_EVENTS);

  private Topics() {}

  public static String deadLetter(String topic) {
    return topic + ".dlt";
  }

  /** The topic a command travels on: the service that acts on it. */
  public static String of(SagaCommand command) {
    return switch (command) {
      case SagaCommand.ReserveInventoryCmd c -> INVENTORY_COMMANDS;
      case SagaCommand.ReleaseInventoryCmd c -> INVENTORY_COMMANDS;
      case SagaCommand.RestockInventoryCmd c -> INVENTORY_COMMANDS;
      case SagaCommand.ProcessPaymentCmd c -> PAYMENT_COMMANDS;
      case SagaCommand.RefundPaymentCmd c -> PAYMENT_COMMANDS;
      case SagaCommand.BookShipmentCmd c -> FULFILMENT_COMMANDS;
      case SagaCommand.ReattemptDeliveryCmd c -> FULFILMENT_COMMANDS;
    };
  }
}
