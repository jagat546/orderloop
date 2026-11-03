package app.orderloop.orchestrator.order;

import app.orderloop.dto.Money;

/** An order's saga as the API shows it. */
public record SagaView(String orderId, String state, Money total) {}
