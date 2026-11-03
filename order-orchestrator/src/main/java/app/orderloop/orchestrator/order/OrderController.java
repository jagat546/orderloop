package app.orderloop.orchestrator.order;

import app.orderloop.dto.Money;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import java.net.URI;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.ErrorResponseException;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Development order API: starts a saga without Shopify. It exists only while development API keys
 * are switched on; real orders arrive through the gateway's webhooks. Creating an order again is
 * safe: the order ID is the idempotency key, and a repeat returns 200 with the saga unchanged.
 */
@RestController
@RequestMapping("/api/v1/orders")
@ConditionalOnProperty(name = "orderloop.security.dev-api-keys-enabled", havingValue = "true")
class OrderController {

  private final SagaService sagas;

  OrderController(SagaService sagas) {
    this.sagas = sagas;
  }

  record CreateOrderRequest(
      @NotNull @Pattern(regexp = "[A-Za-z0-9_-]{1,64}") String orderId, @NotNull Money total) {

    @AssertTrue(message = "total must not be negative")
    public boolean isTotalNotNegative() {
      return total == null || total.amountMinor() >= 0;
    }
  }

  @PostMapping
  ResponseEntity<SagaView> create(@Valid @RequestBody CreateOrderRequest request) {
    SagaService.Created created = sagas.create(request.orderId(), request.total());
    return created.isNew()
        ? ResponseEntity.created(URI.create("/api/v1/orders/" + request.orderId()))
            .body(created.saga())
        : ResponseEntity.ok(created.saga());
  }

  @GetMapping("/{orderId}")
  SagaView get(@PathVariable String orderId) {
    // Another tenant's order is as absent as one that never existed.
    return sagas.find(orderId).orElseThrow(() -> new ErrorResponseException(HttpStatus.NOT_FOUND));
  }
}
