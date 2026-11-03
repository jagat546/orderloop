package app.orderloop.orchestrator.order;

import app.orderloop.dto.Money;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.Immutable;
import org.hibernate.annotations.TenantId;

/**
 * One order's saga row, read through JPA. {@link SagaService} writes it with explicit SQL: an
 * insert that ignores a repeat, and an update that moves {@code version} on by one only if it is
 * still the version that was read. That always bumps the version, even when a transition re-enters
 * an identical state, which Hibernate's dirty check would skip.
 */
@Entity
@Immutable
@Table(schema = "orchestrator", name = "order_saga")
class OrderSaga {

  @Id
  @Column(name = "order_id")
  private String orderId;

  @TenantId
  @Column(name = "tenant_id")
  private String tenantId;

  @Column(name = "state")
  private String state;

  @Column(name = "state_data")
  private String stateData;

  @Column(name = "version")
  private long version;

  @Column(name = "order_total_minor")
  private long orderTotalMinor;

  @Column(name = "currency")
  private String currency;

  protected OrderSaga() {}

  String orderId() {
    return orderId;
  }

  String state() {
    return state;
  }

  String stateData() {
    return stateData;
  }

  long version() {
    return version;
  }

  Money total() {
    return new Money(orderTotalMinor, currency);
  }
}
