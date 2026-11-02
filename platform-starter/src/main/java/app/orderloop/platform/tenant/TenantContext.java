package app.orderloop.platform.tenant;

import java.util.Optional;

/**
 * The verified tenant for the current scope (invariant 3). Bound only from a verified credential,
 * never from a client header. A scoped value does not reach a thread started inside the scope, so
 * pass the tenant explicitly to other threads and rebind it there. A scope never switches to
 * another tenant; finish one tenant's work before starting the next.
 */
public final class TenantContext {

  private static final ScopedValue<String> TENANT = ScopedValue.newInstance();

  private TenantContext() {}

  public static String require() {
    if (!TENANT.isBound()) {
      throw new IllegalStateException("No tenant bound to this scope");
    }
    return TENANT.get();
  }

  public static Optional<String> current() {
    return TENANT.isBound() ? Optional.of(TENANT.get()) : Optional.empty();
  }

  public static <R, X extends Throwable> R callAs(String tenantId, ScopedValue.CallableOp<R, X> op)
      throws X {
    if (tenantId == null || tenantId.isBlank()) {
      throw new IllegalArgumentException("tenantId is required");
    }
    // A transaction joined inside a nested scope would keep the outer tenant's connection and data.
    if (TENANT.isBound() && !TENANT.get().equals(tenantId)) {
      throw new IllegalStateException("Cannot switch tenant inside another tenant's scope");
    }
    return ScopedValue.where(TENANT, tenantId).call(op);
  }
}
