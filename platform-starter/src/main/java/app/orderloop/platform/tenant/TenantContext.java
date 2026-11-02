package app.orderloop.platform.tenant;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * The verified tenant for the current scope (invariant 3). Bound only from a verified credential,
 * never from a client header. A scoped value does not reach a thread started inside the scope, so
 * pass the tenant explicitly to other threads and rebind it there. A scope never switches to
 * another tenant; finish one tenant's work before starting the next.
 */
public final class TenantContext {

  private static final ScopedValue<String> TENANT = ScopedValue.newInstance();
  // Opaque IDs such as t_01J...; never a shop domain. Rejects stray spaces and look-alikes.
  private static final Pattern TENANT_ID = Pattern.compile("t_[A-Za-z0-9]{1,64}");

  private TenantContext() {}

  /** Whether a value has the shape of a tenant ID. Says nothing about whether the tenant exists. */
  public static boolean isValidTenantId(String tenantId) {
    return tenantId != null && TENANT_ID.matcher(tenantId).matches();
  }

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
    if (!isValidTenantId(tenantId)) {
      throw new IllegalArgumentException("tenantId must look like t_01J..., not blank or padded");
    }
    // A transaction joined inside a nested scope would keep the outer tenant's connection and data.
    if (TENANT.isBound() && !TENANT.get().equals(tenantId)) {
      throw new IllegalStateException("Cannot switch tenant inside another tenant's scope");
    }
    return ScopedValue.where(TENANT, tenantId).call(op);
  }
}
