package app.orderloop.platform.tenant;

import org.hibernate.context.spi.CurrentTenantIdentifierResolver;

/**
 * Feeds Hibernate {@code @TenantId} from {@link TenantContext}. A convenience only: row-level
 * security is the boundary (invariant 2). Unbound, it returns an empty identifier, which the
 * database policy matches to no row and refuses to write.
 */
final class TenantIdentifierResolver implements CurrentTenantIdentifierResolver<String> {

  static final String NO_TENANT = "";

  @Override
  public String resolveCurrentTenantIdentifier() {
    return TenantContext.current().orElse(NO_TENANT);
  }

  @Override
  public boolean validateExistingCurrentSessions() {
    return false;
  }
}
