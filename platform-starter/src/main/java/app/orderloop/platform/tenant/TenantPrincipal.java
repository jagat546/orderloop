package app.orderloop.platform.tenant;

/** The authenticated principal of every credential type: who the caller is, as a tenant. */
public record TenantPrincipal(String tenantId) {}
