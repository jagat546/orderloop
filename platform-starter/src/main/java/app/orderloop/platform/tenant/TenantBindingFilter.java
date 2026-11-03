package app.orderloop.platform.tenant;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Runs after authorization. Takes the tenant from the authenticated principal, rejects a mismatched
 * {@code X-Tenant-ID} with 403 (invariant 1), then runs the rest of the request inside {@link
 * TenantContext}, which copies the tenant to MDC for logs.
 */
final class TenantBindingFilter extends OncePerRequestFilter {

  static final String TENANT_HEADER = "X-Tenant-ID";

  private static final Logger log = LoggerFactory.getLogger(TenantBindingFilter.class);

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    Authentication auth =
        SecurityContextHolder.getContextHolderStrategy().getContext().getAuthentication();
    if (auth == null
        || !auth.isAuthenticated()
        || !(auth.getPrincipal() instanceof TenantPrincipal(String tenantId))) {
      // Fail closed: an authenticated request that names no tenant gets nothing.
      ProblemResponses.write(response, 403, "tenant-unresolved", "No tenant for this credential");
      return;
    }

    String claimed = request.getHeader(TENANT_HEADER);
    if (claimed != null && !claimed.equals(tenantId)) {
      // Log only the verified tenant: the claimed value is client input and may be another tenant.
      log.warn("Rejected request whose tenant header does not match tenant {}", tenantId);
      ProblemResponses.write(
          response, 403, "tenant-mismatch", "The tenant header does not match the credential");
      return;
    }

    try {
      TenantContext.callAs(
          tenantId,
          () -> {
            chain.doFilter(request, response);
            return null;
          });
    } catch (IOException | ServletException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new ServletException(e);
    }
  }
}
