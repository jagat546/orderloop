package app.orderloop.platform.tenant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

/**
 * Platform threads are reused, unlike virtual threads, so cleanup must not depend on a new thread.
 */
class TenantBindingFilterTest {

  private final TenantBindingFilter filter = new TenantBindingFilter();

  @AfterEach
  void clear() {
    SecurityContextHolder.clearContext();
    MDC.clear();
  }

  @Test
  void bindsTheTenantAndMdcOnlyForTheRequest() throws Exception {
    authenticateAs(new TenantPrincipal("t_a"));
    AtomicReference<String> seen = new AtomicReference<>();

    filter.doFilter(
        new MockHttpServletRequest(),
        new MockHttpServletResponse(),
        (req, res) -> seen.set(TenantContext.require() + "/" + MDC.get("tenantId")));

    assertThat(seen.get()).isEqualTo("t_a/t_a");
    assertThat(MDC.get("tenantId")).isNull();
    assertThat(TenantContext.current()).isEmpty();
  }

  @Test
  void cleansUpWhenTheRequestThrows() {
    authenticateAs(new TenantPrincipal("t_a"));

    assertThatThrownBy(
            () ->
                filter.doFilter(
                    new MockHttpServletRequest(),
                    new MockHttpServletResponse(),
                    (req, res) -> {
                      throw new IllegalStateException("handler failed");
                    }))
        .hasMessage("handler failed");

    assertThat(MDC.get("tenantId")).isNull();
    assertThat(TenantContext.current()).isEmpty();
  }

  @Test
  void anAuthenticatedPrincipalWithoutATenantGets403() throws Exception {
    authenticateAs("not-a-tenant-principal");
    var response = new MockHttpServletResponse();
    AtomicReference<Boolean> reached = new AtomicReference<>(false);

    filter.doFilter(new MockHttpServletRequest(), response, (req, res) -> reached.set(true));

    assertThat(response.getStatus()).isEqualTo(403);
    assertThat(response.getContentAsString()).contains("tenant-unresolved");
    assertThat(reached.get()).isFalse();
  }

  private static void authenticateAs(Object principal) {
    var context = SecurityContextHolder.createEmptyContext();
    context.setAuthentication(new PreAuthenticatedAuthenticationToken(principal, null, List.of()));
    SecurityContextHolder.setContext(context);
  }
}
