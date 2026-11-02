package app.orderloop.platform.tenant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class TenantContextTest {

  @Test
  void requireThrowsWhenNoTenantIsBound() {
    assertThat(TenantContext.current()).isEmpty();
    assertThatThrownBy(TenantContext::require).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void callAsBindsTheTenantOnlyInsideTheScope() {
    String inside = TenantContext.callAs("t_a", TenantContext::require);

    assertThat(inside).isEqualTo("t_a");
    assertThat(TenantContext.current()).isEmpty();
  }

  @Test
  void switchingToAnotherTenantInsideAScopeIsRefused() {
    // A transaction joined inside the nested scope would keep the outer tenant's connection.
    assertThatThrownBy(
            () -> TenantContext.callAs("t_a", () -> TenantContext.callAs("t_b", () -> "never")))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void rebindingTheSameTenantIsAllowed() {
    List<String> seen =
        TenantContext.callAs(
            "t_a",
            () ->
                List.of(
                    TenantContext.callAs("t_a", TenantContext::require), TenantContext.require()));

    assertThat(seen).containsExactly("t_a", "t_a");
  }

  @ParameterizedTest
  @ValueSource(strings = {" t_a", "t_a ", "T_a", "shop.myshopify.com", "t_", "t_a/b"})
  void refusesToBindAMalformedTenant(String tenantId) {
    assertThat(TenantContext.isValidTenantId(tenantId)).isFalse();
    assertThatThrownBy(() -> TenantContext.callAs(tenantId, () -> "never"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = " ")
  void refusesToBindABlankTenant(String tenantId) {
    assertThatThrownBy(() -> TenantContext.callAs(tenantId, () -> "never"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void eachVirtualThreadSeesOnlyItsOwnTenant() throws Exception {
    int threads = 10_000;
    List<Future<String>> results = new ArrayList<>();

    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < threads; i++) {
        String tenant = "t_" + i;
        results.add(
            executor.submit(
                () ->
                    TenantContext.callAs(
                        tenant,
                        () -> {
                          // Yield so virtual threads unmount and resume on other carrier threads.
                          Thread.yield();
                          if (!Thread.currentThread().isVirtual()) {
                            return "not virtual";
                          }
                          return TenantContext.require();
                        })));
      }
    }

    for (int i = 0; i < threads; i++) {
      assertThat(results.get(i).get()).isEqualTo("t_" + i);
    }
  }

  @Test
  void aThreadStartedInsideAScopeDoesNotInheritTheTenant() throws Exception {
    AtomicReference<Optional<String>> seenByChild = new AtomicReference<>();

    TenantContext.callAs(
        "t_a",
        () -> {
          Thread child = Thread.ofVirtual().start(() -> seenByChild.set(TenantContext.current()));
          child.join();
          return null;
        });

    assertThat(seenByChild.get()).isEmpty();
  }
}
