package app.orderloop.platform.tenant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import app.orderloop.platform.tenant.TenantSecurityProperties.DevApiKey;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

class DevApiKeyAuthenticationProviderTest {

  private static final String ALPHA = "alpha-dev-key-0123456789";
  private static final String BETA = "beta-dev-key-0123456789";

  private final DevApiKeyAuthenticationProvider provider =
      new DevApiKeyAuthenticationProvider(
          List.of(new DevApiKey("t_alpha", ALPHA), new DevApiKey("t_beta", BETA)));

  @Test
  void aKnownKeyAuthenticatesAsItsTenant() {
    var result = provider.authenticate(new PreAuthenticatedAuthenticationToken("X-Api-Key", BETA));

    assertThat(result.isAuthenticated()).isTrue();
    assertThat(result.getPrincipal()).isEqualTo(new TenantPrincipal("t_beta"));
  }

  @Test
  void anUnknownKeyIsRejected() {
    assertThatThrownBy(
            () ->
                provider.authenticate(
                    new PreAuthenticatedAuthenticationToken("X-Api-Key", ALPHA + "x")))
        .isInstanceOf(BadCredentialsException.class);
  }

  @Test
  void refusesShortKeysAndDuplicatesAtStartup() {
    assertThatThrownBy(
            () -> new DevApiKeyAuthenticationProvider(List.of(new DevApiKey("t_a", "short"))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new DevApiKeyAuthenticationProvider(
                    List.of(new DevApiKey("t_a", ALPHA), new DevApiKey("t_b", ALPHA))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new DevApiKeyAuthenticationProvider(
                    List.of(new DevApiKey("t_a", ALPHA), new DevApiKey("t_a", BETA))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new DevApiKeyAuthenticationProvider(List.of(new DevApiKey(" ", ALPHA))))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
