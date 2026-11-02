package app.orderloop.platform.tenant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import app.orderloop.platform.tenant.TenantSecurityProperties.DevApiKey;
import java.util.List;
import org.junit.jupiter.api.Test;

class TenantSecurityPropertiesTest {

  private static final List<DevApiKey> KEYS = List.of(new DevApiKey("t_a", "a-dev-key-0123456789"));

  @Test
  void devKeysAreOffByDefault() {
    assertThat(new TenantSecurityProperties(false, null).activeDevApiKeys()).isEmpty();
  }

  @Test
  void configuredKeysWithoutTheSwitchStopStartup() {
    assertThatThrownBy(() -> new TenantSecurityProperties(false, KEYS))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("dev-api-keys-enabled");
  }

  @Test
  void keysApplyOnlyWhenSwitchedOn() {
    assertThat(new TenantSecurityProperties(true, KEYS).activeDevApiKeys()).isEqualTo(KEYS);
  }
}
