package app.orderloop.platform.tenant;

import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Development API keys, one per tenant. Off unless switched on explicitly:
 *
 * <pre>
 * orderloop.security.dev-api-keys-enabled=true
 * orderloop.security.dev-api-keys[0].tenant=t_demo
 * orderloop.security.dev-api-keys[0].key=at-least-16-characters
 * </pre>
 *
 * Keys configured without the switch stop startup, so keys copied into a production environment by
 * accident never authenticate silently. Shopify session tokens replace these in M3.1.
 */
@ConfigurationProperties("orderloop.security")
public record TenantSecurityProperties(boolean devApiKeysEnabled, List<DevApiKey> devApiKeys) {

  public TenantSecurityProperties {
    devApiKeys = devApiKeys == null ? List.of() : List.copyOf(devApiKeys);
    if (!devApiKeysEnabled && !devApiKeys.isEmpty()) {
      throw new IllegalStateException(
          "orderloop.security.dev-api-keys is set but orderloop.security.dev-api-keys-enabled is"
              + " not true. Remove the keys, or switch them on for development only.");
    }
  }

  /** The keys that may authenticate: none unless switched on. */
  public List<DevApiKey> activeDevApiKeys() {
    return devApiKeysEnabled ? devApiKeys : List.of();
  }

  public record DevApiKey(String tenant, String key) {}
}
