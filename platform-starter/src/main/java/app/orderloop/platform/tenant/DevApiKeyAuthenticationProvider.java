package app.orderloop.platform.tenant;

import static java.nio.charset.StandardCharsets.UTF_8;

import app.orderloop.platform.tenant.TenantSecurityProperties.DevApiKey;
import java.security.MessageDigest;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.core.Authentication;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

/**
 * Turns a development API key into a {@link TenantPrincipal}. Being an {@link
 * AuthenticationProvider} bean also stops Spring Boot creating its default in-memory user.
 */
public final class DevApiKeyAuthenticationProvider implements AuthenticationProvider {

  static final int MIN_KEY_LENGTH = 16;

  private final Map<String, byte[]> keyByTenant = new LinkedHashMap<>();

  public DevApiKeyAuthenticationProvider(List<DevApiKey> keys) {
    var seen = new HashSet<String>();
    for (DevApiKey entry : keys) {
      if (!TenantContext.isValidTenantId(entry.tenant())) {
        throw new IllegalArgumentException("Every dev API key needs a tenant ID like t_01J...");
      }
      if (entry.key() == null || entry.key().length() < MIN_KEY_LENGTH) {
        throw new IllegalArgumentException(
            "Dev API key for " + entry.tenant() + " must have at least 16 characters");
      }
      if (!seen.add(entry.key()) || keyByTenant.containsKey(entry.tenant())) {
        throw new IllegalArgumentException("Dev API keys and tenants must each be unique");
      }
      keyByTenant.put(entry.tenant(), entry.key().getBytes(UTF_8));
    }
  }

  @Override
  public Authentication authenticate(Authentication request) {
    byte[] presented = String.valueOf(request.getCredentials()).getBytes(UTF_8);
    String match = null;
    // Compare against every key, without stopping early, so timing does not reveal a match.
    for (var entry : keyByTenant.entrySet()) {
      if (MessageDigest.isEqual(entry.getValue(), presented)) {
        match = entry.getKey();
      }
    }
    if (match == null) {
      throw new BadCredentialsException("Unknown API key");
    }
    return new PreAuthenticatedAuthenticationToken(new TenantPrincipal(match), null, List.of());
  }

  @Override
  public boolean supports(Class<?> authentication) {
    return PreAuthenticatedAuthenticationToken.class.isAssignableFrom(authentication);
  }
}
