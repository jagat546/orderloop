package app.orderloop.platform.tenant;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Authenticates the {@code X-Api-Key} header. A missing key is left for authorization to reject.
 */
final class ApiKeyAuthenticationFilter extends OncePerRequestFilter {

  static final String API_KEY_HEADER = "X-Api-Key";

  private final DevApiKeyAuthenticationProvider provider;
  private final AuthenticationEntryPoint unauthenticated;

  ApiKeyAuthenticationFilter(
      DevApiKeyAuthenticationProvider provider, AuthenticationEntryPoint unauthenticated) {
    this.provider = provider;
    this.unauthenticated = unauthenticated;
  }

  @Override
  protected void doFilterInternal(
      HttpServletRequest request, HttpServletResponse response, FilterChain chain)
      throws ServletException, IOException {
    String key = request.getHeader(API_KEY_HEADER);
    if (key != null) {
      var holder = SecurityContextHolder.getContextHolderStrategy();
      try {
        var context = holder.createEmptyContext();
        context.setAuthentication(
            provider.authenticate(new PreAuthenticatedAuthenticationToken(API_KEY_HEADER, key)));
        holder.setContext(context);
      } catch (AuthenticationException e) {
        holder.clearContext();
        unauthenticated.commence(request, response, e);
        return;
      }
    }
    chain.doFilter(request, response);
  }
}
