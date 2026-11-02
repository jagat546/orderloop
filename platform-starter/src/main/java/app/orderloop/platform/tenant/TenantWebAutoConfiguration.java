package app.orderloop.platform.tenant;

import jakarta.servlet.DispatcherType;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication.Type;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.security.autoconfigure.web.servlet.ServletWebSecurityAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.AuthenticationEntryPoint;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.access.intercept.AuthorizationFilter;

/**
 * Tenant-aware security for services that serve HTTP. Every request needs a verified credential;
 * the tenant comes only from that credential (invariant 1).
 *
 * <p>The filters are created here, not as beans, so Spring Boot does not also register them as
 * servlet filters outside the security chain.
 */
@AutoConfiguration(before = ServletWebSecurityAutoConfiguration.class)
@ConditionalOnWebApplication(type = Type.SERVLET)
@ConditionalOnClass(SecurityFilterChain.class)
@EnableConfigurationProperties(TenantSecurityProperties.class)
public class TenantWebAutoConfiguration {

  @Bean
  DevApiKeyAuthenticationProvider devApiKeyAuthenticationProvider(
      TenantSecurityProperties properties) {
    return new DevApiKeyAuthenticationProvider(properties.activeDevApiKeys());
  }

  @Bean
  SecurityFilterChain tenantSecurityFilterChain(
      HttpSecurity http, DevApiKeyAuthenticationProvider apiKeys) throws Exception {
    AuthenticationEntryPoint unauthenticated =
        (request, response, e) ->
            ProblemResponses.write(
                response, 401, "unauthenticated", "A valid credential is required");

    http.csrf(AbstractHttpConfigurer::disable)
        .httpBasic(AbstractHttpConfigurer::disable)
        .formLogin(AbstractHttpConfigurer::disable)
        .logout(AbstractHttpConfigurer::disable)
        .anonymous(AbstractHttpConfigurer::disable)
        .requestCache(AbstractHttpConfigurer::disable)
        .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
        .authorizeHttpRequests(
            requests ->
                requests
                    .dispatcherTypeMatchers(DispatcherType.ERROR)
                    .permitAll()
                    .anyRequest()
                    .authenticated())
        .exceptionHandling(
            errors ->
                errors
                    .authenticationEntryPoint(unauthenticated)
                    .accessDeniedHandler(
                        (request, response, e) ->
                            ProblemResponses.write(response, 403, "forbidden", "Access denied")))
        .addFilterBefore(
            new ApiKeyAuthenticationFilter(apiKeys, unauthenticated), AuthorizationFilter.class)
        .addFilterAfter(new TenantBindingFilter(), AuthorizationFilter.class);
    return http.build();
  }
}
