package app.orderloop.platform.tenant;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Import;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootTest(
    classes = TenantWebTest.ProbeApp.class,
    webEnvironment = WebEnvironment.RANDOM_PORT,
    properties = {
      "spring.threads.virtual.enabled=true",
      "orderloop.security.dev-api-keys-enabled=true",
      "orderloop.security.dev-api-keys[0].tenant=t_alpha",
      "orderloop.security.dev-api-keys[0].key=alpha-dev-key-0123456789",
      "orderloop.security.dev-api-keys[1].tenant=t_beta",
      "orderloop.security.dev-api-keys[1].key=beta-dev-key-0123456789"
    })
class TenantWebTest {

  private static final String ALPHA_KEY = "alpha-dev-key-0123456789";

  private final HttpClient http = HttpClient.newHttpClient();

  @Value("${local.server.port}")
  private int port;

  @Test
  void aMissingCredentialReturns401AsProblemDetail() throws Exception {
    HttpResponse<String> response = get(null, null);

    assertThat(response.statusCode()).isEqualTo(401);
    assertThat(response.headers().firstValue("Content-Type"))
        .hasValueSatisfying(type -> assertThat(type).startsWith("application/problem+json"));
    assertThat(response.body()).contains("\"status\":401");
  }

  @Test
  void anUnknownApiKeyReturns401() throws Exception {
    assertThat(get("not-a-real-key-0123456789", null).statusCode()).isEqualTo(401);
  }

  @Test
  void aTenantHeaderAloneIsNotACredential() throws Exception {
    assertThat(get(null, "t_alpha").statusCode()).isEqualTo(401);
  }

  @Test
  void aValidKeyBindsItsTenantForTheRequestOnAVirtualThread() throws Exception {
    HttpResponse<String> response = get(ALPHA_KEY, null);

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEqualTo("tenant=t_alpha virtual=true");
  }

  @Test
  void eachKeyBindsItsOwnTenant() throws Exception {
    HttpResponse<String> response = get("beta-dev-key-0123456789", "t_beta");

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEqualTo("tenant=t_beta virtual=true");
  }

  @Test
  void aMatchingTenantHeaderIsAccepted() throws Exception {
    HttpResponse<String> response = get(ALPHA_KEY, "t_alpha");

    assertThat(response.statusCode()).isEqualTo(200);
    assertThat(response.body()).isEqualTo("tenant=t_alpha virtual=true");
  }

  @Test
  void aMismatchedTenantHeaderReturns403WithoutEchoingTheOtherTenant() throws Exception {
    HttpResponse<String> response = get(ALPHA_KEY, "t_beta");

    assertThat(response.statusCode()).isEqualTo(403);
    assertThat(response.headers().firstValue("Content-Type"))
        .hasValueSatisfying(type -> assertThat(type).startsWith("application/problem+json"));
    assertThat(response.body()).contains("\"status\":403").doesNotContain("t_beta");
  }

  private HttpResponse<String> get(String apiKey, String tenantHeader) throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/probe"));
    if (apiKey != null) {
      request.header("X-Api-Key", apiKey);
    }
    if (tenantHeader != null) {
      request.header("X-Tenant-ID", tenantHeader);
    }
    return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  @Import(ProbeController.class)
  static class ProbeApp {}

  @RestController
  static class ProbeController {

    @GetMapping("/probe")
    String probe() {
      return "tenant=" + TenantContext.require() + " virtual=" + Thread.currentThread().isVirtual();
    }
  }
}
