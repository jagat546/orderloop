package app.orderloop.platform.tenant;

import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * RFC 9457 problem responses for security filters, which run before any
 * {@code @RestControllerAdvice}. Every argument is a constant, never client input.
 */
final class ProblemResponses {

  private ProblemResponses() {}

  static void write(HttpServletResponse response, int status, String problem, String title)
      throws IOException {
    response.setStatus(status);
    response.setContentType("application/problem+json");
    response.setCharacterEncoding(StandardCharsets.UTF_8.name());
    response
        .getWriter()
        .write(
            "{\"type\":\"urn:orderloop:problem:"
                + problem
                + "\",\"title\":\""
                + title
                + "\",\"status\":"
                + status
                + "}");
  }
}
