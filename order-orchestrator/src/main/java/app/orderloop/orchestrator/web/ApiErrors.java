package app.orderloop.orchestrator.web;

import java.net.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

/**
 * Every API error as an RFC 9457 problem with a stable type, {@code urn:orderloop:problem:<status>}
 * such as {@code bad-request}, matching the security filters. No stack traces, SQL or exception
 * messages reach the client.
 */
@RestControllerAdvice
class ApiErrors extends ResponseEntityExceptionHandler {

  private static final Logger log = LoggerFactory.getLogger(ApiErrors.class);

  @ExceptionHandler(Exception.class)
  ResponseEntity<Object> unexpected(Exception e, WebRequest request) {
    log.error("Unhandled {} in API request", e.getClass().getName(), e);
    HttpStatus status = HttpStatus.INTERNAL_SERVER_ERROR;
    return handleExceptionInternal(
        e, ProblemDetail.forStatus(status), new HttpHeaders(), status, request);
  }

  @Override
  protected ResponseEntity<Object> handleExceptionInternal(
      Exception e, Object body, HttpHeaders headers, HttpStatusCode status, WebRequest request) {
    // Spring passes no body for its own exceptions and builds the problem in super, so set the
    // type on what comes back.
    ResponseEntity<Object> response =
        super.handleExceptionInternal(e, body, headers, status, request);
    if (response != null && response.getBody() instanceof ProblemDetail problem) {
      problem.setType(URI.create("urn:orderloop:problem:" + slug(status)));
      problem.setDetail(null); // Spring's details can quote the request; titles are enough.
    }
    return response;
  }

  private static String slug(HttpStatusCode status) {
    HttpStatus known = HttpStatus.resolve(status.value());
    return known == null
        ? Integer.toString(status.value())
        : known.name().toLowerCase().replace('_', '-');
  }
}
