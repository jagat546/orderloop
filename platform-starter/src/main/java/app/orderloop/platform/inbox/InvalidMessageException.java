package app.orderloop.platform.inbox;

/**
 * A record that can never be handled: missing tenant or event ID, wrong type, unreadable payload.
 * It is dead-lettered at once, without retries.
 */
public final class InvalidMessageException extends RuntimeException {

  InvalidMessageException(String message) {
    super(message);
  }

  InvalidMessageException(String message, Throwable cause) {
    super(message, cause);
  }
}
