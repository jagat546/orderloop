package app.orderloop.dto;

/** Kafka header names, from .claude/rules/messaging.md. */
public final class Headers {

  public static final String TENANT_ID = "x-tenant-id";
  public static final String EVENT_ID = "x-event-id";
  public static final String EVENT_TYPE = "x-event-type";
  public static final String SCHEMA_VERSION = "x-schema-version";
  public static final String CAUSATION_ID = "x-causation-id";
  public static final String TRACEPARENT = "traceparent";

  private Headers() {}
}
