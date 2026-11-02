package app.orderloop.dto;

import static java.util.stream.Collectors.toUnmodifiableMap;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Registry of message type names, sent in the {@code x-event-type} header. Built from the sealed
 * types, so adding a record registers it. Duplicate simple names fail at class load.
 */
public final class MessageTypes {

  /**
   * shortcut: every type is at version 1. When the first breaking change arrives, add the new type
   * and keep an explicit version per type here.
   */
  public static final int SCHEMA_VERSION = 1;

  private static final Map<String, Class<? extends Message>> BY_NAME =
      Stream.of(SagaCommand.class, SagaEvent.class)
          .flatMap(type -> Arrays.stream(type.getPermittedSubclasses()))
          .collect(toUnmodifiableMap(Class::getSimpleName, type -> type.asSubclass(Message.class)));

  private MessageTypes() {}

  public static Set<String> names() {
    return BY_NAME.keySet();
  }

  public static String nameOf(Class<? extends Message> type) {
    String name = type.getSimpleName();
    if (BY_NAME.get(name) != type) {
      throw new IllegalArgumentException("Not a registered message type: " + type.getName());
    }
    return name;
  }

  public static Class<? extends Message> classOf(String name) {
    Class<? extends Message> type = BY_NAME.get(name);
    if (type == null) {
      throw new IllegalArgumentException("Unknown message type: " + name);
    }
    return type;
  }
}
