package app.orderloop.orchestrator.order;

import app.orderloop.orchestrator.saga.SagaState;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import tools.jackson.databind.json.JsonMapper;

/**
 * Stores a {@link SagaState} as JSON. The engine stays free of Jackson (ArchUnit), so the type name
 * is added here through a mix-in rather than an annotation on the states.
 */
final class SagaStateJson {

  @JsonTypeInfo(use = JsonTypeInfo.Id.SIMPLE_NAME, property = "type")
  private interface Typed {}

  private static final JsonMapper JSON =
      JsonMapper.builder()
          .addMixIn(SagaState.class, Typed.class)
          .registerSubtypes(SagaState.class.getPermittedSubclasses())
          .build();

  private SagaStateJson() {}

  static String write(SagaState state) {
    return JSON.writerFor(SagaState.class).writeValueAsString(state);
  }

  static SagaState read(String json) {
    return JSON.readValue(json, SagaState.class);
  }

  static String name(SagaState state) {
    return state.getClass().getSimpleName();
  }
}
