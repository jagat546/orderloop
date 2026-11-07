package app.orderloop.architecture;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Roadmap M1.14b: the Grafana board queries what the services emit. Metric names are the Micrometer
 * names as Prometheus stores them from OTLP: dots become underscores, counters gain {@code _total},
 * and a gauge in seconds gains {@code _seconds}.
 */
class TelemetryBoardTest {

  private static final Path BOARD =
      Path.of("..", "deploy", "telemetry", "grafana", "dashboards", "orderloop.json");

  @Test
  void theBoardShowsSagaStatesOutboxAgeMissedDeadlinesAndDeadLettersPerService() throws Exception {
    List<String> queries = queries();

    assertThat(queries).anyMatch(q -> q.contains("orderloop_saga_transitions_total"));
    // Per service: grouped by job, which Prometheus fills from each service's name.
    assertThat(queries)
        .anyMatch(q -> q.contains("orderloop_outbox_oldest_age_seconds") && q.contains("job"));
    assertThat(queries).anyMatch(q -> q.contains("orderloop_outbox_backlog") && q.contains("job"));
    assertThat(queries)
        .anyMatch(q -> q.contains("orderloop_saga_deadlines_missed_total") && q.contains("job"));
    assertThat(queries)
        .anyMatch(q -> q.contains("orderloop_inbox_dead_letters_total") && q.contains("job"));
  }

  @Test
  void theBoardFindsAnOrdersTracesByTheAttributeConsumerSpansCarry() throws Exception {
    // InboxConsumer tags each consumer span with order.id.
    assertThat(queries()).contains("{ span.order.id = \"$order\" }");
  }

  private static List<String> queries() throws Exception {
    JsonNode board = JsonMapper.builder().build().readTree(Files.readString(BOARD));
    assertThat(board.get("uid").asString()).isEqualTo("orderloop");
    List<String> queries = new ArrayList<>();
    for (JsonNode panel : board.get("panels")) {
      for (JsonNode target : panel.get("targets")) {
        JsonNode query = target.has("expr") ? target.get("expr") : target.get("query");
        queries.add(query.asString());
      }
    }
    return queries;
  }
}
