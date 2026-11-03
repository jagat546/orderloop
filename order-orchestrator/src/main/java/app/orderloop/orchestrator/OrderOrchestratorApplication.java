package app.orderloop.orchestrator;

import app.orderloop.dto.Topics;
import java.time.Clock;
import java.util.stream.Stream;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@SpringBootApplication
public class OrderOrchestratorApplication {

  public static void main(String[] args) {
    SpringApplication.run(OrderOrchestratorApplication.class, args);
  }

  @Bean
  Clock clock() {
    return Clock.systemUTC();
  }

  /**
   * Every topic and its dead-letter topic, since the broker never creates one on its own. Existing
   * topics are left as they are. shortcut: one replica for the single-server deployment (ADR 0005).
   */
  @Bean
  KafkaAdmin.NewTopics topics() {
    return new KafkaAdmin.NewTopics(
        Topics.ALL.stream()
            .flatMap(topic -> Stream.of(topic, Topics.deadLetter(topic)))
            .map(topic -> TopicBuilder.name(topic).partitions(3).replicas(1).build())
            .toArray(NewTopic[]::new));
  }
}
