package app.orderloop.platform.outbox;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.boot.kafka.autoconfigure.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

/**
 * The outbox writer and relay, for services that set {@code orderloop.outbox.schema}. The relay's
 * ops pool and producer are private to it, so they are not beans: a second {@code DataSource} bean
 * would replace the application's, and the producer settings must not be overridden. Both are
 * created at startup, so a wrong password or producer setting stops startup instead of failing
 * every poll.
 */
@AutoConfiguration(
    afterName = {
      "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
      "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration"
    })
@ConditionalOnClass({
  KafkaTemplate.class,
  JdbcTemplate.class,
  JsonMapper.class,
  HikariDataSource.class
})
@ConditionalOnProperty("orderloop.outbox.schema")
@EnableConfigurationProperties(OutboxProperties.class)
public class OutboxAutoConfiguration {

  @Bean
  OutboxWriter outboxWriter(JdbcTemplate jdbc, OutboxProperties properties) {
    return new OutboxWriter(jdbc, properties.schema());
  }

  @Bean(destroyMethod = "close")
  OutboxRelay outboxRelay(
      OutboxProperties properties, DataSourceProperties dataSource, KafkaProperties kafka) {
    var ops = new HikariConfig();
    ops.setPoolName("outbox-relay");
    ops.setJdbcUrl(dataSource.determineUrl());
    ops.setUsername(properties.opsUsername());
    ops.setPassword(properties.opsPassword());
    ops.setMaximumPoolSize(2);

    Map<String, Object> producer = new HashMap<>(kafka.buildProducerProperties());
    producer.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producer.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    // Fixed, whatever the application configures: an idempotent producer that keeps order.
    producer.put(ProducerConfig.ACKS_CONFIG, "all");
    producer.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    producer.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    producer.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    // Bounded, so every send has a final result before the relay's transaction ends.
    producer.put(
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) properties.deliveryTimeout().toMillis());
    producer.put(
        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) properties.requestTimeout().toMillis());
    producer.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, properties.requestTimeout().toMillis());
    var producerFactory = new DefaultKafkaProducerFactory<String, String>(producer);
    producerFactory.createProducer(); // Validates the settings now, not on the first poll.

    return new OutboxRelay(new HikariDataSource(ops), producerFactory, properties);
  }

  @Bean
  @ConditionalOnProperty(
      name = "orderloop.outbox.relay-enabled",
      havingValue = "true",
      matchIfMissing = true)
  OutboxRelayPoller outboxRelayPoller(OutboxRelay relay, OutboxProperties properties) {
    return new OutboxRelayPoller(relay::publishBatch, relay.batchSize(), properties.pollInterval());
  }
}
