package app.orderloop.platform.inbox;

import app.orderloop.platform.outbox.OutboxAutoConfiguration;
import app.orderloop.platform.outbox.OutboxProperties;
import app.orderloop.platform.outbox.OutboxRelay;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The inbox and error handling for every listener of a service with an outbox. Spring Boot attaches
 * the {@link CommonErrorHandler} bean to its listener container factory. Dead letters are sent by
 * {@link OutboxRelay}, the only class that sends to Kafka.
 */
@AutoConfiguration(after = OutboxAutoConfiguration.class)
@ConditionalOnClass(DefaultErrorHandler.class)
@ConditionalOnProperty("orderloop.outbox.schema")
@EnableConfigurationProperties(InboxProperties.class)
public class InboxAutoConfiguration {

  @Bean
  InboxConsumer inboxConsumer(
      JdbcTemplate jdbc,
      TransactionTemplate tx,
      OutboxProperties outbox,
      ObjectProvider<Tracer> tracer,
      ObjectProvider<Propagator> propagator) {
    return new InboxConsumer(
        jdbc,
        tx,
        outbox.schema(),
        tracer.getIfAvailable(() -> Tracer.NOOP),
        propagator.getIfAvailable(() -> Propagator.NOOP));
  }

  @Bean
  CommonErrorHandler inboxErrorHandler(
      OutboxRelay relay, InboxProperties properties, MeterRegistry meters) {
    var backOff = new ExponentialBackOffWithMaxRetries(properties.maxRetries());
    backOff.setInitialInterval(properties.initialInterval().toMillis());
    backOff.setMultiplier(2);
    backOff.setMaxInterval(properties.maxInterval().toMillis());
    var handler =
        new DefaultErrorHandler(
            (record, failure) -> {
              relay.deadLetter(record, failure);
              // Counted once it is on the dead-letter topic; every one needs a look.
              meters.counter("orderloop.inbox.dead.letters", "topic", record.topic()).increment();
            },
            backOff);
    handler.addNotRetryableExceptions(InvalidMessageException.class);
    return handler;
  }
}
