package app.orderloop.orchestrator.order;

import app.orderloop.orchestrator.saga.DeadlineKind;
import app.orderloop.platform.outbox.OutboxProperties;
import app.orderloop.platform.tenant.TenantContext;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Fires missed deadlines. It finds due deadlines across tenants as orderloop_ops, which may only
 * read them, then fires each one as orderloop_app inside that deadline's tenant scope. Deadlines
 * live in the database, so one stored before a restart fires after it, and several orchestrators
 * may scan at once: {@link SagaService#fireDeadline} fires each deadline once.
 *
 * <p>The ops pool is private, like the outbox relay's: a second DataSource bean would replace the
 * application's.
 */
@Component
class DeadlineScanner implements DisposableBean {

  private static final Logger log = LoggerFactory.getLogger(DeadlineScanner.class);

  // kind stays text until the loop, so a row the enum cannot read fails alone.
  private record Due(String tenantId, String orderId, String kind, Instant dueAt) {}

  private final SagaService sagas;
  private final Clock clock;
  private final HikariDataSource ops;
  private final JdbcTemplate opsJdbc;

  DeadlineScanner(
      SagaService sagas, Clock clock, DataSourceProperties dataSource, OutboxProperties outbox) {
    this.sagas = sagas;
    this.clock = clock;
    var config = new HikariConfig();
    config.setPoolName("deadline-scanner");
    config.setJdbcUrl(dataSource.determineUrl());
    config.setUsername(outbox.opsUsername());
    config.setPassword(outbox.opsPassword());
    config.setMaximumPoolSize(1);
    this.ops = new HikariDataSource(config);
    this.opsJdbc = new JdbcTemplate(ops);
  }

  @Scheduled(fixedDelayString = "${orderloop.orchestrator.deadline-poll-interval:5s}")
  void run() {
    // shortcut: a deadline that keeps failing stays at the head of the scan; 100 of them would
    // starve the rest. Back off failed rows if that is ever seen.
    List<Due> due =
        opsJdbc.query(
            "SELECT tenant_id, order_id, kind, due_at FROM orchestrator.saga_deadline"
                + " WHERE due_at <= ? ORDER BY due_at LIMIT 100",
            (row, n) ->
                new Due(
                    row.getString("tenant_id"),
                    row.getString("order_id"),
                    row.getString("kind"),
                    row.getTimestamp("due_at").toInstant()),
            Timestamp.from(clock.instant()));
    for (Due deadline : due) {
      try {
        TenantContext.callAs(
            deadline.tenantId(),
            () ->
                sagas.fireDeadline(
                    deadline.orderId(), DeadlineKind.valueOf(deadline.kind()), deadline.dueAt()));
      } catch (RuntimeException e) {
        // The deadline stays due, so the next pass tries again; one bad row never blocks the rest.
        log.error(
            "Deadline {} for order {} could not be fired", deadline.kind(), deadline.orderId(), e);
      }
    }
  }

  @Override
  public void destroy() {
    ops.close();
  }
}
