package app.orderloop.payment;

import app.orderloop.platform.outbox.OutboxProperties;
import app.orderloop.platform.tenant.TenantContext;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.autoconfigure.DataSourceProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Sends recorded refunds and settles those whose outcome is unknown. It finds due refunds across
 * tenants as orderloop_ops, which may only read them, then handles each one as orderloop_app inside
 * that refund's tenant scope. A refund marked SENT or UNKNOWN is due once it has waited {@code
 * orderloop.payment.reconcile-after}, so a call still in flight is left alone; even if it is not,
 * the provider's idempotency key keeps a second call from refunding twice.
 *
 * <p>The ops pool is private, like the outbox relay's: a second DataSource bean would replace the
 * application's.
 */
@Component
class RefundReconciler implements DisposableBean {

  private static final Logger log = LoggerFactory.getLogger(RefundReconciler.class);

  private record Due(String tenantId, String refundId) {}

  private final Refunds refunds;
  private final Duration reconcileAfter;
  private final HikariDataSource ops;
  private final JdbcTemplate opsJdbc;

  RefundReconciler(
      Refunds refunds,
      DataSourceProperties dataSource,
      OutboxProperties outbox,
      @Value("${orderloop.payment.reconcile-after:30s}") Duration reconcileAfter) {
    this.refunds = refunds;
    this.reconcileAfter = reconcileAfter;
    var config = new HikariConfig();
    config.setPoolName("refund-reconciler");
    config.setJdbcUrl(dataSource.determineUrl());
    config.setUsername(outbox.opsUsername());
    config.setPassword(outbox.opsPassword());
    config.setMaximumPoolSize(1);
    this.ops = new HikariDataSource(config);
    this.opsJdbc = new JdbcTemplate(ops);
  }

  @Scheduled(fixedDelayString = "${orderloop.payment.refund-poll-interval:1s}")
  void run() {
    List<Due> due =
        opsJdbc.query(
            "SELECT tenant_id, refund_id FROM payment.refund_intent WHERE status = 'INTENT'"
                + " OR (status IN ('SENT', 'UNKNOWN')"
                + " AND updated_at < now() - make_interval(secs => ?))"
                + " ORDER BY updated_at LIMIT 100",
            (row, n) -> new Due(row.getString("tenant_id"), row.getString("refund_id")),
            (double) reconcileAfter.toMillis() / 1000);
    for (Due refund : due) {
      try {
        TenantContext.callAs(
            refund.tenantId(),
            () -> {
              refunds.process(refund.refundId());
              return null;
            });
      } catch (RuntimeException e) {
        // The refund stays due, so the next pass tries again; one bad row never blocks the rest.
        log.error("Refund {} could not be processed", refund.refundId(), e);
      }
    }
  }

  @Override
  public void destroy() {
    ops.close();
  }
}
