package app.orderloop.platform.tenant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import app.orderloop.platform.TestDatabase;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceContext;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DelegatingDataSource;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Proves the database is the isolation boundary (invariant 2). Connects as orderloop_app, never a
 * superuser, because superusers bypass row-level security and would prove nothing. One pooled
 * connection, so every test reuses the same physical session.
 */
@Testcontainers
@SpringBootTest(
    classes = TenantRlsIT.ProbeApp.class,
    webEnvironment = WebEnvironment.NONE,
    properties = {
      "spring.datasource.hikari.maximum-pool-size=1",
      "spring.jpa.hibernate.ddl-auto=validate"
    })
class TenantRlsIT {

  private static final String APP_PASSWORD = TestDatabase.APP_PASSWORD;

  @Container static final PostgreSQLContainer POSTGRES = TestDatabase.newContainer();

  @DynamicPropertySource
  static void database(DynamicPropertyRegistry registry) {
    TestDatabase.register(registry, POSTGRES);
  }

  @PersistenceContext private EntityManager em;
  @Autowired private JdbcTemplate jdbc;
  @Autowired private TransactionTemplate tx;
  @Autowired private EntityManagerFactory emf;

  @Test
  void connectsAsTheAppRoleWhichCannotBypassRowLevelSecurity() {
    String role =
        unbound(
            () ->
                jdbc.queryForObject(
                    "SELECT current_user || '|' || rolsuper || '|' || rolbypassrls"
                        + " FROM pg_roles WHERE rolname = current_user",
                    String.class));

    assertThat(role).isEqualTo("orderloop_app|false|false");
  }

  @Test
  void aTenantSeesItsOwnRow() {
    UUID id = insertAs("t_a", "original");

    TenantProbe found = as("t_a", () -> em.find(TenantProbe.class, id));

    assertThat(found).isNotNull();
    assertThat(found.tenantId()).isEqualTo("t_a");
    assertThat(found.label()).isEqualTo("original");
  }

  @Test
  void anotherTenantCannotFindTheRowById() {
    UUID id = insertAs("t_a", "original");

    assertThat(as("t_b", () -> em.find(TenantProbe.class, id))).isNull();
  }

  @Test
  void anotherTenantCannotReadTheRowWithNativeSql() {
    UUID id = insertAs("t_a", "original");

    assertThat(as("t_b", () -> countProbe(id))).isZero();
  }

  @Test
  void anotherTenantCannotUpdateTheRow() {
    UUID id = insertAs("t_a", "original");

    int viaJpa =
        as(
            "t_b",
            () ->
                em.createQuery("update TenantProbe p set p.label = 'changed' where p.id = :id")
                    .setParameter("id", id)
                    .executeUpdate());
    int viaSql =
        as(
            "t_b",
            () ->
                jdbc.update(
                    "UPDATE orchestrator.tenant_probe SET label = 'changed' WHERE probe_id = ?",
                    id));

    assertThat(viaJpa).isZero();
    assertThat(viaSql).isZero();
    assertThat(as("t_a", () -> em.find(TenantProbe.class, id).label())).isEqualTo("original");
  }

  @Test
  void anotherTenantCannotDeleteTheRow() {
    UUID id = insertAs("t_a", "original");

    int viaJpa =
        as(
            "t_b",
            () ->
                em.createQuery("delete from TenantProbe p where p.id = :id")
                    .setParameter("id", id)
                    .executeUpdate());
    int viaSql =
        as(
            "t_b",
            () -> jdbc.update("DELETE FROM orchestrator.tenant_probe WHERE probe_id = ?", id));

    assertThat(viaJpa).isZero();
    assertThat(viaSql).isZero();
    assertThat(as("t_a", () -> countProbe(id))).isEqualTo(1);
  }

  @Test
  void anotherTenantCannotWriteRowsForTheFirstTenant() {
    assertThatThrownBy(
            () ->
                as(
                    "t_b",
                    () ->
                        jdbc.update(
                            "INSERT INTO orchestrator.tenant_probe (tenant_id, probe_id, label)"
                                + " VALUES ('t_a', ?, 'planted')",
                            UUID.randomUUID())))
        .isInstanceOf(DataAccessException.class)
        .rootCause()
        .hasMessageContaining("new row violates row-level security policy");
  }

  @Test
  void anUnboundScopeSeesNothing() {
    UUID id = insertAs("t_a", "original");

    assertThat(unbound(() -> em.find(TenantProbe.class, id))).isNull();
    assertThat(
            unbound(
                () ->
                    jdbc.queryForObject(
                        "SELECT count(*) FROM orchestrator.tenant_probe", Integer.class)))
        .isZero();
  }

  @Test
  void anUnboundScopeCannotWrite() {
    assertThatThrownBy(
            () ->
                unbound(
                    () ->
                        jdbc.update(
                            "INSERT INTO orchestrator.tenant_probe (tenant_id, probe_id, label)"
                                + " VALUES ('', ?, 'orphan')",
                            UUID.randomUUID())))
        .isInstanceOf(DataAccessException.class)
        .rootCause()
        .hasMessageContaining("new row violates row-level security policy");
  }

  @Test
  void aPooledConnectionNeverCarriesTheLastTenant() {
    UUID id = insertAs("t_a", "original");
    int sessionForA = as("t_a", this::backendPid);

    int sessionUnbound = unbound(this::backendPid);
    int seenUnbound = unbound(() -> countProbe(id));
    int sessionForB = as("t_b", this::backendPid);
    int seenByB = as("t_b", () -> countProbe(id));

    assertThat(sessionUnbound).isEqualTo(sessionForA);
    assertThat(sessionForB).isEqualTo(sessionForA);
    assertThat(seenUnbound).isZero();
    assertThat(seenByB).isZero();
  }

  @Test
  void rowLevelSecurityHidesTheRowEvenWhenHibernateFiltersForItsOwner() {
    UUID id = insertAs("t_a", "original");
    SessionFactory sessions = emf.unwrap(SessionFactory.class);

    // Hibernate 7 filters find() by the session's tenant. Give the session the owner's tenant while
    // the connection is bound to another, so only row-level security can hide the row.
    TenantProbe sameTenant =
        TenantContext.callAs("t_a", () -> findWithSessionTenant(sessions, "t_a", id));
    TenantProbe crossed =
        TenantContext.callAs("t_b", () -> findWithSessionTenant(sessions, "t_a", id));

    assertThat(sameTenant).isNotNull();
    assertThat(crossed).isNull();
  }

  @Test
  void theTenantSettingSurvivesARollbackWhenAutoCommitIsOff() throws Exception {
    var raw = new PGSimpleDataSource();
    raw.setUrl(POSTGRES.getJdbcUrl());
    raw.setUser("orderloop_app");
    raw.setPassword(APP_PASSWORD);
    DataSource noAutoCommit =
        new DelegatingDataSource(raw) {
          @Override
          public Connection getConnection() throws SQLException {
            Connection connection = super.getConnection();
            connection.setAutoCommit(false);
            return connection;
          }
        };
    var dataSource = new TenantAwareDataSource(noAutoCommit);

    String afterRollback =
        TenantContext.callAs(
            "t_a",
            () -> {
              try (Connection connection = dataSource.getConnection();
                  Statement statement = connection.createStatement()) {
                connection.rollback();
                try (ResultSet row =
                    statement.executeQuery("SELECT current_setting('app.tenant_id', true)")) {
                  row.next();
                  return row.getString(1);
                }
              }
            });

    assertThat(afterRollback).isEqualTo("t_a");
  }

  private static TenantProbe findWithSessionTenant(
      SessionFactory sessions, String sessionTenant, UUID id) {
    try (Session session = sessions.withOptions().tenantIdentifier(sessionTenant).openSession()) {
      return session.find(TenantProbe.class, id);
    }
  }

  private UUID insertAs(String tenant, String label) {
    UUID id = UUID.randomUUID();
    as(
        tenant,
        () -> {
          em.persist(new TenantProbe(id, label));
          return null;
        });
    return id;
  }

  private int countProbe(UUID id) {
    return jdbc.queryForObject(
        "SELECT count(*) FROM orchestrator.tenant_probe WHERE probe_id = ?", Integer.class, id);
  }

  private int backendPid() {
    return jdbc.queryForObject("SELECT pg_backend_pid()", Integer.class);
  }

  /** Binds the scope before the transaction opens, as .claude/rules/tenancy.md requires. */
  private <T> T as(String tenant, Supplier<T> work) {
    return TenantContext.callAs(tenant, () -> tx.execute(status -> work.get()));
  }

  private <T> T unbound(Supplier<T> work) {
    assertThat(TenantContext.current()).isEmpty();
    return tx.execute(status -> work.get());
  }

  @SpringBootConfiguration
  @EnableAutoConfiguration
  static class ProbeApp {}
}
