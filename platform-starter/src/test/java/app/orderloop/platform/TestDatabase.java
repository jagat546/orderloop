package app.orderloop.platform;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * PostgreSQL for integration tests, set up by the same init script as the development stack. The
 * application connects as orderloop_app, never a superuser, because superusers bypass row-level
 * security and would prove nothing. Flyway runs as orderloop_owner.
 */
public final class TestDatabase {

  public static final String OWNER_PASSWORD = "owner-test-password";
  public static final String APP_PASSWORD = "app-test-password";
  public static final String OPS_PASSWORD = "ops-test-password";

  private TestDatabase() {}

  public static PostgreSQLContainer newContainer() {
    return new PostgreSQLContainer("postgres:18.6")
        .withDatabaseName("orderloop")
        .withUsername("postgres")
        .withPassword("superuser-test-password")
        .withEnv("POSTGRES_OWNER_PASSWORD", OWNER_PASSWORD)
        .withEnv("POSTGRES_APP_PASSWORD", APP_PASSWORD)
        .withEnv("POSTGRES_OPS_PASSWORD", OPS_PASSWORD)
        .withCopyFileToContainer(
            MountableFile.forHostPath("../deploy/postgres/init/01-roles-and-schemas.sh"),
            "/docker-entrypoint-initdb.d/01-roles-and-schemas.sh");
  }

  public static void register(DynamicPropertyRegistry registry, PostgreSQLContainer postgres) {
    register(registry, postgres, "orchestrator");
  }

  /** As {@link #register(DynamicPropertyRegistry, PostgreSQLContainer)}, for a service's schema. */
  public static void register(
      DynamicPropertyRegistry registry, PostgreSQLContainer postgres, String schema) {
    registry.add("spring.datasource.url", postgres::getJdbcUrl);
    registry.add("spring.datasource.username", () -> "orderloop_app");
    registry.add("spring.datasource.password", () -> APP_PASSWORD);
    registry.add("spring.flyway.user", () -> "orderloop_owner");
    registry.add("spring.flyway.password", () -> OWNER_PASSWORD);
    registry.add("spring.flyway.schemas", () -> schema);
    registry.add("spring.jpa.open-in-view", () -> "false");
  }
}
