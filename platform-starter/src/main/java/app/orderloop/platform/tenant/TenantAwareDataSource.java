package app.orderloop.platform.tenant;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DelegatingDataSource;

/**
 * Sets {@code app.tenant_id} on every connection checkout, so PostgreSQL row-level security sees
 * the current tenant (invariant 2). Overwritten at every checkout, so a pooled connection never
 * carries an earlier tenant. Unbound, it sets an empty string, which the policy matches to nothing.
 */
public final class TenantAwareDataSource extends DelegatingDataSource {

  private static final String SET_TENANT = "SELECT set_config('app.tenant_id', ?, false)";

  public TenantAwareDataSource(DataSource target) {
    super(target);
  }

  @Override
  public Connection getConnection() throws SQLException {
    return bindTenant(super.getConnection());
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return bindTenant(super.getConnection(username, password));
  }

  private static Connection bindTenant(Connection connection) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(SET_TENANT)) {
      statement.setString(1, TenantContext.current().orElse(""));
      statement.execute();
      // A session setting made inside a transaction is undone by rollback, so commit it now.
      if (!connection.getAutoCommit()) {
        connection.commit();
      }
      return connection;
    } catch (SQLException | RuntimeException e) {
      try {
        connection.close();
      } catch (SQLException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
  }
}
