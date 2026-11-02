package app.orderloop.platform.tenant;

import javax.sql.DataSource;
import org.hibernate.cfg.AvailableSettings;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.hibernate.autoconfigure.HibernatePropertiesCustomizer;
import org.springframework.context.annotation.Bean;

/**
 * Tenant-aware data access for services with a database. Every {@link DataSource} bean is wrapped
 * in {@link TenantAwareDataSource}. Flyway's own owner connection is not a bean, so it is not.
 */
@AutoConfiguration
@ConditionalOnClass(HibernatePropertiesCustomizer.class)
public class TenantDataAutoConfiguration {

  @Bean
  static BeanPostProcessor tenantAwareDataSourceWrapper() {
    return new BeanPostProcessor() {
      @Override
      public Object postProcessAfterInitialization(Object bean, String name) {
        return bean instanceof DataSource dataSource && !(bean instanceof TenantAwareDataSource)
            ? new TenantAwareDataSource(dataSource)
            : bean;
      }
    };
  }

  @Bean
  HibernatePropertiesCustomizer tenantIdentifierResolver() {
    return properties ->
        properties.put(
            AvailableSettings.MULTI_TENANT_IDENTIFIER_RESOLVER, new TenantIdentifierResolver());
  }
}
