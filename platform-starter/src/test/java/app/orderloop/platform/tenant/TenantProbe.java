package app.orderloop.platform.tenant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.util.UUID;
import org.hibernate.annotations.TenantId;

/** Test-only sample entity for proving row-level security. */
@Entity
@Table(schema = "orchestrator", name = "tenant_probe")
class TenantProbe {

  @Id
  @Column(name = "probe_id")
  private UUID id;

  @TenantId
  @Column(name = "tenant_id")
  private String tenantId;

  @Column(name = "label")
  private String label;

  protected TenantProbe() {}

  TenantProbe(UUID id, String label) {
    this.id = id;
    this.label = label;
  }

  String tenantId() {
    return tenantId;
  }

  String label() {
    return label;
  }
}
