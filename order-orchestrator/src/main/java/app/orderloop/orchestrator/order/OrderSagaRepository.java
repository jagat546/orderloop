package app.orderloop.orchestrator.order;

import org.springframework.data.jpa.repository.JpaRepository;

/** Hibernate filters by the session's tenant; row-level security enforces it underneath. */
interface OrderSagaRepository extends JpaRepository<OrderSaga, String> {}
