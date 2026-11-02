---
paths:
  - "**/*.java"
---

# Java rules

- Target Java 25. Use records, sealed types and pattern matching. Favour immutable data.
- No Lombok. JPA entities are plain classes with a protected no-argument constructor and domain methods, not setters.
- Enable virtual threads with `spring.threads.virtual.enabled=true`. They do not add database capacity, so keep Hikari pools bounded and limit provider calls with semaphores.
- Use `ScopedValue.where(KEY, value).call(...)`. `callWhere` and `runWhere` were removed before Java 25, so do not copy older examples.
- Log through SLF4J with parameters, not string concatenation. Use structured JSON in production, with `traceId`, `spanId` and `tenantId` in MDC.
- Log state transitions and failures, with IDs only. Log no names, phones, emails, addresses, tokens or payloads.
- Inject `Clock`. Do not call `Instant.now()` in domain code.
- Depend on interfaces at provider boundaries. Avoid speculative abstractions elsewhere.
