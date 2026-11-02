package app.orderloop.orchestrator.saga;

import java.time.Instant;

public record Deadline(DeadlineKind kind, Instant dueAt) {}
