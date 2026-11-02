# 0001: Use Java 25 and Spring Boot 4.1

**Status:** Accepted
**Date:** 2026-11-02

## Context

The original spec asked for Java 21 and Spring Boot 3.3+. [`ScopedValue` is a preview API on Java 21 and final only in Java 25](https://openjdk.org/jeps/506), and its API changed in between. [Spring Boot 3.3 lost open-source support in June 2025, and all of 3.x by June 2026](https://www.danvega.dev/blog/spring-boot-end-of-life). [Pinning on `synchronized` was removed for virtual threads by JEP 491](https://openjdk.org/jeps/491) after Java 21.

## Decision

Use Java 25 LTS and the newest supported Spring Boot 4.x, currently 4.1. Do not use preview features in production code.

## Consequences

- Scoped values work without preview flags, and virtual threads pin far less.
- Fewer tutorials and some libraries may lag, so verify compatibility during M1.1.
- Spring Boot releases have short free support windows, so plan a minor upgrade about every 6 to 12 months.
- Revisit if a required library cannot run on this stack.
