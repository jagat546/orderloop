# 0006: JSON message contracts in version 1

**Status:** Accepted
**Date:** 2026-11-02

## Context

Avro with a schema registry gives stronger compatibility checks, but a registry means another service to run and more memory on one VPS. All producers and consumers live in one repository.

## Decision

Use JSON with records in `common-dto`, a `schema_version` header, and golden-file tests that fail if the serialized form changes.

## Consequences

- Simple to debug and to run.
- Compatibility rests on tests and discipline, not a registry, and messages are larger.
- Revisit Avro or Protobuf if services split into separate repositories or message volume makes size matter.
