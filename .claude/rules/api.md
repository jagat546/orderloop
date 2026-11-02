---
paths:
  - "**/web/**"
  - "**/*Controller.java"
  - "**/*Advice.java"
---

# API rules

- Requests and responses are records with Bean Validation annotations. Never expose JPA entities.
- All errors go through `@RestControllerAdvice` as RFC 9457 `ProblemDetail` with a stable `type` URI. RFC 9457 replaced RFC 7807 and keeps the same format.
- Never include stack traces, SQL or another tenant's identifiers in an error. A cross-tenant lookup returns 404.
- Paths are versioned: `/api/v1/...`. Lists use cursor pagination with a maximum page size.
- Money appears in JSON as `{"amountMinor": 12345, "currency": "EUR"}`: integer minor units and an ISO 4217 code. Never a decimal number. The dashboard formats it.
- State-changing endpoints that can be retried accept an `Idempotency-Key`.
- Approving a refund must record who approved it, when, and the exact amount shown to them.
- Controllers stay thin: validate, call one application service, map the result.
- Controllers return synchronously. `Callable`, `DeferredResult`, `WebAsyncTask`, `SseEmitter`, `ResponseBodyEmitter` and `StreamingResponseBody` run outside the request's tenant scope and fail closed with 500 or 401. Add an ArchUnit rule for this with the first controller.
- Keep the OpenAPI document current, because the dashboard's types are generated from it.
