---
paths:
  - "web-dashboard/**"
---

# Frontend rules

- Use React, strict TypeScript and Vite. Generate API types from the OpenAPI document.
- The app is embedded in Shopify Admin. Use Shopify's current App Bridge and Polaris guidance; confirm it in their docs before adding UI dependencies.
- Authenticate with Shopify session tokens. Never store tokens in `localStorage`.
- The home screen answers one question: how much money is at risk this week, and what should I do first?
- Format money with `Intl.NumberFormat(locale, { style: 'currency', currency })`, using the amount's own currency and the merchant's locale. Convert minor units with that currency's ISO 4217 exponent, never an assumed 2. Do arithmetic on integers only, and never add amounts in different currencies.
- Show the exact amount and consequence on the refund approval screen, and disable the button while the request is in flight.
- Every list needs loading, empty and error states, and every screen must work by keyboard.
- Avoid showing customer PII unless the merchant needs it to act.
- Test critical flows with Playwright.
