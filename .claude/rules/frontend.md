---
paths:
  - "web-dashboard/**"
---

# Frontend rules

- Use React, strict TypeScript and Vite. Generate API types from the OpenAPI document.
- The app is embedded in Shopify Admin. Use Shopify's current App Bridge and Polaris guidance; confirm it in their docs before adding UI dependencies.
- Authenticate with Shopify session tokens. Never store tokens in `localStorage`.
- The home screen answers one question: how much money is at risk this week, and what should I do first?
- Format paise as Indian rupees with `Intl.NumberFormat('en-IN', { style: 'currency', currency: 'INR' })`. Do arithmetic on integers only.
- Show the exact amount and consequence on the refund approval screen, and disable the button while the request is in flight.
- Every list needs loading, empty and error states, and every screen must work by keyboard.
- Avoid showing customer PII unless the merchant needs it to act.
- Test critical flows with Playwright.
