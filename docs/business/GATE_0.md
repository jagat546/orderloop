# Gate 0: prove someone pays

**Optional.** Since ADR 0007, OrderLoop is built first as an open-source project, and this gate is not in the build path. Run it only if you decide to sell audits or a hosted version. It was written for Indian D2C brands, so adapt the audience, currency and providers to the market you test.

Test willingness to pay before investing in sales. The goal is 3 paid audits within 30 days.

## First: check you can do this

Read your employment contract for rules on outside work and IP, and get advice before selling. Use no code, data or documents from any employer. Ask an accountant about registration and GST before invoicing. This is not legal or tax advice.

## Who to talk to

Founders or operations leads at Shopify brands in India with 1,000 to 10,000 orders a month and a meaningful COD share. Find them through LinkedIn, Shopify-partner agencies and D2C communities.

## Outreach message

> I'm researching how D2C brands track money stuck after an order: RTOs not restocked, refunds missed, COD remittance gaps. Could I ask you about it for 15 minutes? I'm not selling anything on this call.

## Interview questions

Ask about what they did, not what they might do.

1. Tell me about the last order that went wrong after it was placed. What happened?
2. Who noticed, and how? How long did it take?
3. How do you reconcile Shopify, your gateway and your courier today?
4. Roughly what share of orders is COD, and what is your RTO rate? How do you know?
5. When did you last find money you were owed, or stock you had lost track of? How much?
6. What have you tried? What did you stop using, and why?
7. What do you pay for today in this area?
8. Who would approve spending to fix this?

Then offer the audit: "If I can show you what leaked in your last 90 days, using your exports, would you pay ₹4,999 for that report?"

Count only money paid. "Sounds interesting" is not evidence.

## The Leak Audit

**Inputs:** 90 days of Shopify order export, Razorpay payments and settlements, and Shiprocket shipments and COD remittance.

**Data handling:** get written permission, ask for exports with customer names, phones and addresses removed, keep files encrypted, and delete them within 30 days.

**Method:** join on order number, then run the five checks.

| Check | Rule |
|---|---|
| Paid, not fulfilled | Captured payment, with a cancelled order and no refund, or no shipment |
| NDR past SLA | Failed delivery with no action within 24 hours |
| RTO not restocked | Returned to origin, with no matching stock adjustment |
| COD remittance gap | Delivered COD order missing from remittance after the expected date |
| Stuck order | No status change for 10 days and not closed |

**Report:** one page with the total in rupees, each category's count and amount, the 10 largest cases by order number, and what to do next. Separate confirmed amounts from estimates, and do not inflate them.

## Decision rules

| Checkpoint | Result | Decision |
|---|---|---|
| Day 30 | Fewer than 3 paid audits from 15 conversations | Stop selling. Keep this as a portfolio project |
| After audits | Median finding under ₹25,000 a month | The leak is too small to sell against |
| After audits | Buyers say they would pay monthly for it | Continue to M2, with buyers as design partners |
| 6 months after launch | Fewer than 5 paying merchants | Stop investing in sales |

Record the result with `/record-decision`.
