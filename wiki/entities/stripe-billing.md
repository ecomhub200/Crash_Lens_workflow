---
title: Stripe Billing
type: entity
tags: [payments, stripe, billing, subscriptions]
created: 2026-04-05
updated: 2026-04-05
sources: [source-frontend-repo]
---

# Stripe Billing

**Payment processing and subscription management** for [[crash-lens-overview|Crash Lens]].

## Plan Tiers
| Plan | Target |
|------|--------|
| Trial | New users evaluating the platform |
| Individual | Solo traffic engineers |
| Team | Small agency teams |
| Agency | Full department access |

## Integration
- Stripe SDK in Node.js backend (`server/qdrant-proxy.js`)
- Handles subscription creation, upgrades, cancellations
- Works with [[firebase-auth]] for user identity

## Related Pages

- [[douglas-county-frontend]] — The app with billing UI
- [[firebase-auth]] — User identity paired with billing
