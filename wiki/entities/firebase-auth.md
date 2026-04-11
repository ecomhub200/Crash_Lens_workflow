---
title: Firebase Authentication
type: entity
tags: [auth, firebase, users, security]
created: 2026-04-05
updated: 2026-04-05
sources: [source-frontend-repo]
---

# Firebase Authentication

**User authentication and management** for the [[douglas-county-frontend|Crash Lens web app]].

## Auth Methods
- Google OAuth (single sign-on)
- Email/Password registration

## Integration
- Firebase Admin SDK on the Node.js backend (`server/qdrant-proxy.js`)
- Firebase client SDK on the frontend
- Used alongside [[stripe-billing]] for subscription management

## Related Pages

- [[douglas-county-frontend]] — The app that uses this auth
- [[stripe-billing]] — Payment system paired with auth
