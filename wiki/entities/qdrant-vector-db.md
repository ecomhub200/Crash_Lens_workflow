---
title: Qdrant Vector Database
type: entity
tags: [ai, vector-db, search, qdrant]
created: 2026-04-05
updated: 2026-04-05
sources: [source-frontend-repo]
---

# Qdrant Vector Database

**Vector database** powering semantic search and [[ai-integration|AI-assisted analysis]] in the [[douglas-county-frontend|Crash Lens web app]].

## Purpose
- Stores vector embeddings of crash data and safety knowledge
- Enables semantic similarity search for the AI assistant
- Queried through the Node.js proxy server (`server/qdrant-proxy.js`)

## Related Pages

- [[ai-integration]] — The AI assistant that queries Qdrant
- [[douglas-county-frontend]] — The app housing the proxy server
