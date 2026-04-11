---
title: AI Integration
type: concept
tags: [ai, claude, llm, natural-language, assistant]
created: 2026-04-05
updated: 2026-04-05
sources: [source-frontend-repo]
---

# AI Integration

**Claude AI assistant** embedded in the [[douglas-county-frontend|Crash Lens web app]] for natural language crash analysis.

## What It Does

The AI Assistant tab lets users ask questions in plain English:
- "What are the top 5 most dangerous intersections in this county?"
- "Compare pedestrian crash trends 2020 vs 2023"
- "What countermeasures would reduce crashes at Route 7 and Main St?"

## Architecture

- **Frontend**: AI context awareness module (`app/modules/ai/`)
- **Backend**: Claude API integration via Node.js proxy
- **Knowledge**: [[qdrant-vector-db]] stores embeddings for semantic search
- **Context**: AI module has access to current crash data, filters, and analysis state (`aiState`)

## How Context Flows

1. User asks a question in the AI tab
2. Frontend packages current data context (selected jurisdiction, filters, crash counts)
3. Backend queries [[qdrant-vector-db]] for relevant knowledge
4. Claude receives question + data context + retrieved knowledge
5. Response is displayed with citations to specific crash records or wiki knowledge

## Related Pages

- [[douglas-county-frontend]] — The app housing the AI tab
- [[qdrant-vector-db]] — Vector database for semantic search
- [[safety-countermeasures]] — AI can recommend treatments
- [[hotspot-analysis]] — AI can explain hotspot patterns
