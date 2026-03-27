# CrashLens API and MCP Server Configuration Reference

> **Auto-generated reference** for AI code assistants and developers.
> When enhancing CrashLens, read this file to understand all integration points, API endpoints, MCP tools, environment variables, and data pipeline scripts.

---

## Table of Contents

1. [MCP Server Configuration](#1-mcp-server-configuration)
2. [MCP Tools (22 total)](#2-mcp-tools-22-total)
3. [MCP Resources (6 total)](#3-mcp-resources-6-total)
4. [Node.js API Server Endpoints](#4-nodejs-api-server-endpoints)
5. [Environment Variables](#5-environment-variables)
6. [Configuration Files](#6-configuration-files)
7. [Data Pipeline Scripts](#7-data-pipeline-scripts)
8. [Shared Libraries (MCP Server)](#8-shared-libraries-mcp-server)
9. [Authentication and Authorization](#9-authentication-and-authorization)
10. [Infrastructure](#10-infrastructure)

---

## 1. MCP Server Configuration

### Registration (`.mcp.json`)

```json
{
  "mcpServers": {
    "crashlens": {
      "command": "node",
      "args": ["mcp-server/index.js"]
    }
  }
}
```

### Package Metadata (`mcp-server/package.json`)

| Field | Value |
|-------|-------|
| Name | `@crashlens_maq/mcp` |
| Version | `1.2.0` |
| Server Version | `2.0.0` |
| Type | ES Module (`"type": "module"`) |
| Binary | `crashlens-mcp` |
| Node Engine | `>=18.0.0` |
| Transport | stdio (stdin/stdout) |

### Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `@modelcontextprotocol/sdk` | `^1.12.1` | MCP protocol SDK |
| `csv-parse` | `^5.6.0` | CSV crash data parsing |
| `zod` | `^3.24.0` | Schema validation |

### Operating Modes

#### Standalone Mode (Production)
For end-users via Claude Desktop. Auto-downloads data from R2 cloud storage.

| Env Variable | Required | Description |
|-------------|----------|-------------|
| `CRASHLENS_STATE` | Yes | State name (e.g., `virginia`) |
| `CRASHLENS_JURISDICTION` | Yes | Jurisdiction name (e.g., `henrico`) |
| `CRASHLENS_ROAD_TYPE` | No | `all_roads` (default), `county_roads`, `no_interstate` |
| `CRASHLENS_API_KEY` | Yes | User API key (format: `clmcp_` + 32 hex chars) |

Detection: Standalone mode activates when both `CRASHLENS_STATE` and `CRASHLENS_JURISDICTION` are set.

Data is cached locally to `~/.crashlens/`.

#### Dev Mode (Development)
Uses local project directory for data. Activates when standalone env vars are absent.

### Claude Desktop Configuration Example

```json
{
  "mcpServers": {
    "crashlens": {
      "command": "npx",
      "args": ["-y", "@crashlens_maq/mcp"],
      "env": {
        "CRASHLENS_STATE": "virginia",
        "CRASHLENS_JURISDICTION": "henrico",
        "CRASHLENS_ROAD_TYPE": "all_roads",
        "CRASHLENS_API_KEY": "clmcp_your_key_here"
      }
    }
  }
}
```

---

## 2. MCP Tools (22 total)

### Category A: Crash Data Tools (Tools 1-5)
**Source:** `mcp-server/tools/crash-tools.js`

#### Tool 1: `query_crashes`
Query crash records with flexible filters. Returns matching crash summaries.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `route` | string | No | — | Filter by route/road name (partial match) |
| `node` | string | No | — | Filter by intersection node ID (partial match) |
| `severity` | string[] | No | — | Filter by severity: `K`, `A`, `B`, `C`, `O` |
| `date_start` | string | No | — | Start date (YYYY-MM-DD) |
| `date_end` | string | No | — | End date (YYYY-MM-DD) |
| `collision_type` | string | No | — | Filter by collision type (partial match) |
| `weather` | string | No | — | Filter by weather condition (partial match) |
| `factors` | string[] | No | — | Contributing factors: `ped`, `bike`, `alcohol`, `speed`, `distracted`, `night`, `hitrun` |
| `limit` | number | No | 100 | Max records to return |

**Returns:** `{ dataContext, total, returned, limit, crashes[] }` where each crash has: `id, date, year, severity, route, node, collision, weather, light, ped, bike, alcohol, speed, x, y, jurisdiction`

#### Tool 2: `get_crash_statistics`
Get aggregate crash statistics — county-wide or for a specific route/intersection.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `route` | string | No | — | Route name to analyze |
| `node` | string | No | — | Intersection node to analyze |
| `date_start` | string | No | — | Start date (YYYY-MM-DD) |
| `date_end` | string | No | — | End date (YYYY-MM-DD) |

**Returns:** `{ dataContext, total, severity{K,A,B,C,O}, epdo, collisionTypes, contributingFactors, weatherConditions, lightConditions, pedestrianInvolved, bicycleInvolved, temporal, weekdayVsWeekend, byYear }`

#### Tool 3: `calculate_epdo`
Calculate EPDO score from severity counts. Optionally use state-specific weights.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `K` | number | Yes | — | Fatal crash count |
| `A` | number | Yes | — | Serious injury (A) count |
| `B` | number | Yes | — | Minor injury (B) count |
| `C` | number | Yes | — | Possible injury (C) count |
| `O` | number | Yes | — | Property damage only count |
| `state_fips` | string | No | — | State FIPS code (e.g., `"51"` for Virginia) |

**Returns:** `{ epdo, weights, weightSource, source, breakdown{K,A,B,C,O} }`

#### Tool 4: `analyze_hotspots`
Identify and rank crash hotspot locations.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `type` | enum | Yes | — | `route` or `intersection` |
| `min_crashes` | number | No | 5 | Minimum crashes to qualify |
| `sort_by` | enum | No | `epdo` | `epdo`, `total`, `ka`, `perYear` |
| `limit` | number | No | 20 | Max hotspots to return |

**Returns:** `{ dataContext, type, totalLocations, qualifyingLocations, dataYears, sortedBy, hotspots[] }`

#### Tool 5: `build_crash_profile`
Generate a detailed crash profile for a specific route or intersection.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `route` | string | No* | — | Route name (*at least one of route/node required) |
| `node` | string | No* | — | Intersection node ID |
| `date_start` | string | No | — | Start date (YYYY-MM-DD) |
| `date_end` | string | No | — | End date (YYYY-MM-DD) |

**Returns:** `{ dataContext, location{route, node}, profile }` — profile includes severityDist, collisionTypes, weatherDist, lightDist, contributingFactors, pedInvolved, bikeInvolved, temporal patterns, extended analysis

---

### Category B: Analysis Tools (Tools 6-7, 18, 20)
**Source:** `mcp-server/tools/analysis-tools.js`

#### Tool 6: `calculate_baselines`
Calculate county-wide baseline crash rates for statistical comparison.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| *(none)* | — | — | — | Uses all loaded crash data |

**Returns:** `{ dataContext, severityRates, patternRates, perLocationAverages }`

#### Tool 7: `analyze_over_representation`
Calculate Over-Representation Index (ORI) for a location vs county baselines.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `route` | string | No* | — | Route name (*at least one required) |
| `node` | string | No* | — | Intersection node ID |

**Returns:** `{ dataContext, location, crashCount, patterns, overRepresentation, significance }`

#### Tool 18: `analyze_crash_trends`
Analyze temporal crash trends: year-over-year changes, severity trends, time-of-day and day-of-week patterns.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `route` | string | No | — | Route name |
| `node` | string | No | — | Intersection node ID |
| `date_start` | string | No | — | Start date (YYYY-MM-DD) |
| `date_end` | string | No | — | End date (YYYY-MM-DD) |

**Returns:** `{ dataContext, totalCrashes, location, severityTrend, yearOverYear, temporal }`

#### Tool 20: `compare_locations`
Side-by-side comparison of two locations.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `location_a` | object | Yes | — | `{ route?, node? }` — first location |
| `location_b` | object | Yes | — | `{ route?, node? }` — second location |
| `date_start` | string | No | — | Start date (YYYY-MM-DD) |
| `date_end` | string | No | — | End date (YYYY-MM-DD) |

**Returns:** `{ dataContext, locationA, locationB, comparison{totalDiff, epdoDiff, higherCrashCount, higherEPDO} }`

---

### Category C: Infrastructure & Discovery Tools (Tools 8-12, 21-22)
**Source:** `mcp-server/tools/infrastructure-tools.js`

#### Tool 8: `evaluate_signal_warrant`
Evaluate MUTCD signal warrant criteria for an intersection based on traffic volumes.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `major_volume` | number | Yes | — | Major street hourly volume (veh/hr) |
| `minor_volume` | number | Yes | — | Minor street hourly volume (veh/hr) |
| `major_lanes` | number | Yes | — | Major street approach lanes |
| `minor_lanes` | number | Yes | — | Minor street approach lanes |
| `community_pop` | number | No | 50000 | Community population |
| `speed_limit` | number | No | 35 | Speed limit (mph) |

**Returns:** `{ warrant1{met, majorVolume, minorVolume, minorThreshold, laneConfig, reductionFactor, description}, parameters }`

#### Tool 9: `score_grant_eligibility`
Score a location for traffic safety grant funding eligibility (HSIP, SS4A, 402, 405d).

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `route` | string | No* | — | Route name (*at least one required) |
| `node` | string | No* | — | Intersection node ID |
| `scoring_profile` | enum | No | `balanced` | `balanced`, `hsip`, `ss4a`, `402`, `405d` |

**Returns:** `{ dataContext, location, crashCount, scoringProfile, scores, eligibility }`

#### Tool 10: `get_forecasts`
Get crash forecasts for a state/jurisdiction.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `state` | string | Yes | — | State name (e.g., `"virginia"`) |
| `jurisdiction` | string | Yes | — | Jurisdiction name (e.g., `"henrico"`) |
| `road_type` | enum | No | `all_roads` | `all_roads`, `county_roads`, `no_interstate` |

**Returns:** `{ dataContext, state, jurisdiction, roadType, forecasts }`

#### Tool 11: `search_grants`
Search available traffic safety grants.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `program` | string | No | — | Grant program (HSIP, SS4A, 402, 405b, 405c, 405d) |
| `keyword` | string | No | — | Keyword search in title/description |
| `status` | string | No | — | Grant status filter |

**Returns:** `{ dataContext, total, grants[] }` (max 50)

#### Tool 12: `get_jurisdiction_info`
Get jurisdiction metadata, available configurations, and EPDO weights.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `state_fips` | string | No | — | State FIPS code (e.g., `"51"`) |
| `state_name` | string | No | — | State name (e.g., `"virginia"`) |

**Returns:** `{ epdoWeights?, stateConfig?, availableStates, jurisdictionCount, jurisdictionSample[] }`

#### Tool 21: `list_locations`
List available routes and intersections in the crash data with crash counts.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `type` | enum | No | `both` | `routes`, `intersections`, `both` |
| `min_crashes` | number | No | 1 | Minimum crash count |
| `search` | string | No | — | Name filter (partial match) |
| `sort_by` | enum | No | `total` | `name`, `total`, `epdo` |
| `limit` | number | No | 50 | Max locations to return |

**Returns:** `{ routes[]?, totalRoutes?, intersections[]?, totalIntersections? }`

#### Tool 22: `get_data_quality`
Assess data quality and completeness metrics for the crash dataset.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `route` | string | No | — | Scope to a route |
| `node` | string | No | — | Scope to an intersection |

**Returns:** `{ totalRecords, scope, dateRange, fieldCompleteness{}, unknownValues{}, recommendations[] }`

Fields assessed: Severity, Collision Type, Weather, Light Condition, Date, Time, Route, Node, Coordinates (X/Y), Jurisdiction.

---

### Category D: CMF/Countermeasure Tools (Tools 13-15)
**Source:** `mcp-server/tools/cmf-tools.js`

#### Tool 13: `search_cmf_database`
Search the FHWA CMF Clearinghouse database of 808 countermeasures.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `crash_types` | string[] | No | — | `angle`, `rear_end`, `pedestrian`, `run_off_road`, `head_on`, `sideswipe`, `bicycle`, `speed`, `nighttime`, etc. |
| `category` | string | No | — | CMF category: `Roadway`, `Intersection geometry`, `Speed management`, `Shoulder treatments`, etc. |
| `location_type` | enum | No | `both` | `intersection`, `segment`, `both` |
| `area_type` | enum | No | `all` | `all`, `rural`, `urban` |
| `min_rating` | number | No | 3 | Minimum star rating (1-5) |
| `proven_only` | boolean | No | false | FHWA Proven Safety Countermeasures only |
| `hsm_only` | boolean | No | false | Highway Safety Manual included only |
| `keywords` | string | No | — | Keyword search in names |
| `limit` | number | No | 15 | Max results |

**Returns:** `{ dataContext, totalInDatabase, returned, filters, results[] }`

#### Tool 14: `recommend_countermeasures`
Analyze a location crash profile and automatically recommend best-matching FHWA countermeasures.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `route` | string | No* | — | Route name (*at least one required) |
| `node` | string | No* | — | Intersection node ID |
| `date_start` | string | No | — | Start date (YYYY-MM-DD) |
| `date_end` | string | No | — | End date (YYYY-MM-DD) |
| `max_results` | number | No | 10 | Max recommendations |

**Returns:** `{ dataContext, location, crashCount, epdo, topCollisionTypes[], recommendations[] }`

#### Tool 15: `calculate_combined_cmf`
Calculate combined effect of applying multiple countermeasures (FHWA successive multiplication method).

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `cmf_values` | number[] | Yes | — | Individual CMF values to combine |
| `names` | string[] | No | — | Countermeasure names for labeling |

**Returns:** `{ combinedCMF, reductionPct, individual[], method }`

---

### Category E: Safety Tools (Tools 16-17, 19)
**Source:** `mcp-server/tools/safety-tools.js`

#### Tool 16: `analyze_safety_category`
Analyze crashes for a specific systemic safety focus category.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `category` | enum | Yes | — | One of 21 categories (see below) |
| `route` | string | No | — | Scope to a specific route |
| `date_start` | string | No | — | Start date (YYYY-MM-DD) |
| `date_end` | string | No | — | End date (YYYY-MM-DD) |

**21 Safety Categories:** `curves`, `workzone`, `school`, `guardrail`, `senior`, `young`, `roaddeparture`, `lgtruck`, `pedestrian`, `bicycle`, `speed`, `impaired`, `intersection`, `nighttime`, `distracted`, `motorcycle`, `hitrun`, `weather`, `animal`, `unrestrained`, `drowsy`

**Returns:** Category-specific crash analysis with counts, severity breakdown, patterns.

#### Tool 17: `get_safety_overview`
Comprehensive overview of all 21 safety categories for the jurisdiction.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `date_start` | string | No | — | Start date (YYYY-MM-DD) |
| `date_end` | string | No | — | End date (YYYY-MM-DD) |
| `sort_by` | enum | No | `epdo` | `total`, `epdo`, `ka`, `pct` |

**Returns:** `{ dataContext, totalCrashes, categoriesAnalyzed, sortedBy, categories[], topConcerns[5] }`

#### Tool 19: `run_before_after_study`
Run a before/after crash study to evaluate treatment effectiveness.

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `route` | string | No* | — | Route name (*at least one required) |
| `node` | string | No* | — | Intersection node ID |
| `treatment_date` | string | Yes | — | Treatment date (YYYY-MM-DD) |
| `construction_months` | number | No | 3 | Months to exclude during construction |
| `study_period_years` | number | No | 3 | Years for before/after period |
| `method` | enum | No | `naive` | `naive` or `empirical_bayes` |

**Returns:** `{ dataContext, location, beforePeriod, afterPeriod, crashReduction, significance, method }`

---

## 3. MCP Resources (6 total)

**Source:** `mcp-server/index.js` (lines 84-165)

| Resource Name | URI | Description |
|---------------|-----|-------------|
| `data-summary` | `crashlens://data/summary` | Data context + summary (jurisdiction, total records, date range) |
| `epdo-weights` | `crashlens://config/epdo-weights` | All state-specific EPDO weights (25+ states) |
| `available-states` | `crashlens://config/states` | List of states with data available |
| `cmf-database-summary` | `crashlens://data/cmf-summary` | CMF database version, record count, categories |
| `safety-categories` | `crashlens://config/safety-categories` | List of 21 safety focus categories |
| `cmf-categories` | `crashlens://config/cmf-categories` | CMF countermeasure categories with record counts |

---

## 4. Node.js API Server Endpoints

**Source:** `server/qdrant-proxy.js`
**Port:** 3001 (configurable via `PROXY_PORT` env var)
**Nginx proxy:** `/api/*` → `http://127.0.0.1:3001/`

All endpoints return JSON with CORS headers. From the browser, prefix all paths with `/api`.

### Health

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Server health check. Returns `{ status: "ok", version }` |

### Notification / Email (Brevo)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/notify/status` | None | Check Brevo email configuration status |
| `POST` | `/notify/send` | None | Send email notification via Brevo |
| `POST` | `/notify/upload-report` | None | Email a report with optional PDF attachment |

### Subscriptions

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/subscribe` | None | Subscribe email to notifications |

### R2 Storage (Cloudflare)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/r2/status` | None | Check R2 direct (S3) configuration status |
| `POST` | `/r2/upload-geocoded` | None | Upload geocoded crash data to R2 via S3 SDK |
| `GET` | `/r2/worker-status` | None | Check R2 Worker proxy configuration status |
| `GET` | `/r2/worker-list?prefix=&delimiter=` | None | List R2 objects via Worker proxy |
| `POST` | `/r2/worker-upload` | None | Upload file to R2 via Worker proxy (uses `R2_WORKER_SECRET`) |

### Subscriber Management (R2-backed)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/subscribers/save` | None | Save subscriber list to R2 |
| `GET` | `/subscribers/load?jurisdiction=` | None | Load subscriber list from R2 |

### Schedule Management (R2-backed)

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/schedule/save` | None | Save report schedule to R2 |
| `GET` | `/schedule/list` | None | List all saved schedules |
| `DELETE` | `/schedule/:id` | None | Delete a schedule by ID |

### Forecasts

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/forecasts/:state/:jurisdiction/:roadType` | None | Get crash forecasts (5-min in-memory cache) |
| `GET` | `/forecasts/check/:state/:jurisdiction` | None | Check which road types have forecast data |

### Geocoding

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/geocode?address=&lat=&lon=` | None | Forward/reverse geocode via Census TigerWeb |

### Stripe Payment Processing

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/stripe/status` | None | Check Stripe configuration status |
| `POST` | `/stripe/create-checkout-session` | Firebase Token | Create Stripe Checkout session. Body: `{ plan, billingCycle, userId, email, successUrl?, cancelUrl? }` |
| `POST` | `/stripe/webhook` | Stripe Signature | Handle Stripe webhook events (checkout.session.completed, invoice.paid, subscription.updated/deleted) |
| `POST` | `/stripe/create-portal-session` | Firebase Token | Create Stripe Customer Portal session. Body: `{ customerId, returnUrl? }` |

### MCP API Key Management

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/mcp/validate-key` | None | Validate MCP API key + check subscription. Body: `{ apiKey }` |
| `POST` | `/mcp/generate-key` | Firebase Token | Generate new MCP API key for user. Returns: `{ apiKey }` |
| `POST` | `/mcp/revoke-key` | Firebase Token | Revoke user's MCP API key |

### Qdrant Vector Database Proxy

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `*` | `/?path=<qdrant-api-path>` | None (server uses `QDRANT_API_KEY`) | Proxy any request to Qdrant Cloud (avoids CORS) |

---

## 5. Environment Variables

**Source:** `.env.example`, `entrypoint.sh`

### Client-Side (injected into `config/api-keys.json` at container startup)

These are set in Coolify Dashboard and written to `config/api-keys.json` by `entrypoint.sh` using `jq`.

| Variable | Key in api-keys.json | Purpose |
|----------|---------------------|---------|
| `MAPBOX_ACCESS_TOKEN` | `mapbox.accessToken` | Satellite tiles, geocoding search |
| `GOOGLE_MAPS_API_KEY` | `google.mapsApiKey` | Street View imagery |
| `MAPILLARY_ACCESS_TOKEN` | `mapillary.accessToken` | Street-level imagery |
| `FIREBASE_API_KEY` | `firebase.apiKey` | Firebase Authentication |
| `FIREBASE_AUTH_DOMAIN` | `firebase.authDomain` | Firebase Auth domain |
| `FIREBASE_PROJECT_ID` | `firebase.projectId` | Firebase project |
| `FIREBASE_STORAGE_BUCKET` | `firebase.storageBucket` | Firebase storage |
| `FIREBASE_MESSAGING_SENDER_ID` | `firebase.messagingSenderId` | Firebase messaging |
| `FIREBASE_APP_ID` | `firebase.appId` | Firebase app ID |
| `STRIPE_PUBLISHABLE_KEY` | `stripe.publishableKey` | Stripe client-side key |
| `R2_WORKER_URL` | `r2Worker.workerUrl` | R2 Worker proxy URL |
| `R2_PUBLIC_URL` | `r2Worker.publicUrl` | R2 public read URL (default: `https://data.aicreatesai.com`) |

### Server-Side (used directly by Node.js proxy, never exposed to frontend)

| Variable | Purpose |
|----------|---------|
| `STRIPE_SECRET_KEY` | Stripe server-side API key |
| `STRIPE_WEBHOOK_SECRET` | Stripe webhook signature verification |
| `STRIPE_PRICE_INDIVIDUAL_MONTHLY` | Stripe Price ID for Individual Monthly |
| `STRIPE_PRICE_INDIVIDUAL_ANNUAL` | Stripe Price ID for Individual Annual |
| `STRIPE_PRICE_TEAM_MONTHLY` | Stripe Price ID for Team Monthly |
| `STRIPE_PRICE_TEAM_ANNUAL` | Stripe Price ID for Team Annual |
| `APP_URL` | Application URL for Stripe redirects (default: `https://crashlens.aicreatesai.com`) |
| `FIREBASE_SERVICE_ACCOUNT` | Firebase Admin SDK JSON (single-line) |
| `QDRANT_ENDPOINT` | Qdrant Cloud endpoint URL |
| `QDRANT_API_KEY` | Qdrant Cloud API key |
| `BREVO_API_KEY` | Brevo email API key (starts with `xkeysib-`) |
| `BREVO_SMTP_LOGIN` | Brevo SMTP login (fallback mode) |
| `BREVO_SMTP_PASSWORD` | Brevo SMTP password (fallback mode) |
| `NOTIFICATION_FROM_EMAIL` | Verified sender email for Brevo |
| `CF_ACCOUNT_ID` | Cloudflare account ID (R2 direct) |
| `CF_R2_ACCESS_KEY_ID` | R2 S3-compatible access key |
| `CF_R2_SECRET_ACCESS_KEY` | R2 S3-compatible secret key |
| `R2_BUCKET_NAME` | R2 bucket name (default: `crash-lens-data`) |
| `R2_WORKER_SECRET` | R2 Worker upload secret (`X-Upload-Secret` header) |
| `PROXY_PORT` | API server port (default: `3001`) |

### MCP Server-Specific (for standalone mode)

| Variable | Purpose |
|----------|---------|
| `CRASHLENS_STATE` | State name for data loading |
| `CRASHLENS_JURISDICTION` | Jurisdiction name for data loading |
| `CRASHLENS_ROAD_TYPE` | Road type filter (default: `all_roads`) |
| `CRASHLENS_API_KEY` | MCP API key for authentication |

---

## 6. Configuration Files

### `config.json` (Root)
Central application configuration containing:
- **Jurisdiction definitions** (133 Virginia entries): name, type, FIPS, coordinates, bounding boxes, education districts
- **API endpoint configs**: Mapbox, Google Maps, Mapillary, Firebase, TigerWeb, R2 Worker
- State designations (VA, CO)

### `config/api-keys.json` (Runtime-generated, gitignored)
Client-side API keys. Generated by `entrypoint.sh` from environment variables. Template: `config/api-keys.example.json`.

```json
{
  "mapbox": { "accessToken": "..." },
  "google": { "mapsApiKey": "..." },
  "mapillary": { "accessToken": "..." },
  "firebase": { "apiKey": "...", "authDomain": "...", "projectId": "...", "storageBucket": "...", "messagingSenderId": "...", "appId": "..." },
  "stripe": { "publishableKey": "..." },
  "r2Worker": { "workerUrl": "...", "publicUrl": "..." }
}
```

### `config/settings.json`
Application settings (UI preferences, feature flags).

### `states/{state}/config.json`
Per-state configuration containing:
- `dotName`: State DOT abbreviation
- `epdoWeights`: State-specific EPDO severity weights
- `columnMapping`: Source CSV → standardized column mapping
- `derivedFields`: Computed fields definitions
- `jurisdictionFiltering`: Rules for filtering by jurisdiction

### `states/{state}/hierarchy.json`
Administrative hierarchy:
- `allCounties`: FIPS → name mapping
- `regions`: County groupings
- `mpos`: Metropolitan Planning Organizations

---

## 7. Data Pipeline Scripts

**Location:** `scripts/`

### Core Pipeline (Execution Order)

```
Raw CSV (from DOT/ArcGIS)
    ↓
1. process_crash_data.py        ← Master orchestrator (4 stages: CONVERT → VALIDATE → GEOCODE → SPLIT)
    ↓
2. state_adapter.py             ← Auto-detect state, normalize to standard format
    ↓
3. validate_data.py             ← Data quality checks with state-specific bounds
    ↓
4. geocode_data.py              ← Fill missing GPS coordinates
    ↓
5. split_jurisdictions.py       ← Split statewide CSV into per-jurisdiction files
    ↓
6. split_road_type.py           ← Create 3 road-type variants per jurisdiction
    ↓
7. generate_aggregates.py       ← Pre-compute JSON summaries by State/Region/MPO
    ↓
8. upload-to-r2.py              ← Upload to Cloudflare R2 storage
```

### Supporting Scripts

| Script | Purpose |
|--------|---------|
| `pipeline_server.py` | HTTP bridge (port 5050) between browser UI and Python pipeline |
| `resolve_scope.py` | Translate user selection (state + scope + selection) to jurisdiction lists |
| `aggregate_by_scope.py` | Region/MPO specific aggregations |
| `generate_forecast.py` | Crash prediction forecasts |
| `generate_all_hierarchies.py` | Region/MPO hierarchy generation |
| `generate_state_folders.py` | Create state-specific directory structures |
| `fetch_jurisdiction_bboxes.py` | Boundary box management |
| `create_r2_folders.py` | R2 storage structure setup |
| `init_cache.py` | Initialize pipeline cache |
| `download_crash_data.py` | Virginia Roads ArcGIS API client |
| `merge_bboxes_to_config.py` | Merge bounding boxes into config.json |
| `rebuild_road_type_csvs.py` | Rebuild road type CSV variants |

### Pipeline Server API (`pipeline_server.py`, port 5050)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/pipeline/run` | Upload CSV, trigger processing (max 5 GB, 8 MB chunks) |
| `GET` | `/api/pipeline/status` | Check pipeline progress |
| `GET` | `/api/pipeline/states` | List supported states |
| `GET` | `/health` | Health check |

### Supported States (Auto-Detection)

| State | Detection Signature (CSV Headers) |
|-------|----------------------------------|
| Colorado (CDOT) | `"CUID"`, `"System Code"`, `"Injury 00/04"` |
| Virginia (TREDS) | `"Document Nbr"`, `"Crash Severity"`, `"RTE Name"`, `"SYSTEM"` |
| Maryland (ACRS) | Both MoCo and statewide variants |

### CI/CD Triggers

**Primary:** `.github/workflows/pipeline.yml` (Unified Pipeline v6)

6-stage processing:
1. Init Cache → 2. Split Jurisdiction → 3. Split Road Type → 4. Aggregate → 5. Upload to R2 → 6. Predict & Manifest

Inputs: `state`, `scope` (jurisdiction/region/mpo/statewide), `selection`, `data_source`, `skip_forecasts`, `dry_run`

**Scheduled Downloads:** `.github/workflows/download-data.yml`
- Crash data: First Monday of month at 11:00 UTC
- Grants data: Every Monday at 11:00 UTC
- CMF data: Quarterly (Jan, Apr, Jul, Oct)

---

## 8. Shared Libraries (MCP Server)

**Location:** `mcp-server/lib/`

| Module | Exports | Purpose |
|--------|---------|---------|
| `constants.js` | `COL`, `EPDO_WEIGHTS_DEFAULT`, `EPDO_PRESETS`, `STATE_EPDO_WEIGHTS` | Column indices, severity weights |
| `epdo.js` | `calcEPDO()`, `getStateEPDOWeights()`, `isYes()` | EPDO score calculation |
| `crash-profile.js` | `buildLocationCrashProfile()`, `buildDetailedLocationProfile()` | Crash profile generation |
| `baselines.js` | `calculateCountyBaselines()`, `calculateORI()`, `testPatternSignificance()`, `calculatePSI()` | Statistical baselines |
| `hotspots.js` | `scoreAndRank()` | Hotspot scoring/ranking |
| `signal-warrant.js` | `interpolateThreshold()`, `getLaneConfig()`, `getReductionFactor()`, `WARRANT_1_CURVES` | Signal warrant evaluation |
| `grant-ranking.js` | `scoreGrantEligibility()` | Grant eligibility scoring |
| `cmf.js` | `searchCMF()`, `recommendCountermeasures()`, `calculateCombinedCMF()` | CMF database operations |
| `safety-focus.js` | `analyzeSafetyCategory()`, `analyzeAllCategories()`, `listCategories()` | Safety category analysis |
| `trends.js` | `calculateSeverityTrend()`, `analyzeTemporalPatterns()`, `calculateYearOverYearChange()` | Temporal trend analysis |
| `before-after.js` | `runBeforeAfterStudy()` | Before/after study analysis |
| `data-loader.js` | `init()`, `initStandalone()`, `loadCrashData()`, `buildAggregates()`, `filterCrashes()`, `loadCMFData()`, `loadGrantData()`, `loadForecasts()`, `getDataContext()`, `getDataSummary()`, `listStates()`, `loadStateConfig()`, `loadConfig()`, `getCMFRecords()`, `getCMFMetadata()`, `analyzeCrashPatterns()` | Data loading, caching, filtering |

### Key Constants

#### COL Object (Column Mapping)
```javascript
COL.ID = 'Document Nbr'       COL.YEAR = 'Crash Year'
COL.DATE = 'Crash Date'       COL.TIME = 'Crash Military Time'
COL.SEVERITY = 'Crash Severity'  COL.COLLISION = 'Collision Type'
COL.WEATHER = 'Weather Condition'  COL.LIGHT = 'Light Condition'
COL.ROUTE = 'RTE Name'        COL.NODE = 'Node'
COL.X = 'x'                   COL.Y = 'y'
COL.JURISDICTION = 'Physical Juris Name'
COL.PED = 'Pedestrian?'       COL.BIKE = 'Bike?'
COL.ALCOHOL = 'Alcohol?'      COL.SPEED = 'Speed?'
COL.DISTRACTED = 'Distracted?'  COL.HITRUN = 'Hitrun?'
COL.SENIOR = 'Senior?'        COL.YOUNG = 'Young?'
COL.NIGHT = 'Night?'          COL.MOTORCYCLE = 'Motorcycle?'
COL.FUNC_CLASS = 'Functional Class'  COL.AREA_TYPE = 'Area Type'
COL.ROAD_SYSTEM = 'SYSTEM'    COL.MP = 'RNS MP'
```

#### EPDO Weights (Default — HSM 2010)
```javascript
{ K: 883, A: 94, B: 21, C: 11, O: 1 }
```

#### State-Specific EPDO Weights (by FIPS Code)

| FIPS | State | K | A | B | C | O |
|------|-------|---|---|---|---|---|
| 51 | Virginia (VDOT 2025) | 1032 | 53 | 16 | 10 | 1 |
| 06 | California (Caltrans 2023) | 1100 | 58 | 17 | 11 | 1 |
| 48 | Texas (TxDOT 2023) | 920 | 55 | 14 | 9 | 1 |
| 12 | Florida (FDOT 2023) | 985 | 50 | 15 | 9 | 1 |
| 36 | New York (NYSDOT 2023) | 1050 | 55 | 15 | 10 | 1 |
| 17 | Illinois (IDOT 2023) | 850 | 45 | 10 | 5 | 1 |
| 37 | North Carolina (NCDOT 2023) | 770 | 77 | 8 | 8 | 1 |
| 25 | Massachusetts (MassDOT 2024) | 1200 | 60 | 18 | 12 | 1 |
| 04 | Arizona (ADOT 2023) | 462 | 62 | 12 | 5 | 1 |

---

## 9. Authentication and Authorization

### Client-Side: Firebase Auth
- **Module:** `assets/js/auth.js` (`CrashLensAuth`)
- **Providers:** Google OAuth + Email/Password
- **Login page:** `login/index.html`

### Server-Side: Firebase Admin
- Token verification via `Authorization: Bearer <firebase-id-token>` header
- Used by Stripe and MCP key management endpoints
- Initialized lazily from `FIREBASE_SERVICE_ACCOUNT` env var

### MCP API Key
- **Format:** `clmcp_` + 32 hex characters (e.g., `clmcp_a1b2c3d4e5f6...`)
- **Generation:** `POST /mcp/generate-key` (requires Firebase Auth)
- **Validation:** `POST /mcp/validate-key` (checks Firestore `users` collection)
- **Revocation:** `POST /mcp/revoke-key` (requires Firebase Auth)
- **Storage:** Firestore `users/{uid}.mcpApiKey`

### Plan-Based Limits

| Plan | AI Query Limit | Seats |
|------|---------------|-------|
| `trial` | 0 | 1 |
| `free_trial` | 0 | 1 |
| `individual` | 100 | 1 |
| `team` | 500 | 5 |
| `agency` | 1000 | unlimited |

### Subscription Status Check
Active subscription requires one of:
- `status` in `['active', 'trialing', 'past_due']` (for paid plans)
- `trialEndsAt > now` (for trial plan)
- `status !== 'pending_verification'`

---

## 10. Infrastructure

### Docker Container Architecture

```
┌─────────────────────────────────────────────┐
│  Docker Container (Coolify-managed)          │
│                                              │
│  entrypoint.sh                               │
│    ├── Reads env vars from Coolify Dashboard │
│    ├── Generates config/api-keys.json (jq)   │
│    └── Starts supervisord                    │
│                                              │
│  supervisord                                 │
│    ├── Nginx (port 80)                       │
│    │     ├── Static files (/usr/share/nginx/html)  │
│    │     └── Proxy: /api/* → localhost:3001  │
│    │                                         │
│    └── Node.js (port 3001)                   │
│          └── server/qdrant-proxy.js          │
│              ├── Qdrant proxy                │
│              ├── Stripe payments             │
│              ├── Brevo email                 │
│              ├── R2 storage                  │
│              ├── Forecast cache              │
│              └── MCP key management          │
└─────────────────────────────────────────────┘
```

### Nginx Configuration (`nginx.conf`)
- Serves static files from `/usr/share/nginx/html`
- Proxies `/api/*` to `http://127.0.0.1:3001/`
- Port 80 (HTTP)

### Data Storage (Cloudflare R2)

**Bucket:** `crash-lens-data`
**Public URL:** `https://data.aicreatesai.com`

R2 folder hierarchy:
```
{state}/
  ├── {jurisdiction}/
  │     ├── all_roads.csv
  │     ├── county_roads.csv
  │     ├── no_interstate.csv
  │     └── forecasts/
  ├── _statewide/
  │     ├── aggregates.json
  │     └── county_summary.json
  ├── _region/{region}/
  │     └── aggregates.json
  └── _mpo/{mpo}/
        └── aggregates.json
```

### Configuration Injection Flow

```
Coolify Dashboard (env vars)
    ↓
Docker Container starts → entrypoint.sh runs
    ↓
jq generates config/api-keys.json from env vars
    ↓
supervisord starts Nginx + Node.js
    ↓
Client JS reads api-keys.json at runtime
Node.js reads env vars directly (STRIPE_SECRET_KEY, FIREBASE_SERVICE_ACCOUNT, etc.)
```

---

## Quick Reference: File Paths

| File | Purpose |
|------|---------|
| `.mcp.json` | MCP server registration |
| `mcp-server/index.js` | MCP server entry (22 tools, 6 resources) |
| `mcp-server/package.json` | MCP package (`@crashlens_maq/mcp` v1.2.0) |
| `mcp-server/tools/crash-tools.js` | Tools 1-5 (crash queries) |
| `mcp-server/tools/analysis-tools.js` | Tools 6-7, 18, 20 (analysis) |
| `mcp-server/tools/infrastructure-tools.js` | Tools 8-12, 21-22 (infra/discovery) |
| `mcp-server/tools/cmf-tools.js` | Tools 13-15 (countermeasures) |
| `mcp-server/tools/safety-tools.js` | Tools 16-17, 19 (safety) |
| `mcp-server/lib/constants.js` | COL, EPDO weights |
| `mcp-server/lib/data-loader.js` | Data loading + standalone mode |
| `server/qdrant-proxy.js` | Node.js API server (20+ endpoints) |
| `.env.example` | Environment variables reference |
| `config/api-keys.example.json` | Client API keys template |
| `config.json` | Application configuration |
| `entrypoint.sh` | Docker env → api-keys.json injection |
| `supervisord.conf` | Process manager config |
| `nginx.conf` | Nginx web server config |
| `Dockerfile` | Docker container definition |
| `scripts/process_crash_data.py` | Master pipeline orchestrator |
| `scripts/state_adapter.py` | Multi-state format adapter |
| `scripts/pipeline_server.py` | HTTP pipeline bridge (port 5050) |
| `.github/workflows/pipeline.yml` | Main CI/CD pipeline |
| `states/{state}/config.json` | Per-state configuration |
| `states/{state}/hierarchy.json` | Administrative hierarchy |
