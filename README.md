# Odoo ↔ Shopify Connector

A production-ready Flask application that keeps Odoo ERP and Shopify stores in real-time sync. Handles inventory, orders, fulfillments, returns, customers, and products across multiple Shopify stores from a single hosted service.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Core Workflows](#core-workflows)
3. [Scheduling & Automation](#scheduling--automation)
4. [Health Monitoring System](#health-monitoring-system)
5. [Database Schema](#database-schema)
6. [API Reference](#api-reference)
7. [Environment Variables](#environment-variables)
8. [Local Development](#local-development)
9. [Render Deployment Guide](#render-deployment-guide)
10. [Changelog](#changelog)
11. [Sentry Bug Reports & Fixes](#sentry-bug-reports--fixes)
12. [Project Scope & Roadmap](SCOPE.md)

---

## Architecture Overview

```
Shopify Store(s)
      │
      │  Webhooks (orders/create, products/create, inventory_levels/update …)
      ▼
┌─────────────────────────────────────────────────────────┐
│                    Flask App (app.py)                   │
│                                                         │
│  /webhooks/shopify  ──► Redis Queue (critical/default)  │
│  /auth              ──► OAuth + Webhook Registration    │
│  /dashboard         ──► Admin UI                        │
│  /api/dashboard/status ► Live Status JSON               │
│  /health  /ping     ──► Uptime Checks                   │
└────────────┬────────────────────────────────────────────┘
             │
     ┌───────┴────────┐
     │   RQ Workers   │  (2 queues)
     │                │
     │  critical ──► Inventory, Orders, Cancellations
     │  default  ──► Products, Customers, Fulfillments, Returns
     └───────┬────────┘
             │
    ┌────────▼────────┐          ┌──────────────────┐
    │   Odoo (XML-RPC)│          │  Supabase (PgSQL) │
    │   ERP Backend   │          │  app state + logs │
    └─────────────────┘          └──────────────────┘
```

**Key design decisions:**
- **Multi-tenant**: Every DB table is scoped by `shop_url` — a single deployment serves multiple Shopify stores
- **Two queues**: `critical` (10 min timeout, fast retries) for time-sensitive ops; `default` (2 hr timeout, slow retries) for heavy sync
- **Distributed locks**: Redis locks prevent duplicate concurrent syncs per shop
- **PgBouncer**: Supabase pooler URL (port 6543, transaction mode) keeps connection count low on free tier

---

## Core Workflows

### 1. OAuth Installation Flow

1. Merchant visits `GET /install?shop=mystore.myshopify.com`
2. App redirects to Shopify OAuth consent screen
3. Shopify POSTs back to `/auth/callback` with `code`
4. App exchanges code for permanent `access_token`, stores in `Shop` table
5. `automate_webhook_registration()` registers all 7 webhooks automatically
6. Merchant lands on `/dashboard`

### 2. Inventory Sync (Shopify → Odoo)

**Trigger:** Scheduled every 30 minutes per shop  
**Flow:**
1. `run_inventory_sync(shop_url)` enqueued by `_run_shop_schedule`
2. `perform_inventory_sync()` fetches all Shopify inventory levels via REST API
3. For each variant with a mapped SKU in `ProductMap`, pushes stock update to Odoo via `stock.quant` model
4. Logs result; updates `SyncHealth` row for entity `inventory`

**Webhook path:** `inventory_levels/update` → queues immediate inventory push

### 3. Order Sync (Shopify → Odoo)

**Trigger:** `orders/create` webhook + scheduled retry of `FailedSyncOrder` table  
**Flow:**
1. Webhook received → validates HMAC → checks rate limit (100/60s) → enqueues `process_new_order`
2. `process_new_order()` looks up or creates Odoo partner (via `CustomerMap`)
3. Creates Odoo sale order with lines mapped from Shopify line items using `ProductMap`
4. Marks order in `ProcessedOrder` (idempotency guard — duplicate webhooks are no-ops)
5. On failure: order saved to `FailedSyncOrder`; retried on schedule (up to 50 attempts, then email alert)

### 4. Fulfillment Sync (Odoo → Shopify)

**Trigger:** Scheduled every 60 minutes  
**Flow:**
1. Queries Odoo for stock pickings in `done` state created after `sync_start_date`
2. For each picking, finds corresponding Shopify order via `ProcessedOrder`
3. Creates Shopify Fulfillment with tracking number from Odoo carrier
4. Raises `RuntimeError` on connection failure so `SyncHealth` records the miss

### 5. Order Cancellation Sync (Odoo → Shopify)

**Trigger:** Scheduled every 5 minutes (most frequent — cancellations are time-sensitive)  
**Flow:**
1. Queries Odoo for cancelled sale orders
2. Issues Shopify `POST /orders/{id}/cancel` for matching orders
3. Raises on connection failure

### 6. Return / Refund Sync (Shopify → Odoo)

**Trigger:** `refunds/create` webhook + scheduled hourly  
**Flow:**
1. Fetches Shopify refunds since last sync
2. Creates Odoo reverse transfer (stock return) for each refunded line item
3. Updates `last_return_sync_success` timestamp on Shop record

### 7. Customer Sync (Shopify → Odoo)

**Trigger:** Scheduled every 12 hours  
**Flow:**
1. Fetches all Shopify customers updated since last sync
2. For each customer, upserts Odoo `res.partner` record
3. Stores mapping in `CustomerMap` for future order creation
4. Raises on connection failure

### 8. Product Sync (Shopify → Odoo)

**Trigger:** `products/create` webhook + scheduled every 24 hours  
**Flow:**
1. Fetches Shopify products/variants
2. Matches by SKU to Odoo `product.product` records
3. Upserts `ProductMap` rows; syncs images (hash-checked to avoid redundant uploads)
4. Raises on connection failure

### 9. Clearance Collection Sync (Odoo → Shopify)

**Trigger:** Runs automatically at the end of every inventory sync (fault-isolated — a clearance failure never affects inventory health)
**Purpose:** Sells stock sitting in the Clearance + Damaged Odoo locations — which the main inventory sync deliberately *excludes* — as separate discounted products that live only in a Clearance collection.

**Why separate products (not variants):** Shopify allows one price and one inventory pool per product. To show clearance stock at a different price *and* only in one collection, it must be its own product. `perform_clearance_sync` (`services/clearance.py`) auto-manages a mirror product per base SKU: `{sku}-CLR`.

**Flow:**
1. Query `stock.quant` for the configured clearance locations only (`location_id child_of clearance_locations`)
2. Aggregate quantity per product; read the earliest lot `expiration_date` from `stock.lot`
3. For each product with clearance stock, create/update the `{sku}-CLR` mirror: copy the base product's images/vendor/type, set price = `list_price × (1 − discount%)`, set `compare_at_price` to the original, add to the Clearance collection, and write the badge signals — the `Clearance` product tag, `clearance.is_clearance` (boolean) and `clearance.expiry_date` (date) metafields (the theme renders a "Clearance" badge + expiry from these)
4. Push the clearance quantity (pack-size divided, same as main sync) to the mirror's variant
5. Any previously-active mirror whose clearance stock has dropped to 0 is zeroed and set to **draft** (kept, not deleted — it reactivates when stock returns)

**Normal-product lifecycle** (so an out-of-stock normal listing doesn't sit alongside its clearance mirror):

| Normal stock | Clearance stock | Normal product | Clearance mirror |
|---|---|---|---|
| > 0 | > 0 | active | active |
| > 0 | 0 | active | draft |
| **0** | **> 0** | **drafted** | active |
| 0 | 0 | active (back up) | draft |

The base product is drafted **only** when normal stock hits 0 while clearance stock exists, and re-activated as soon as normal stock returns or clearance runs out. The connector records `base_drafted` on the `clearance_mirror` row and re-activates **only** products it drafted itself — a product the merchant drafted for another reason is never touched. Normal stock is read from the main sync's target locations minus excludes (so the decision matches what Shopify shows), which means the clearance locations must be in the exclude list.

**No double-counting:** the two location sets are disjoint (main sync excludes exactly what this pass includes), so a product stocked in both Pick/Bulk and Clearance shows the full quantity on its normal listing and the clearance quantity on the mirror.

**Order routing:** a `{sku}-CLR` line in an order maps back to the *base* Odoo product — `services/orders.py` strips the clearance suffix up front (before the create-new-product step) so no junk product is created, and the discounted line price is preserved via `manual_price=True`. Which Odoo location the line decrements from is governed by Odoo's own delivery/picking rules (configure a stock rule to source from the clearance location).

**Food-safety expiry cutoff:** stock that's expired or expiring too soon is never sold, no exceptions. Every lot's quantity is checked individually against `clearance_expiry_cutoff_days` (default 15) — if a lot carries a best-before date and `(expiry_date − today) <= cutoff_days`, that lot's quantity is excluded entirely from the mirror's sellable stock and from the displayed expiry date, regardless of how much of it sits in Odoo. A lot with **no** expiry date is unaffected (still sellable) — the cutoff only ever removes stock that carries a date and falls inside the window. If this drops a product's sellable clearance quantity to 0, it's treated exactly like normal stock running out: the mirror is zeroed/drafted, and if the base product had been hidden for having only clearance stock, it's restored to active (showing its normal 0-stock state) via the same zero-out/reactivate lifecycle above — no separate code path. A mixed batch (some lots fine, some too close to expiry) still sells the fine lots; only the near/past-expiry portion is withheld.

**Settings** (`app_settings`): `clearance_enabled`, `clearance_locations`, `clearance_discount_pct` (default 40), `clearance_collection_id`, `clearance_sku_suffix` (default `-CLR`), `clearance_expiry_cutoff_days` (default 15).

---

## Scheduling & Automation

All scheduling is handled inside `_run_shop_schedule()`, called by APScheduler every **5 minutes**. It uses Redis TTL keys to gate each entity's interval:

| Entity            | Interval | Queue    | Redis TTL Key                    | RQ Retry Policy          |
|-------------------|----------|----------|----------------------------------|--------------------------|
| Inventory         | 30 min   | critical | `last_inv_sync_{shop}`           | 3× (5m / 15m / 30m)      |
| Cancellations     | 5 min    | critical | `last_cancel_sync_{shop}`        | 3× (5m / 15m / 30m)      |
| Fulfillments      | 60 min   | default  | `last_fulfill_sync_{shop}`       | 3× (10m / 30m / 1h)      |
| Returns           | 60 min   | default  | `last_return_sync_{shop}`        | 3× (10m / 30m / 1h)      |
| Customers         | 12 h     | default  | `last_customer_sync_{shop}`      | 3× (10m / 30m / 1h)      |
| Products          | 24 h     | default  | `last_prod_sync_{shop}`          | 3× (10m / 30m / 1h)      |
| Failed Orders     | 15 min   | critical | `last_failed_order_retry_{shop}` | 5× (1m / 2m / 4m / 10m / 20m) |
| Product Scan      | 6 h      | default  | `last_scan_{shop}`               | —                        |
| Health Monitor    | 2 h      | default  | `last_health_monitor_{shop}`     | —                        |

`Force Run` button on dashboard clears all TTL keys and immediately enqueues every entity.

---

## Health Monitoring System

### SyncHealth Table

Every sync function is wrapped in `_run_with_health(entity, shop_url, fn)`:
- **On success**: sets `last_success_at`, `last_attempt_at`, resets `consecutive_failures = 0`
- **On failure**: sets `last_attempt_at`, increments `consecutive_failures`, stores `last_error`; re-raises so RQ retries fire

### sync_health_monitor

Runs every 2 hours per shop. For each entity, compares `last_attempt_at` to `now - (interval × 2)`:
- If stale (not attempted in 2× normal interval) → re-enqueues immediately
- If `FailedSyncOrder` count ≥ 3 → logs a warning event

### Order Health (Webhook-Driven)

Orders are event-driven (fire when a customer buys), not scheduled, so they have no fixed interval. `background_order_sync` writes to `SyncHealth` with entity `order` on every attempt. The dashboard shows an **Orders** row with a `webhook` label instead of a countdown — dot colour is based purely on `consecutive_failures` (green = all good, red = failures present).

### Dashboard Health Card

`GET /api/dashboard/status` returns JSON with:
- `queue_default` / `queue_critical` — job counts per queue
- `failed_orders` — count of orders in retry table
- `health` — array of per-entity rows with `last_success`, `last_attempt`, `consecutive_failures`, `status` (ok / warning / critical)
- `system` — Shopify/Odoo/worker liveness checks

Dashboard JS polls this endpoint every 30 seconds and renders color-coded dots (green / yellow / red).

---

## Database Schema

| Table               | Purpose                                                   |
|---------------------|-----------------------------------------------------------|
| `shop`              | One row per installed Shopify store; stores tokens + Odoo creds (password AES-encrypted) |
| `product_map`       | Links Shopify variant IDs to Odoo product IDs by SKU     |
| `customer_map`      | Links Shopify customer IDs to Odoo partner IDs           |
| `processed_orders`  | Idempotency log — prevents duplicate Odoo orders from duplicate webhooks |
| `failed_sync_orders`| Retry queue for orders that failed on first attempt (capped at 50 attempts) |
| `sync_logs`         | Append-only event log, scoped per shop                   |
| `app_settings`      | Key-value config store per shop (scan mode, alert email, etc.) |
| `sync_health`       | Per-entity health tracking: attempt time, success time, failure count |
| `clearance_mirror`  | Bridges a base SKU to its auto-managed `{sku}-CLR` clearance mirror (Shopify product/variant/inventory-item IDs, last qty, active flag) |

---

## API Reference

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| GET | `/install` | — | Start OAuth flow |
| GET | `/auth/callback` | Shopify HMAC | Complete OAuth, store token |
| GET | `/dashboard` | Session | Admin dashboard UI |
| POST | `/save_settings` | Session | Save Odoo credentials + config |
| POST | `/force_schedule` | Session | Immediately enqueue all syncs |
| GET | `/api/dashboard/status` | Session | Live queue + health JSON |
| GET | `/sync/logs` | Session | Recent sync log entries |
| POST | `/webhooks/shopify` | HMAC | Receive all Shopify webhooks |
| POST | `/webhooks/app_uninstalled` | HMAC | Handle uninstall, clean up |
| GET | `/ping` | — | Uptime check (returns `pong`) |
| GET | `/health` | Session | Detailed health check JSON |

---

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `SHOPIFY_API_KEY` | ✅ | App API key from Shopify Partners |
| `SHOPIFY_API_SECRET` | ✅ | App secret (used for HMAC validation) |
| `SHOPIFY_API_VERSION` | ✅ | e.g. `2025-10` |
| `HOST` | ✅ | Public URL of this app (e.g. `https://myapp.onrender.com`) |
| `DATABASE_URL` | ✅ | Supabase PostgreSQL pooler URL (port 6543) |
| `REDIS_URL` | ✅ | Redis connection string |
| `SECRET_KEY` | ✅ | Flask session secret |
| `ENCRYPTION_KEY` | ✅ | Fernet key for encrypting Odoo passwords at rest |
| `SMTP_PASSWORD` | ✅ | SMTP password for inventory alert emails |
| `SENTRY_DSN` | ⚠️ | Sentry DSN for error tracking (optional but recommended) |
| `ANTHROPIC_API_KEY` | ⚠️ | Claude API key — required for AI daily health report |
| `REPORT_EMAIL` | ⚠️ | Email address to receive the daily AI health report |

---

## Local Development

```bash
# 1. Clone and install
git clone https://github.com/your-org/odoo-connector.git
cd odoo-connector
pip install -r requirements.txt

# 2. Set environment variables
cp .env.example .env
# Edit .env with your values

# 3. Start Redis (Docker)
docker run -p 6379:6379 redis:alpine

# 4. Start RQ workers (two terminals)
rq worker critical --url redis://localhost:6379
rq worker default  --url redis://localhost:6379

# 5. Run Flask
flask run --port 5000

# 6. Tunnel for webhooks (ngrok)
ngrok http 5000
# Set HOST= to your ngrok URL
```

---

## Render Deployment Guide

1. **Web Service**: Connect GitHub repo, set build command `pip install -r requirements.txt`, start command `gunicorn app:app`
2. **Worker Service** (×2): Same repo, start command `rq worker critical` and `rq worker default`
3. **Environment Variables**: Add all variables from the table above
4. **Health Check Path**: Set to `/ping` (no auth required, fast response)
5. **Database**: Use Supabase PostgreSQL — set `DATABASE_URL` to the pooler URL (port **6543**, not 5432)
6. **Redis**: Add a Redis instance on Render or use Upstash; set `REDIS_URL`
7. **Sentry**: Add `SENTRY_DSN` env var — errors will appear in your Sentry project automatically

---

## Changelog

| # | Change | Description |
|---|--------|-------------|
| 1 | **Project inception** | Flask app scaffolded with Shopify OAuth, basic Odoo XML-RPC client, single-tenant proof of concept |
| 2 | **Multi-tenant architecture** | All DB tables scoped by `shop_url`; `Shop` model added to store per-store tokens and Odoo credentials |
| 3 | **ProductMap + SKU matching** | `ProductMap` table created; sync logic matches Shopify variants to Odoo products by SKU |
| 4 | **Webhook receiver** | `/webhooks/shopify` endpoint; HMAC validation; handles `orders/create`, `products/create`, `inventory_levels/update` |
| 5 | **Redis Queue (RQ) integration** | Moved sync work off the request thread into RQ workers; `critical` and `default` queues introduced |
| 6 | **APScheduler** | `_run_shop_schedule` added; Redis TTL keys gate per-entity intervals; replaces cron-based approach |
| 7 | **CustomerMap + order creation** | `CustomerMap` table; `process_new_order` creates Odoo sale order and maps customer on first order |
| 8 | **Fulfillment sync** | `sync_odoo_fulfillments` pushes Odoo `done` pickings as Shopify fulfillments with tracking |
| 9 | **Cancellation sync** | `sync_odoo_cancellations` checks Odoo for cancelled orders and cancels matching Shopify orders |
| 10 | **Return sync** | `refunds/create` webhook + scheduled `sync_odoo_returns`; creates Odoo reverse transfer |
| 11 | **Dashboard UI** | Flask-admin-style dashboard: sync logs table, queue status card, settings form, force-run button |
| 12 | **Odoo password encryption** | Fernet AES encryption at rest via `security_utils.py`; `odoo_password` property auto-encrypts/decrypts |
| 13 | **ProcessedOrder idempotency** | `ProcessedOrder` table prevents duplicate Odoo orders when Shopify sends duplicate webhooks |
| 14 | **Webhook auto-registration** | `automate_webhook_registration()` called on auth callback and settings save; no manual webhook setup needed |
| 15 | **FailedSyncOrder retry table** | Failed orders persisted to DB; retried every 15 min up to 50 attempts before email alert fires |
| 16 | **Image sync + hash deduplication** | Products sync images to Odoo; MD5 hash stored in `ProductMap.image_hash` to skip unchanged images |
| 17 | **Distributed Redis locks** | `acquire_distributed_lock()` prevents two workers running the same sync concurrently for the same shop |
| 18 | **AppSetting key-value store** | Per-shop config (scan mode, alert email, sync start date) stored in `app_settings` table |
| 19 | **SyncLog entity** | Structured logging with `entity`, `status`, `message` fields; displayed in dashboard log viewer |
| 20 | **Shopify Multipass / customer login** | `CustomerMap.password_hash` + `reset_token`; customers can log into Shopify storefront via app-managed passwords |
| 21 | **Supabase migration** | Moved from local SQLite/Postgres to Supabase hosted Postgres; `DATABASE_URL` env var introduced |
| 22 | **PgBouncer pooler URL** | `DATABASE_URL` switched to Supabase pooler port 6543 (transaction mode); SQLAlchemy pool_size=3 / max_overflow=5 |
| 23 | **SyncHealth model** | New `sync_health` table tracks `last_attempt_at`, `last_success_at`, `consecutive_failures`, `last_error` per entity per shop |
| 24 | **`_run_with_health` wrapper** | All sync functions wrapped; success/failure automatically recorded in `SyncHealth`; failures re-raised for RQ retry |
| 25 | **sync_health_monitor** | Background job runs every 2h; detects stale entities (not attempted in 2× interval) and re-enqueues them |
| 26 | **RQ Retry policies** | `critical` queue: 3 retries at 5m/15m/30m; `default`: 3× at 10m/30m/1h; orders: 5× at 1m/2m/4m/10m/20m |
| 27 | **Service functions raise on failure** | `fulfillments`, `cancellations`, `returns`, `customers`, `products` now raise `RuntimeError` instead of silently returning so SyncHealth records misses |
| 28 | **`/api/dashboard/status` endpoint** | Single JSON endpoint combining both queue counts, per-entity health rows, failed order count, and system status |
| 29 | **Dashboard Health Monitor card** | New UI card with live color-coded dots (green/yellow/red) per entity; JS polls every 30s |
| 30 | **Dashboard live queue counts** | Both `critical` and `default` queues counted and displayed; last-updated timestamp shown |
| 31 | **System status badges** | Shopify / Odoo / Workers badges update live from `/api/dashboard/status` instead of being static |
| 32 | **Run Scan button responsive fix** | Button layout changed from `col-md-*` to `col-sm-*` + `col-12` so it renders correctly on small screens |
| 33 | **Sentry error tracking** | `sentry-sdk[flask]` added; `FlaskIntegration` + `RqIntegration` initialized on startup if `SENTRY_DSN` set |
| 34 | **`/ping` health check endpoint** | Unauthenticated `/ping` → `pong` added; set as Render health check path to avoid false restarts |
| 35 | **Webhook rate limiting** | Redis counter `wh_rate_{shop}` limits webhooks to 100/60s per shop; returns 429 on breach |
| 36 | **FailedSyncOrder 50-attempt cap** | Orders exceeding 50 retry attempts trigger an SMTP email alert to the configured shop admin email |
| 37 | **Failed order pile-up warning** | `sync_health_monitor` logs a warning if ≥3 failed orders are queued for any shop |
| 38 | **Redis idle disconnect fix** | Sentry caught unhandled `ConnectionError` from Redis; fixed with `health_check_interval=30`, `socket_keepalive`, and `retry_on_error` — see Sentry Bug #1 below |
| 39 | **N+1 query fix in inventory sync** | Sentry caught 6 separate `app_settings` queries per sync run; replaced with single bulk query — see Sentry Bug #2 below |
| 40 | **Orders added to Health Monitor** | `background_order_sync` now writes to `SyncHealth` on success/failure; Orders row appears in dashboard health card with "webhook" label instead of a countdown (no fixed interval — fires on customer purchase) |
| 41 | **AI Daily Health Report** | New `services/ai_report.py`; global daily job collects 24h data from all shops (SyncHealth, SyncLogs, FailedSyncOrder, ProcessedOrder, queue counts), sends to Claude Haiku API, emails plain-English summary to `REPORT_EMAIL` every 24 hours |
| 42 | **Manual AI Report button in Tools** | `POST /maintenance/send_ai_report` added; "Send AI Health Report Now" button in Tools page enqueues the report immediately and resets the daily TTL so the auto-schedule is unaffected |
| 43 | **Fix RQ serialization error on AI report job** | Changed enqueue call from direct function reference to string path `'services.ai_report.generate_daily_ai_report'` so RQ can reliably deserialize the job on retry without `ValueError: Invalid attribute name` |
| 44 | **Zero failed jobs guarantee** | `_job_safe` decorator applied to all non-critical jobs (`sync_health_monitor`, `poll_missed_orders`, `check_and_repair_webhooks`, `retry_failed_orders`, `generate_daily_ai_report`) — catches all exceptions and logs to SyncLog without re-raising, so RQ never marks them failed |
| 45 | **`clean_failed_jobs` cleanup** | Global job runs every 30 min — fetches any jobs that do reach the failed registry (critical sync jobs after exhausting all retries), logs them to SyncLog, and removes them so the RQ failed count is always zero |
| 46 | **Fix AI report RQ path resolution (Sentry PYTHON-FLASK-3)** | `services/__init__.py` is empty so RQ's `import_attribute` could not resolve `services.ai_report` as a string path. Moved to `run_ai_report_job()` wrapper defined in `app.py` (always importable by workers) — enqueued by direct reference, no string path needed |
| 47 | **Fix `clean_failed_jobs` same RQ string path error** | `'app.clean_failed_jobs'` string also failed for same reason — switched to direct function reference |
| 48 | **N+1 fix in product sync** | Replaced 12 individual `get_config()` calls in `sync_products_master` with a single bulk `AppSetting` query — same pattern as inventory sync fix (entry #39) |
| 49 | **Centralise SMTP config** | `SMTP_SERVER`, `SMTP_PORT`, `SMTP_FROM`, `SMTP_PASSWORD` defined once as module-level constants in `utils.py`; `ai_report.py` and `app.py` contact form now import from there — no more 5-file edits to change email provider |
| 50 | **AI report errors now logged to DB** | Top-level exception in `generate_daily_ai_report` previously only `print()`ed; now also writes to `SyncLog` so failures are visible in the dashboard Live Logs tab |
| 51 | **Phase 1: shared-email customer safety** | `_get_safe_shopify_email()` added to `customers.py` — if two Odoo customers share an email, the second gets `{local}+{odoo_id}@{domain}` as their Shopify account email; `CustomerMap` always stores the real email for comms |
| 52 | **Multipass shared-email guard** | `request_password_setup` now detects multiple `CustomerMap` rows for the same email and returns a message directing the customer to use their Store ID instead |
| 53 | **CustomerMap-first order routing** | `process_order_data` now resolves the Odoo partner by `shopify_customer_id` via `CustomerMap` before falling back to email search — prevents duplicate Odoo partners when Shopify email is a `+` alias |
| 54 | **Fix silent customer save failure** | `c.save()` return value was never checked — Shopify could reject a customer (duplicate/invalid phone) and the connector logged "Synced" without creating anything. Now: checks return value, retries once without phone if rejected, raises and logs the real error if still failing |
| 55 | **Archive propagation: Odoo → Shopify** | Products archived in Odoo were silently ignored — they stayed live and purchasable in Shopify forever. Added `_archive_odoo_products_in_shopify()`: delta mode runs on every 24h sync (catches newly archived products), full mode runs on Force Full Resync (compares all ProductMap entries against active Odoo IDs — handles the one-time migration of bulk-archived products) |
| 56 | **Archive propagation: SKU reuse safety guard** | Archive propagation could incorrectly archive a Shopify product if a new active Odoo product was created with the same SKU as a just-archived one. Fix: before any archive action, check the Shopify product's primary variant SKU against the full set of currently active Odoo SKUs — if the SKU is still live, the product is skipped. If the active-SKU set cannot be fetched, the entire archive run aborts rather than risk false-archiving |
| 57 | **Purge Junk: archive instead of permanent delete** | `emergency_purge_junk_products` was calling `sp.destroy()` — permanent, unrecoverable deletion. Changed to `sp.status = 'archived'; sp.save()` — fully reversible from Shopify Admin. Also improved validity check to inspect ALL variants (not just `variants[0]`) so pack products with multiple variant SKUs are correctly protected |
| 58 | **Clearance Collection sync** | New `services/clearance.py` + `clearance_mirror` table. Second inventory pass sells Clearance + Damaged location stock as separate discounted `{sku}-CLR` mirror products (auto-created/drafted, priced at `list × (1 − clearance_discount_pct)`, added to a Clearance collection, with an earliest-lot `clearance.expiry_date` metafield). Main sync excludes those locations and skips `-CLR` SKUs (guardrail so it can't tombstone the mirrors). `services/orders.py` strips the `-CLR` suffix so clearance orders route to the base Odoo product at the discounted price (`manual_price=True`) without creating a junk product. Dashboard gains a Clearance Collection settings card. See Core Workflow #9 |
| 59 | **Clearance: normal-product lifecycle** | The clearance pass now drafts the **normal** product when its normal-location stock is 0 but clearance stock exists (so only the clearance mirror is buyable), and re-activates it when normal stock returns or clearance runs out. Tracked via `base_drafted` / `base_shopify_product_id` on `clearance_mirror` (added by an idempotent startup `ALTER TABLE ADD COLUMN IF NOT EXISTS` since `db.create_all` doesn't add columns) — only products the connector itself drafted are ever re-activated, never merchant-drafted ones |
| 60 | **Clearance: badge signals** | Mirror products now carry a `clearance.is_clearance` (boolean) metafield and keep the `Clearance` product tag on both create and update, alongside the existing `clearance.expiry_date` and Clearance-collection membership — so the storefront theme can render a "Clearance" badge + expiry. The badge markup itself is a theme (Liquid) change, not connector code |
| 61 | **Clearance: keep mirrors out of smart collections + fix draft targeting** | Testing showed mirrors leaking into vendor/type/tag smart collections and the normal-product draft hitting the wrong product. Fixes: (a) mirrors are created and kept with NO vendor, NO product_type, and ONLY the `Clearance` tag (re-asserted every sync) so smart collections stop matching them (merchant still adds a `tag ≠ Clearance` exclusion to any catch-all/inventory-based collections); (b) `_get_base_shopify_product` now resolves the base product via the reliable GraphQL exact-SKU lookup instead of `shopify.Variant.find(params={'sku'})`, which returned the wrong product; (c) product sync respects `ClearanceMirror.base_drafted` and won't re-activate a product the clearance pass has drafted |
| 62 | **Clearance: food-safety expiry cutoff** | Legal/compliance requirement — never sell stock that's expired or expiring too soon. Each lot's quantity is now checked individually: a lot with a best-before date where `(expiry − today) <= clearance_expiry_cutoff_days` (default 15) is excluded entirely from the mirror's sellable quantity and from the displayed expiry, no matter how much sits in Odoo. Lots with no date are unaffected. A mixed batch still sells its fine lots; only the near/past-expiry portion is withheld. If this drops sellable quantity to 0, the existing zero-out/reactivate lifecycle handles it automatically — mirror drafts, and the base product (if it had been hidden for having only clearance stock) returns to active showing its normal 0-stock state, with no separate code path required |
| 63 | **Fix: delivery-address child contact created with a blank street, and duplicated on near-identical addresses** | `find_or_create_child_address` (used by order sync to create/reuse the delivery-address child under a customer) read `address_data.get('street')`, but Shopify's address objects use `address1`/`address2`, not `street`/`street2` — the street line was always blank while city/zip/country_code came through fine (those happen to share Shopify's field names). Its dedup search also matched on an *exact string* comparison of that (always-blank) street, so a repeat customer's next order — even to a genuinely different address — would either wrongly reuse the first blank-street child ever created, or spawn a fresh duplicate the moment Shopify's free-text street line differed by so much as whitespace/capitalisation between orders (most visible on multi-location franchise customers, e.g. one company with several Caltex sites each ordering under the same Odoo parent). Fixed: (a) read `address1`/`address2` (`street`/`street2` fallback for any other caller); (b) match "the same real-world address" on **`zip` alone** instead of an exact street match — not even requiring city to also match, since real NZ addresses routinely use inconsistent locality names for the same postcode (e.g. "Kapiti" vs "Paraparaumu" for the same 5032 area), so zip+city was still letting duplicates through; falls back to a fuzzy (`ilike`) street match only when there's no zip to go on at all; (c) when a match is found, **update** it with the latest details instead of leaving it alone, which also self-heals old blank-street records the next time that customer orders (never overwrites a good existing value with a blank, only fills gaps). Trade-off: this prioritises "never duplicate" over perfectly distinguishing two genuinely different sites that happen to share a postcode (rare) — a shared-postcode collision just means that one Odoo child record tracks whichever order touched it most recently, which doesn't affect the actual Shopify order/fulfillment (that has its own independent address copy). Existing duplicate/blank contacts already in Odoo are not merged or deleted — this only prevents new ones and heals a matched record going forward |

---

## Sentry Bug Reports & Fixes

Sentry was added in entry #33. Within hours of going live it caught two real production bugs that would have been invisible without it.

---

### Bug #1 — `ConnectionError: Connection closed by server`
**Sentry ID:** PYTHON-FLASK-2  
**Severity:** Unhandled error (crashed the worker process)  
**Discovered:** 2026-04-19, ~32 minutes after first deploy with Sentry enabled

**What happened:**  
Our app keeps a persistent connection open to Redis (the fast memory store used for job queues and rate limiting). When the app was idle for a few minutes — no jobs running, no webhooks coming in — the Redis server on Render quietly closed the connection from its side to free up resources. Our app had no idea. The next time a sync job tried to use Redis, it reached for a connection that no longer existed and crashed with an unhandled `ConnectionError`. Without Sentry, this would have shown up as a silent worker failure with no explanation.

**Stack trace pointed to:** `redis._parsers.socket` → `_read_from_socket` — deep inside the Redis client library trying to read from a socket that was already closed (`fd=-1`).

**Root cause:** `redis.from_url()` was called with no keepalive settings. Default behaviour is to hold connections open silently with no heartbeat — so when the server closes them, the client doesn't find out until it tries to use the dead socket.

**Fix applied** (`utils.py`):
```python
conn = redis.from_url(
    redis_url,
    health_check_interval=30,   # ping Redis every 30s to keep connection alive
    retry_on_timeout=True,
    retry_on_error=[redis.exceptions.ConnectionError, redis.exceptions.TimeoutError],
    socket_keepalive=True,
)
```

**Prevention going forward:**  
- `health_check_interval=30` sends a silent ping every 30 seconds — the server never sees a long silence and never drops the connection
- `retry_on_error` means if a connection ever does die, the client reconnects automatically instead of crashing
- This class of error will not appear in Sentry again

---

### Bug #2 — N+1 Query: `SELECT app_settings...`
**Sentry ID:** PYTHON-FLASK-1  
**Severity:** Performance issue (not a crash, but wasteful and worsens with scale)  
**Discovered:** 2026-04-19, ~3 hours after first deploy with Sentry enabled  
**Triggered by:** `app.run_inventory_sync`

**What happened:**  
Every time inventory sync ran (every 30 minutes), the function needed 6 configuration values for the shop: location ID, target locations, inventory field, zero-stock flag, alert threshold, and alert email. Each value was fetched by calling `get_config()` individually. `get_config()` runs a `SELECT` query against the `app_settings` database table every time it is called. So for each sync run, the app made **6 round trips to the database** when 1 would do.

This is called an **N+1 query pattern** — instead of asking "give me everything I need in one go", the code asks N times. With one shop it's barely noticeable. With many shops running syncs constantly, it becomes significant load on the database.

**Root cause:** `get_config()` is a convenience helper designed for one-off lookups. Using it 6 times in a row in a hot path was the wrong tool.

**Fix applied** (`app.py`):
```python
# One query fetches all 6 keys at once
_rows = AppSetting.query.filter(
    AppSetting.shop_url == shop_url,
    AppSetting.key.in_({'shopify_target_location_id', 'inventory_locations',
                         'inventory_field', 'sync_zero_stock', 'alert_threshold', 'alert_email'})
).all()
_cfg = {r.key: (json.loads(r.value) if ... else r.value) for r in _rows}

# All 6 values now read from memory — zero extra DB queries
target_locations = _cfg.get('inventory_locations') or []
target_field     = _cfg.get('inventory_field') or 'qty_available'
# ... etc
```

**Prevention going forward:**  
- Any sync function that needs multiple config values should bulk-load them in one query at the top, not call `get_config()` in sequence
- `get_config()` is still fine for one-off single-key lookups (e.g. in webhook handlers)
- Sentry's N+1 detector will flag any future recurrence of this pattern automatically
