# Project Scope & Architecture

This document covers what has been built, what Multipass does, how the full B2B login system will work, and every known edge case.

---

## Table of Contents

1. [What This App Does](#what-this-app-does)
2. [What Has Been Built](#what-has-been-built)
3. [Multipass — Current State](#multipass--current-state)
4. [Multipass — How It Will Work End-to-End](#multipass--how-it-will-work-end-to-end)
5. [Shared Email Problem — Phase 1](#shared-email-problem--phase-1)
6. [Phase 2 — Full Synthetic Emails](#phase-2--full-synthetic-emails)
7. [Edge Cases & How They Are Handled](#edge-cases--how-they-are-handled)
8. [Known Gaps & Outstanding Work](#known-gaps--outstanding-work)

---

## What This App Does

This is a production connector between **Shopify Plus** (storefront) and **Odoo ERP** (back-office). It keeps both systems in sync so that:

- Stock levels in Odoo are reflected in Shopify automatically
- Orders placed on Shopify flow into Odoo as sale orders
- Customer records created in Odoo appear in Shopify automatically
- Fulfillments, cancellations, and returns are mirrored between both systems
- B2B customers can log in to Shopify using their Odoo ID and a password they set — not Shopify's native email/OTP flow

The app runs on Render (Flask + RQ workers), uses Supabase (PostgreSQL) for state, and Redis for job queues and distributed locks. It supports multiple Shopify stores from a single deployment.

---

## What Has Been Built

### Core Sync Engine
| Feature | Status |
|---|---|
| Shopify OAuth install flow | Done |
| Webhook registration (auto on install + hourly repair) | Done |
| Inventory sync: Odoo → Shopify, every 30 min | Done |
| Order sync: Shopify → Odoo, webhook + 30 min poll fallback | Done |
| Fulfillment sync: Odoo → Shopify, every 60 min | Done |
| Cancellation sync: Odoo → Shopify, every 5 min | Done |
| Return/refund sync: Shopify → Odoo, webhook + scheduled | Done |
| Customer sync: Odoo → Shopify, every 12 hours | Done |
| Product sync: Shopify ↔ Odoo, webhook + 24 hour scheduled | Done |
| Multi-tenant (one app, many stores) | Done |

### Reliability
| Feature | Status |
|---|---|
| RQ retry policies (3–5× with exponential backoff) | Done |
| SyncHealth table tracking per-entity attempt/success/failures | Done |
| sync_health_monitor re-enqueues stale entities automatically | Done |
| FailedSyncOrder retry table (up to 50 attempts, then email alert) | Done |
| Redis distributed locks preventing duplicate concurrent syncs | Done |
| Webhook HMAC validation | Done |
| Webhook rate limiting (100/60s per shop) | Done |
| Redis keepalive + auto-reconnect | Done |
| `_job_safe` decorator: non-critical jobs never enter RQ failed queue | Done |
| `clean_failed_jobs` clears failed registry every 30 min | Done |
| Sentry error tracking (Flask + RQ integrations) | Done |

### Dashboard & Observability
| Feature | Status |
|---|---|
| Admin dashboard with live sync logs | Done |
| Health Monitor card: per-entity status dots (green/yellow/red) | Done |
| Live queue counts (critical + default) | Done |
| System status badges (Shopify / Odoo / Workers) | Done |
| `/ping` health check endpoint for Render | Done |
| AI Daily Health Report via Claude Haiku → email | Done |
| Manual "Send AI Report Now" button in Tools | Done |

### Customer Identity (B2B)
| Feature | Status |
|---|---|
| CustomerMap table bridging Shopify ↔ Odoo customer IDs | Done |
| Multipass login: Odoo ID + password → Shopify session | Done |
| Password setup/reset via one-time email link | Done |
| Shared-email + alias assignment (Phase 1) | Done |
| CustomerMap-first order routing (no email dependency) | Done |
| Shared-email guard on password reset | Done |
| Odoo password AES encryption at rest | Done |

---

## Multipass — Current State

### What It Is
Shopify Multipass is a Shopify Plus feature that lets an external system authenticate a customer and drop them directly into a Shopify session — bypassing Shopify's native email/OTP login entirely. The customer never creates a Shopify password. Instead, they use credentials managed by this connector.

### What Is Built
**Login flow (`/multipass/login`):**
1. Customer visits the B2B login page on the Shopify storefront
2. Enters their **Store ID** (= Odoo partner ID, e.g. `31274`) and their **password**
3. POST to `/multipass/login` on this connector
4. App looks up `CustomerMap` by `odoo_partner_id` to find the `shopify_customer_id` and `email`
5. Validates the password against `CustomerMap.password_hash` (bcrypt)
6. Generates a Shopify Multipass token using `SHOPIFY_MULTIPASS_SECRET`
7. Redirects the customer to `https://mystore.myshopify.com/account/login/multipass/{token}`
8. Customer lands in their Shopify account — authenticated, B2B catalog loaded

**Password setup (`/multipass/setup` or admin-triggered):**
1. Admin triggers "Send Setup Email" from the dashboard, OR
2. Customer clicks "Set up my password" on the login page
3. App finds the `CustomerMap` record and generates a one-time token (UUID, 7-day expiry)
4. Sends an HTML email to the customer's real email (from `CustomerMap.email`, not Shopify's field)
5. Email contains their Store ID and a link to `https://mystore.myshopify.com/pages/set-password?token=xxx`
6. Customer clicks link → POST new password → bcrypt hash stored in `CustomerMap.password_hash`
7. Customer can now log in

**Password reset (same flow as setup):**
- Same endpoint, same email, replaces the existing hash

### What the Shopify Email Field Contains Right Now
Currently, the Shopify customer email field holds the real email from Odoo (e.g. `pratham@worthy.nz`). This works fine when each customer has a unique email. The Phase 1 shared-email fix adds `+alias` handling for the minority of corporate customers who share emails.

---

## Multipass — How It Will Work End-to-End

Once Multipass is live and the Shopify storefront has a B2B login page, this is the complete flow for every customer interaction:

### New Customer Added in Odoo

```
Admin creates partner in Odoo (ID: 35820, email: cxallseasons@petrochemgroup)
        ↓
sync_customers_master picks them up within 12 hours
(or admin hits "Force Run" on dashboard for immediate sync)
        ↓
_get_safe_shopify_email() checks CustomerMap:
  - If email is unique → use as-is in Shopify
  - If another customer already has it → use cxallseasons+35820@petrochemgroup in Shopify
CustomerMap.email always = cxallseasons@petrochemgroup (real, for comms)
        ↓
Shopify customer created, assigned to B2B Company + Catalog
        ↓
(Future Phase 2) Auto setup email sent to real email:
  Subject: "Set Up Your Password — Your Store ID is 35820"
  Body: one-time link + Store ID prominently displayed
        ↓
Customer sets password → can log in
```

### Customer Logs In

```
Customer visits mystore.myshopify.com/pages/b2b-login
Enters: Store ID = 35820, Password = ••••••••
        ↓
POST /multipass/login
        ↓
CustomerMap.query.filter_by(odoo_partner_id=35820, shop_url=...).first()
        ↓
check_password_hash(customer.password_hash, entered_password)
        ↓
generate_multipass_token(customer.email, shop_url)
Shopify email field used here — doesn't matter if it's real or + alias
        ↓
Redirect → Shopify session active, B2B catalog loaded
Customer never sees or types an email during login
```

### Customer Places an Order

```
Customer places order in Shopify
        ↓
orders/create webhook fires
        ↓
background_order_sync() runs
        ↓
CustomerMap.query.filter_by(shopify_customer_id=..., shop_url=...).first()
Found → Odoo partner ID = 35820 (direct, no email needed)
        ↓
Order created in Odoo against correct partner — no duplicates
        ↓
(Future) contact_email on Shopify order patched to real email
so Shopify sends the order confirmation to cxallseasons@petrochemgroup
```

### Customer Resets Password

```
Customer visits login page, clicks "Forgot password"
Enters their Store ID (35820) OR email (cxallseasons@petrochemgroup)
        ↓
POST /multipass/setup with identifier = "35820" (Store ID path)
        ↓
CustomerMap.query.filter_by(odoo_partner_id=35820, ...).first()
New token generated, email sent to CustomerMap.email (real address)
        ↓
Customer clicks link → sets new password → done
```

If they enter email instead of Store ID and multiple accounts share it:
```
POST /multipass/setup with identifier = "cxallseasons@petrochemgroup"
        ↓
CustomerMap.query.filter_by(email=...).all() → 2 records found
        ↓
Response: "Multiple accounts share this email. Please enter your Store ID instead."
Customer uses Store ID → correct path above
```

---

## Shared Email Problem — Phase 1

### The Problem
A single person or accounting team manages multiple businesses. They use one email (`accounts@conglomerate.com`) for every Odoo partner. Shopify requires a unique email per customer account. Before Phase 1, syncing the second customer with the same email would either fail or overwrite the first customer's Shopify account.

### Phase 1 Solution (Built)

**`_get_safe_shopify_email()` in `customers.py`:**

```
Customer A: Pratham Jani (Odoo ID 31274, email accounts@conglomerate.com)
→ No conflict → Shopify email = accounts@conglomerate.com
→ CustomerMap.email = accounts@conglomerate.com

Customer B: Caltex All Seasons (Odoo ID 35820, email accounts@conglomerate.com)
→ Conflict detected (31274 already has this email on this shop)
→ Shopify email = accounts+35820@conglomerate.com (unique key)
→ CustomerMap.email = accounts@conglomerate.com (real, for all comms)
```

**What stays the same for customers:**
- Both receive emails at `accounts@conglomerate.com` — the + alias delivers to the same inbox
- Setup email for Caltex contains Store ID `35820` — the one-time link is tied to Odoo ID `35820` specifically
- When Caltex logs in, they use Store ID `35820` — email is never typed or shown
- When either customer places an order, `CustomerMap` lookup by `shopify_customer_id` finds the right Odoo partner directly — no email involved

**What Shopify sees:**
Two distinct account emails. No conflict. No overwrites.

### What Phase 1 Does NOT Cover
- Existing customers already in Shopify with duplicate emails — Phase 1 only applies to new syncs. Existing duplicates need a one-time migration script (Phase 2 item)
- Shopify's own transactional emails (order confirmation, shipping) still go to the Shopify email field, which may be the + alias — covered in Phase 2 with order `contact_email` patching

---

## Phase 2 — Full Synthetic Emails

Phase 2 is optional but the cleanest long-term state. Instead of + aliases only for conflicts, every B2B customer gets a synthetic Shopify email (`{odoo_id}@worthyproducts.nz`) regardless of whether their real email is shared. This completely eliminates the shared-email problem at the root.

### What Changes in Phase 2

| Component | Phase 1 | Phase 2 |
|---|---|---|
| Shopify email | Real or + alias | Always `{id}@worthyproducts.nz` |
| CustomerMap.email | Always real | Always real |
| Login | Store ID + password | Store ID + password (unchanged) |
| Order routing | CustomerMap by Shopify ID | CustomerMap by Shopify ID (unchanged) |
| Order confirmation email | Goes to Shopify field (may be alias) | Patched to real email via Shopify Order API |
| Migration | None needed for new customers | One-time script updates all existing customers |

### Phase 2 Prerequisites
1. Multipass fully live and tested on the storefront
2. `@worthyproducts.nz` email domain set up (catches or discards inbound — customers never need to check it)
3. Shopify native account emails (welcome, password reset) disabled in Shopify Admin → Settings → Notifications
4. Order `contact_email` patch implemented in `background_order_sync` so confirmations reach the real inbox

---

## Edge Cases & How They Are Handled

| Scenario | How It Is Handled |
|---|---|
| Duplicate webhook — same order fired twice | `ProcessedOrder` table (composite PK: shopify_id + shop_url) — second webhook is a no-op |
| Order sync fails (Odoo down) | Saved to `FailedSyncOrder`, retried every 15 min up to 50 attempts. After 50, email alert sent |
| Worker crashes mid-sync | Redis TTL key already set — scheduler re-enqueues on next cycle. SyncHealth detects stale entity within 2× interval and re-enqueues |
| Redis connection drops | `health_check_interval=30` + `retry_on_error` — reconnects automatically, no crash |
| Two workers picking up same sync | Redis distributed lock (`acquire_distributed_lock`) — second worker skips |
| Shopify webhook flood (attack or runaway) | Rate limit: 100 webhooks/60s per shop — 429 returned above threshold |
| Same customer email, two Odoo partners | `_get_safe_shopify_email()` assigns + alias to second customer — both accounts unique in Shopify |
| Password reset for shared-email customer | Multiple `CustomerMap` matches detected → customer directed to use Store ID instead |
| Order from customer not yet in CustomerMap | Falls back to `odoo.search_partner_by_email(email)` — B2C and unmapped customers work as before |
| New Shopify order from B2B customer with + alias Shopify email | `CustomerMap` lookup by `shopify_customer_id` → Odoo ID found directly — email never used |
| Odoo partner has no email | Falls back to `no-email-{id}@pos.local` in Shopify — customer flagged, no Multipass comms |
| Sentry catches a new unhandled error | Error grouped in Sentry, AI daily report mentions it, SyncLog entry created |
| RQ job exhausts all retries | `clean_failed_jobs` runs every 30 min — logs final failure to SyncLog, removes from failed registry |
| Non-critical job throws exception | `_job_safe` decorator catches it, logs to SyncLog, job exits cleanly — never lands in failed queue |
| Inventory discrepancy > alert threshold | SMTP alert email sent to configured `alert_email` per shop |
| Shop uninstalls app | `app/uninstalled` webhook fires → shop marked inactive → scheduler skips it |
| Multipass token expired (7-day link) | `reset_token_expires` check returns "Link expired. Please request a new one." |
| Customer tries Shopify native password reset | Goes to the synthetic/+ alias email — a mailbox the customer doesn't check. To be mitigated in Phase 2 by disabling Shopify native account emails |

---

## Known Gaps & Outstanding Work

### Must Do Before Multipass Goes Live

| Item | Why |
|---|---|
| Build B2B login page on Shopify storefront (`/pages/b2b-login`) | Customers need somewhere to enter Store ID + password |
| Build password set page on Shopify storefront (`/pages/set-password?token=xxx`) | The one-time link from setup emails lands here |
| Set `SHOPIFY_MULTIPASS_SECRET` env var on Render | Multipass token generation will fail without it |
| Disable Shopify native account emails | Prevent Shopify sending password resets to synthetic/alias addresses |
| Test end-to-end: new customer → setup email → set password → login | Validate the full Multipass flow before merchant launch |

### Phase 2 (When Ready)

| Item | Description |
|---|---|
| Full synthetic email migration script | One-time job: update all existing Shopify customers to `{odoo_id}@worthyproducts.nz`, update CustomerMap |
| Order `contact_email` patch | After `orders/create`, patch Shopify order `contact_email` to real email from CustomerMap so order confirmations reach the right inbox |
| Auto setup email on new customer sync | After `_sync_single_customer` creates a new record with no `password_hash`, automatically trigger the setup email — no admin manual step needed |
| `@worthyproducts.nz` domain setup | Configure the domain to catch or discard inbound — customers should never need to check it |

### Nice to Have

| Item | Description |
|---|---|
| Sentry → GitHub PR auto-fix | When Sentry catches a new error, call Claude API to propose a fix and open a PR automatically |
| Dashboard order for health monitor | Allow reordering entities in the health monitor card |
| Per-shop Sentry environment tags | Tag Sentry events with `shop_url` so issues are filterable by store |
| Bulk password reset | Dashboard button to re-send setup emails to all customers with no `password_hash` |
