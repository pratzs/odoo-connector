# Read before `shopify app deploy` in this repo

Verified 2026-09-15 against the **live** config (`shopify app config pull`), not against git
history.

## Correction to the first version of this file

An earlier version of this document claimed the live app still had `[webhooks.privacy_compliance]`
and no `[access_scopes]`, and that deploying would strip them. **That was wrong.** It was written
by comparing the repo file against an old commit (`c31cbc5`, 2026-01-14) and assuming that commit
reflected production. It did not. The live app had already been moved to managed install with a
15-scope block, and the repo file matched it almost exactly. The only real difference was TOML key
ordering.

## The actual problem, which is live right now

This app runs on **managed install** (`use_legacy_install_flow = false`). Under managed install,
**Shopify grants exactly the scopes declared in `shopify.app.toml` and ignores the `scope=`
parameter the app builds in its own OAuth URL** (`app.py`, `SCOPES` constant).

- `SCOPES` in `app.py` asks for **38**.
- `shopify.app.toml` declared **15**.
- So **23 were never granted**, silently, with no install-time warning.

Checked against what the code actually calls, one of those gaps is real:

| Missing scope | Called by | Consequence |
|---|---|---|
| `write_companies`, `read_companies` | `services/customers.py`: `companyCreate`, `companyLocationCreate`, `companyContactCreate`, `companyContactRoleAssign` | **B2B company + location + contact creation cannot work.** This is the "assigned to B2B Company + Catalog" step in the customer sync described in `SCOPE.md`. |

The other 21 have **no call sites anywhere in the codebase** and were simply over-requested:
`product_listings`, `files`, `reports`, `shipping`, `price_rules`, `discounts`, `draft_orders`,
`assigned_fulfillment_orders`, `write_locations`, `write_payments`, `returns`. `read_all_orders`
is not needed either: the order poll looks back **48 hours**, well inside the 60-day window that
scope exists to widen.

## What this repo's toml now contains

`read_companies` and `write_companies` added (**15 -> 17**), and the three **mandatory compliance
webhooks declared**. Their handlers have existed in `app.py` all along
(`/gdpr/customers/data_request`, `/gdpr/customers/redact`, `/gdpr/shop/redact`) but were never
declared, so Shopify was never told where to send them.

## Deploying this is NOT routine

`shopify app deploy` here changes `access_scopes` on a **live client system** (Worthy, Shopify
Plus). Adding a scope under managed install means **every installed merchant must re-approve the
app**. Until they do, the app holds the old grant.

Before deploying:

1. Agree a window with the merchant; they will see a permission prompt.
2. Deploy, then confirm the merchant completes re-authorisation.
3. Verify B2B company creation actually works afterwards, rather than assuming.

`railway up` is irrelevant here. This app is hosted on **Render**, not Railway.
