# Do NOT run `shopify app deploy` on this repo without reading this first

Checked 2026-09-15. `shopify.app.toml` in this repo has **never been deployed**. The live
release is `live-connector-for-odoo-12` from **2026-01-13**; the toml was rewritten on
**2026-01-21** (commit `1b8da66`) and has sat undeployed ever since.

Deploying it as-is would make three breaking changes at once:

1. **Removes the GDPR compliance URLs.** The live config carries
   `[webhooks.privacy_compliance]` with `customer_data_request_url`,
   `customer_redaction_url` and `shop_redaction_url`. **This file has none.** Shopify
   requires mandatory compliance webhooks for public apps; deploying would drop them.

2. **Declares `[access_scopes]` where the live app declares none** (15 scopes:
   read/write customers, fulfillments, inventory, locations, orders, products,
   merchant- and third-party-managed fulfillment orders).

3. **Switches the install flow**: `use_legacy_install_flow = false`, i.e. legacy OAuth ->
   Shopify managed install. Combined with (2), Shopify grants only the scopes the app
   **declares**, not what an install URL asks for. Every installed merchant would be
   forced through re-auth, and anything relying on a scope not in that list breaks
   silently.

The backend at https://odoo-connector-oivx.onrender.com was responding (HTTP 200) when this
was written, so this is a live app, not an abandoned one.

**Before any deploy here:** decide deliberately whether the managed-install migration is
intended, re-add the privacy compliance URLs, and confirm the scope list against what the
code actually calls. `railway up` is irrelevant for this app; it is hosted on Render.
