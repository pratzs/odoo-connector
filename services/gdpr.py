"""
Shopify mandatory compliance webhooks (GDPR / CCPA) and the uninstall hook.

Scope: ONLY this app's own database. Nothing here talks to the merchant's Odoo
instance or to Shopify. Customer records the connector created in Odoo
(res.partner) belong to the merchant's ERP and are the merchant's to handle.

Logging rule for this module: never print or log an email, name, address or a
payload. Shop domain, topic, numeric ids and row counts only.

Every function expects to run inside a Flask app context.
"""
import json
import re
import smtplib
from datetime import datetime
from email.message import EmailMessage

from sqlalchemy import func, or_

from models import (
    db, AppSetting, ClearanceMirror, CustomerMap, FailedSyncOrder,
    ProcessedOrder, ProductMap, Shop, SyncHealth, SyncLog,
)

REDACTED = "[redacted]"
EXPORT_KEY_PREFIX = "gdpr_export_"   # AppSetting.key is String(50)


# ---------------------------------------------------------------------------
# Payload helpers
# ---------------------------------------------------------------------------
def _id_forms(raw_id, gid_type):
    """Numeric string plus gid form. Empty list when the id is missing, so an
    empty value can never end up inside an OR filter."""
    if raw_id is None:
        return []
    s = str(raw_id).strip()
    if not s or s.lower() == "none":
        return []
    if s.startswith("gid://"):
        num = s.rsplit("/", 1)[-1]
        return [num, s] if num else [s]
    return [s, f"gid://shopify/{gid_type}/{s}"]


def parse_customer_payload(payload):
    """Returns (customer_ids, email_lower_or_None, order_ids) from a
    customers/redact or customers/data_request payload."""
    payload = payload or {}
    customer = payload.get("customer") or {}
    customer_ids = _id_forms(customer.get("id"), "Customer")
    email = (customer.get("email") or "").strip().lower() or None
    raw_orders = (payload.get("orders_to_redact")
                  or payload.get("orders_requested") or [])
    order_ids = []
    for oid in raw_orders:
        for form in _id_forms(oid, "Order"):
            if form not in order_ids:
                order_ids.append(form)
    return customer_ids, email, order_ids


def _like_escape(value):
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _contains_ci(column, needle):
    return func.lower(column).like(f"%{_like_escape(needle)}%", escape="\\")


def _scrub(text, email):
    if not text or not email:
        return text
    return re.sub(re.escape(email), REDACTED, text, flags=re.IGNORECASE)


def _order_belongs_to(order, customer_ids, email, order_ids, shopify_id):
    """Confirms in Python that a stored order really is this customer's, so a
    LIKE match on a digit run inside some other number cannot delete it."""
    if shopify_id and str(shopify_id) in order_ids:
        return True
    if not isinstance(order, dict):
        return False
    if str(order.get("id", "")) in order_ids:
        return True
    cust = order.get("customer") or {}
    if customer_ids and str(cust.get("id", "")) in customer_ids:
        return True
    if email:
        for candidate in (order.get("email"), order.get("contact_email"),
                          cust.get("email")):
            if candidate and str(candidate).strip().lower() == email:
                return True
    return False


def _matching_failed_orders(shop_url, customer_ids, email, order_ids):
    clauses = []
    if order_ids:
        clauses.append(FailedSyncOrder.shopify_id.in_(order_ids))
    for cid in customer_ids:
        if not cid.startswith("gid://"):
            clauses.append(FailedSyncOrder.order_data.like(
                f"%{_like_escape(cid)}%", escape="\\"))
    if email:
        clauses.append(_contains_ci(FailedSyncOrder.order_data, email))
    if not clauses:
        return []
    candidates = FailedSyncOrder.query.filter(
        FailedSyncOrder.shop_url == shop_url, or_(*clauses)).all()
    rows = []
    for row in candidates:
        try:
            order = json.loads(row.order_data)
        except Exception:
            order = None
        if _order_belongs_to(order, customer_ids, email, order_ids, row.shopify_id):
            rows.append((row, order))
    return rows


def _matching_customer_maps(shop_url, customer_ids, email):
    clauses = []
    if customer_ids:
        clauses.append(CustomerMap.shopify_customer_id.in_(customer_ids))
    if email:
        clauses.append(func.lower(CustomerMap.email) == email)
    if not clauses:
        return []
    return CustomerMap.query.filter(
        CustomerMap.shop_url == shop_url, or_(*clauses)).all()


def _matching_logs(shop_url, email):
    if not email:
        return []
    return SyncLog.query.filter(
        SyncLog.shop_url == shop_url, _contains_ci(SyncLog.message, email)).all()


def _export_keys(customer_ids, data_request_id=None):
    keys = [f"{EXPORT_KEY_PREFIX}{c}"[:50] for c in customer_ids
            if not c.startswith("gid://")]
    if data_request_id:
        keys.append(f"{EXPORT_KEY_PREFIX}req_{data_request_id}"[:50])
    return keys


# ---------------------------------------------------------------------------
# customers/data_request
# ---------------------------------------------------------------------------
def collect_customer_data(shop_url, payload):
    """Everything this app stores about one customer, for one shop."""
    customer_ids, email, order_ids = parse_customer_payload(payload)

    maps = _matching_customer_maps(shop_url, customer_ids, email)
    failed = _matching_failed_orders(shop_url, customer_ids, email, order_ids)
    logs = _matching_logs(shop_url, email)
    processed = []
    if order_ids:
        processed = ProcessedOrder.query.filter(
            ProcessedOrder.shop_url == shop_url,
            ProcessedOrder.shopify_id.in_(order_ids)).all()

    def iso(dt):
        return dt.isoformat() if dt else None

    return {
        "shop": shop_url,
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "customer_id": customer_ids[0] if customer_ids else None,
        "note": ("Data held by the Odoo connector app's own database. Records "
                 "the connector created in your Odoo instance are in your Odoo "
                 "database, not here."),
        "customer_mappings": [{
            "shopify_customer_id": m.shopify_customer_id,
            "odoo_partner_id": m.odoo_partner_id,
            "email": m.email,
            # The password hash and reset token are credentials, not personal
            # data the merchant can use; report only whether they exist.
            "storefront_password_set": bool(m.password_hash),
            "password_reset_pending": bool(m.reset_token),
        } for m in maps],
        "orders_awaiting_retry": [{
            "shopify_order_id": row.shopify_id,
            "last_attempt_at": iso(row.last_attempt_at),
            "attempt_count": row.attempt_count,
            "last_error": row.last_error,
            "order": order if order is not None else row.order_data,
        } for row, order in failed],
        "orders_synced": [{
            "shopify_order_id": p.shopify_id,
            "synced_at": iso(p.created_at),
        } for p in processed],
        "activity_log_entries": [{
            "timestamp": iso(l.timestamp),
            "entity": l.entity,
            "status": l.status,
            "message": l.message,
        } for l in logs],
    }


def _send_export_email(shop_url, recipient, export, smtp):
    """smtp = dict(server, port, sender, password). Returns True on send."""
    if not (recipient and smtp and smtp.get("password")):
        return False
    msg = EmailMessage()
    msg["Subject"] = f"[{shop_url}] Customer data request (GDPR)"
    msg["From"] = smtp["sender"]
    msg["To"] = recipient
    msg.set_content(
        "Shopify sent a customer data request for your store.\n\n"
        "Attached is everything the Odoo connector app holds about that "
        "customer. Please pass it on to the customer as required. Records in "
        "your Odoo database are not included and should be exported from Odoo.\n"
    )
    msg.add_attachment(
        json.dumps(export, indent=2, default=str).encode("utf-8"),
        maintype="application", subtype="json",
        filename=f"customer-data-{export.get('customer_id') or 'request'}.json",
    )
    with smtplib.SMTP_SSL(smtp["server"], smtp["port"], timeout=30) as conn:
        conn.login(smtp["sender"], smtp["password"])
        conn.send_message(msg)
    return True


def handle_data_request(shop_url, payload, recipient=None, smtp=None):
    """
    Builds the export and delivers it to the merchant. Emailed to the shop's
    configured alert address when SMTP is available; otherwise (or if the send
    fails) the export is stored in AppSetting under gdpr_export_<customer id>
    so it can be retrieved and handed to the merchant. A PII-free line goes to
    the shop's activity log either way.
    """
    export = collect_customer_data(shop_url, payload)
    counts = {
        "mappings": len(export["customer_mappings"]),
        "orders_awaiting_retry": len(export["orders_awaiting_retry"]),
        "orders_synced": len(export["orders_synced"]),
        "log_entries": len(export["activity_log_entries"]),
    }

    delivered = "email"
    try:
        sent = _send_export_email(shop_url, recipient, export, smtp)
    except Exception as e:
        print(f"GDPR data_request email failed for {shop_url}: {type(e).__name__}")
        sent = False

    if not sent:
        delivered = "stored"
        customer_ids, _, _ = parse_customer_payload(payload)
        req_id = (payload or {}).get("data_request", {}).get("id") if isinstance(
            (payload or {}).get("data_request"), dict) else None
        keys = _export_keys(customer_ids, None if customer_ids else req_id)
        key = keys[0] if keys else f"{EXPORT_KEY_PREFIX}unknown"
        row = AppSetting.query.filter_by(shop_url=shop_url, key=key).first()
        if not row:
            row = AppSetting(shop_url=shop_url, key=key)
            db.session.add(row)
        row.value = json.dumps(export, default=str)

    cid = export["customer_id"] or "unknown"
    db.session.add(SyncLog(
        shop_url=shop_url, entity="GDPR", status="Info", timestamp=datetime.utcnow(),
        message=(f"Customer data request for customer {cid}: "
                 f"{counts['mappings']} mapping(s), {counts['orders_awaiting_retry']} "
                 f"order(s) awaiting retry, {counts['orders_synced']} synced order id(s), "
                 f"{counts['log_entries']} log entr(ies). Export "
                 + ("emailed to the store's alert address." if delivered == "email"
                    else "stored for retrieval (no alert email / SMTP available)."))))
    db.session.commit()
    return {"delivered": delivered, **counts}


# ---------------------------------------------------------------------------
# customers/redact
# ---------------------------------------------------------------------------
def redact_customer(shop_url, payload):
    """
    Erases one customer's personal data for one shop.

    - CustomerMap rows: DELETED. They are purely personal (email, storefront
      password hash, reset token) and hold nothing the merchant needs; the
      Odoo partner itself lives in the merchant's Odoo.
    - FailedSyncOrder rows for this customer / orders_to_redact: DELETED. They
      are full Shopify order payloads (name, email, addresses, phone). The
      order is no longer retried by the connector; the merchant still has it in
      Shopify.
    - SyncLog / SyncHealth / remaining FailedSyncOrder error text: the email
      is overwritten with "[redacted]" and the rest of the row kept, because
      the activity log is the merchant's operational record.
    - Stored data-request exports for this customer: DELETED.
    """
    customer_ids, email, order_ids = parse_customer_payload(payload)
    counts = {"customer_maps": 0, "failed_orders": 0, "logs_scrubbed": 0,
              "health_scrubbed": 0, "exports": 0}
    if not shop_url or not (customer_ids or email or order_ids):
        return counts

    for row in _matching_customer_maps(shop_url, customer_ids, email):
        db.session.delete(row)
        counts["customer_maps"] += 1

    for row, _ in _matching_failed_orders(shop_url, customer_ids, email, order_ids):
        db.session.delete(row)
        counts["failed_orders"] += 1

    if email:
        for row in _matching_logs(shop_url, email):
            row.message = _scrub(row.message, email)
            counts["logs_scrubbed"] += 1
        for row in SyncHealth.query.filter(
                SyncHealth.shop_url == shop_url,
                _contains_ci(SyncHealth.last_error, email)).all():
            row.last_error = _scrub(row.last_error, email)
            counts["health_scrubbed"] += 1
        for row in FailedSyncOrder.query.filter(
                FailedSyncOrder.shop_url == shop_url,
                _contains_ci(FailedSyncOrder.last_error, email)).all():
            row.last_error = _scrub(row.last_error, email)

    keys = _export_keys(customer_ids)
    if keys:
        counts["exports"] = AppSetting.query.filter(
            AppSetting.shop_url == shop_url, AppSetting.key.in_(keys)
        ).delete(synchronize_session=False)

    db.session.commit()
    return counts


# ---------------------------------------------------------------------------
# shop/redact
# ---------------------------------------------------------------------------
# Every table that is keyed to a shop. There are no foreign keys between them,
# so order only matters for readability; Shop goes last.
SHOP_SCOPED_MODELS = (
    CustomerMap, FailedSyncOrder, ProcessedOrder, SyncHealth, SyncLog,
    ProductMap, ClearanceMirror, AppSetting, Shop,
)


def redact_shop(shop_url):
    """
    Erases ALL of this shop's data from this app's database in one
    transaction: mappings, order payloads, logs, settings (including the
    stored SMTP password and storefront token) and the Shop row itself
    (Shopify access token, encrypted Odoo credentials). Does not touch Odoo.

    No is_active guard: before this fix the uninstall handler never managed to
    commit, so uninstalled shops can still read is_active=True. Shopify only
    sends shop/redact when the app has stayed uninstalled for 48 hours.
    """
    counts = {}
    if not shop_url:
        return counts
    try:
        for model in SHOP_SCOPED_MODELS:
            col = model.shop_url
            counts[model.__tablename__] = model.query.filter(
                col == shop_url).delete(synchronize_session=False)
        db.session.commit()
    except Exception:
        db.session.rollback()
        raise
    return counts


# ---------------------------------------------------------------------------
# app/uninstalled
# ---------------------------------------------------------------------------
def mark_uninstalled(shop_url):
    """
    Deactivates the shop and drops its Shopify credentials. access_token is
    NOT NULL in the schema, so it is set to "" (every reader checks
    truthiness: utils.setup_shopify_session, services/multipass.py). Odoo
    credentials and settings are kept so a reinstall within 48 hours works;
    shop/redact removes everything after that.
    """
    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop:
        return False
    shop.is_active = False
    shop.access_token = ""
    AppSetting.query.filter_by(
        shop_url=shop_url, key="storefront_access_token"
    ).delete(synchronize_session=False)
    db.session.add(SyncLog(
        shop_url=shop_url, entity="System", status="Info",
        timestamp=datetime.utcnow(),
        message="App uninstalled. Syncs stopped and the Shopify token removed."))
    db.session.commit()
    return True
