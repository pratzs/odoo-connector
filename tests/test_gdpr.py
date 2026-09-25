"""
Compliance webhook logic (services/gdpr.py) against an in-memory SQLite DB.

Run: python -m pytest tests/
"""
import json

from models import (
    db, AppSetting, ClearanceMirror, CustomerMap, FailedSyncOrder,
    ProcessedOrder, ProductMap, Shop, SyncHealth, SyncLog,
)
from services import gdpr

SHOP = "a-shop.myshopify.com"
OTHER = "b-shop.myshopify.com"
EMAIL = "Jane_Doe@Example.com"


def order(oid, cid, email, name="#1001"):
    return json.dumps({"id": oid, "name": name, "email": email,
                       "customer": {"id": cid, "email": email, "first_name": "Jane"},
                       "shipping_address": {"address1": "1 Secret St"}})


def seed():
    for shop in (SHOP, OTHER):
        db.session.add(Shop(shop_url=shop, access_token="shpat_x", odoo_url="https://odoo"))
        db.session.add(CustomerMap(shop_url=shop, shopify_customer_id="111",
                                   odoo_partner_id=9, email=EMAIL.lower(),
                                   password_hash="hash", reset_token="tok"))
        db.session.add(FailedSyncOrder(shop_url=shop, shopify_id="5001",
                                       order_data=order(5001, 111, EMAIL)))
        db.session.add(SyncLog(shop_url=shop, entity="Customer Sync", status="Error",
                               message=f"Failed {EMAIL.lower()}: boom"))
        db.session.add(ProcessedOrder(shop_url=shop, shopify_id="5001"))
        db.session.add(ProductMap(shop_url=shop, shopify_variant_id="1", odoo_product_id=1, sku="S1"))
        db.session.add(ClearanceMirror(shop_url=shop, base_sku="S1", clr_sku="S1-CLR", odoo_product_id=1))
        db.session.add(SyncHealth(shop_url=shop, entity="orders", last_error=f"bad {EMAIL}"))
        db.session.add(AppSetting(shop_url=shop, key="smtp_pass", value="enc"))
    # Another customer in SHOP who must survive a redact.
    db.session.add(CustomerMap(shop_url=SHOP, shopify_customer_id="222",
                               odoo_partner_id=10, email="other@example.com"))
    # A blank email must never be matched by anything.
    db.session.add(CustomerMap(shop_url=SHOP, shopify_customer_id="333",
                               odoo_partner_id=11, email=""))
    # Order whose id merely contains "111" as a substring: not this customer.
    db.session.add(FailedSyncOrder(shop_url=SHOP, shopify_id="91119",
                                   order_data=order(91119, 777, "x@example.com")))
    # Order listed in orders_to_redact but placed as a guest.
    db.session.add(FailedSyncOrder(shop_url=SHOP, shopify_id="6002",
                                   order_data=order(6002, None, "guest@example.com")))
    db.session.commit()


def redact_payload():
    return {"shop_domain": SHOP, "customer": {"id": 111, "email": EMAIL},
            "orders_to_redact": [6002]}


def test_customer_redact_erases_only_that_customer_in_that_shop(app_ctx, capsys):
    seed()
    counts = gdpr.redact_customer(SHOP, redact_payload())

    ids = {m.shopify_customer_id for m in CustomerMap.query.filter_by(shop_url=SHOP)}
    assert ids == {"222", "333"}
    left = {f.shopify_id for f in FailedSyncOrder.query.filter_by(shop_url=SHOP)}
    assert left == {"91119"}
    for log in SyncLog.query.filter_by(shop_url=SHOP):
        assert EMAIL.lower() not in log.message.lower()
    assert "[redacted]" in SyncHealth.query.filter_by(shop_url=SHOP).one().last_error
    assert counts["customer_maps"] == 1 and counts["failed_orders"] == 2

    # Other shop untouched.
    assert CustomerMap.query.filter_by(shop_url=OTHER).count() == 1
    assert FailedSyncOrder.query.filter_by(shop_url=OTHER).count() == 1
    assert EMAIL.lower() in SyncLog.query.filter_by(shop_url=OTHER).one().message

    assert EMAIL.lower() not in capsys.readouterr().out.lower()


def test_customer_redact_with_no_identifiers_touches_nothing(app_ctx):
    seed()
    before = CustomerMap.query.count()
    gdpr.redact_customer(SHOP, {"customer": {"id": None, "email": ""}})
    assert CustomerMap.query.count() == before


def test_customer_redact_by_gid_and_email_only(app_ctx):
    seed()
    gdpr.redact_customer(SHOP, {"customer": {"id": "gid://shopify/Customer/111"}})
    assert CustomerMap.query.filter_by(shop_url=SHOP, shopify_customer_id="111").count() == 0
    seed_email_only = {"customer": {"email": "OTHER@example.com"}}
    gdpr.redact_customer(SHOP, seed_email_only)
    assert CustomerMap.query.filter_by(shop_url=SHOP, shopify_customer_id="222").count() == 0
    assert CustomerMap.query.filter_by(shop_url=SHOP, shopify_customer_id="333").count() == 1


def test_data_request_stores_export_when_no_email_and_logs_no_pii(app_ctx, capsys):
    seed()
    result = gdpr.handle_data_request(
        SHOP, {"customer": {"id": 111, "email": EMAIL}, "orders_requested": [5001],
               "data_request": {"id": 42}},
        recipient=None, smtp=None)
    assert result["delivered"] == "stored"
    assert result["mappings"] == 1 and result["orders_awaiting_retry"] == 1
    assert result["orders_synced"] == 1 and result["log_entries"] == 1

    stored = AppSetting.query.filter_by(shop_url=SHOP, key="gdpr_export_111").one()
    export = json.loads(stored.value)
    assert export["customer_mappings"][0]["email"] == EMAIL.lower()
    assert "hash" not in json.dumps(export["customer_mappings"])
    assert export["orders_awaiting_retry"][0]["order"]["id"] == 5001

    audit = SyncLog.query.filter_by(shop_url=SHOP, entity="GDPR").one()
    assert "example.com" not in audit.message.lower()
    assert "example.com" not in capsys.readouterr().out.lower()

    # A later redact removes the stored export too.
    gdpr.redact_customer(SHOP, redact_payload())
    assert AppSetting.query.filter_by(shop_url=SHOP, key="gdpr_export_111").count() == 0


def test_data_request_emails_merchant_when_configured(app_ctx, monkeypatch):
    seed()
    sent = {}

    class FakeSMTP:
        def __init__(self, *a, **k): pass
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def login(self, *a): pass
        def send_message(self, msg): sent["to"] = msg["To"]; sent["n"] = len(list(msg.iter_attachments()))

    monkeypatch.setattr(gdpr.smtplib, "SMTP_SSL", FakeSMTP)
    result = gdpr.handle_data_request(
        SHOP, {"customer": {"id": 111, "email": EMAIL}},
        recipient="merchant@shop.test",
        smtp={"server": "s", "port": 465, "sender": "from@x", "password": "p"})
    assert result["delivered"] == "email"
    assert sent == {"to": "merchant@shop.test", "n": 1}
    assert AppSetting.query.filter(AppSetting.key.like("gdpr_export_%")).count() == 0


def test_shop_redact_erases_every_table_for_that_shop_only(app_ctx):
    seed()
    counts = gdpr.redact_shop(SHOP)
    for model in gdpr.SHOP_SCOPED_MODELS:
        assert model.query.filter_by(shop_url=SHOP).count() == 0, model.__tablename__
        assert model.query.filter_by(shop_url=OTHER).count() >= 1, model.__tablename__
    assert counts["shop"] == 1


def test_shop_scoped_models_cover_every_table():
    tables = set(db.metadata.tables)
    covered = {m.__tablename__ for m in gdpr.SHOP_SCOPED_MODELS}
    assert tables == covered, "a new table was added; decide whether shop/redact must clear it"


def test_old_uninstall_code_violated_not_null(app_ctx):
    """The pre-fix handler set access_token = None; this is why it never committed."""
    import pytest
    from sqlalchemy.exc import IntegrityError
    seed()
    shop = Shop.query.filter_by(shop_url=SHOP).one()
    shop.is_active = False
    shop.access_token = None
    with pytest.raises(IntegrityError):
        db.session.commit()
    db.session.rollback()


def test_mark_uninstalled_respects_not_null_token(app_ctx):
    seed()
    db.session.add(AppSetting(shop_url=SHOP, key="storefront_access_token", value="tok"))
    db.session.commit()
    assert gdpr.mark_uninstalled(SHOP) is True
    shop = Shop.query.filter_by(shop_url=SHOP).one()
    assert shop.is_active is False and shop.access_token == ""
    assert AppSetting.query.filter_by(shop_url=SHOP, key="storefront_access_token").count() == 0
    # Odoo config kept for a reinstall inside 48h.
    assert shop.odoo_url == "https://odoo"
    assert gdpr.mark_uninstalled("missing.myshopify.com") is False
