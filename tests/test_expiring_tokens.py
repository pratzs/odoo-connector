"""Expiring offline tokens: refresh timing, legacy passthrough, lock, migration."""
import threading
import time
from datetime import datetime, timedelta

import pytest

import shopify_tokens as st
from models import db, Shop, SyncLog
from security_utils import encrypt_val, decrypt_val

SHOP = "test-store.myshopify.com"


class FakeResp:
    def __init__(self, status, body):
        self.status_code = status
        self._body = body

    def json(self):
        return self._body


class FakeLock:
    def __init__(self, registry, name, blocking_timeout):
        self.lk = registry.setdefault(name, threading.Lock())
        self.blocking_timeout = blocking_timeout

    def acquire(self, blocking=True):
        return self.lk.acquire(timeout=self.blocking_timeout)

    def release(self):
        self.lk.release()


class FakeRedis:
    def __init__(self):
        self.locks = {}
        self.lock_calls = 0

    def lock(self, name, timeout=None, blocking_timeout=None):
        self.lock_calls += 1
        return FakeLock(self.locks, name, blocking_timeout)


@pytest.fixture
def env(app_ctx, monkeypatch):
    monkeypatch.setenv("SHOPIFY_API_KEY", "cid")
    monkeypatch.setenv("SHOPIFY_API_SECRET", "csecret")
    redis = FakeRedis()
    monkeypatch.setattr(st, "_redis", lambda: redis)
    calls = []

    def post(url, data=None, headers=None, timeout=None):
        calls.append({"url": url, "data": dict(data)})
        return post.response(len(calls))

    n = {"i": 0}

    def default_response(i):
        return FakeResp(200, {"access_token": f"shpat_new{i}", "expires_in": 3600,
                              "refresh_token": f"shprt_new{i}", "refresh_token_expires_in": 7776000,
                              "scope": "read_products"})
    post.response = default_response
    monkeypatch.setattr(st.requests, "post", post)
    return {"app": app_ctx, "redis": redis, "calls": calls, "post": post}


def _add_shop(token="shpat_old", expires_in_min=None, refresh="shprt_old"):
    shop = Shop(shop_url=SHOP, access_token=token)
    if expires_in_min is not None:
        shop.access_token_expires_at = datetime.utcnow() + timedelta(minutes=expires_in_min)
        shop.refresh_token_enc = encrypt_val(refresh)
        shop.refresh_token_expires_at = datetime.utcnow() + timedelta(days=80)
    db.session.add(shop)
    db.session.commit()
    return shop


# --- legacy path ---------------------------------------------------------
def test_legacy_shop_returns_stored_token_without_network_or_lock(env):
    shop = _add_shop("shpat_legacy")
    assert st.get_valid_access_token(shop) == "shpat_legacy"
    assert st.get_valid_access_token(SHOP) == "shpat_legacy"
    assert env["calls"] == [] and env["redis"].lock_calls == 0
    db.session.refresh(shop)
    assert shop.access_token_expires_at is None and shop.refresh_token_enc is None


def test_legacy_shop_stale_token_does_not_refresh(env):
    shop = _add_shop("shpat_legacy")
    assert st.get_valid_access_token(shop, stale_token="shpat_legacy") == "shpat_legacy"
    assert env["calls"] == []


# --- expiring path -------------------------------------------------------
def test_fresh_expiring_token_is_not_refreshed(env):
    shop = _add_shop(expires_in_min=30)
    assert st.get_valid_access_token(shop) == "shpat_old"
    assert env["calls"] == [] and env["redis"].lock_calls == 0


def test_token_near_expiry_is_refreshed_and_rotated(env):
    shop = _add_shop(expires_in_min=2)
    token = st.get_valid_access_token(shop)
    assert token == "shpat_new1"
    (call,) = env["calls"]
    assert call["url"] == f"https://{SHOP}/admin/oauth/access_token"
    assert call["data"] == {"client_id": "cid", "client_secret": "csecret",
                            "grant_type": "refresh_token", "refresh_token": "shprt_old"}
    db.session.expire_all()
    row = Shop.query.filter_by(shop_url=SHOP).first()
    assert row.access_token == "shpat_new1"
    assert decrypt_val(row.refresh_token_enc) == "shprt_new1"
    assert row.refresh_token_enc != "shprt_new1"  # stored encrypted
    assert row.access_token_expires_at > datetime.utcnow() + timedelta(minutes=55)
    assert row.refresh_token_expires_at > datetime.utcnow() + timedelta(days=89)


def test_expired_token_is_refreshed(env):
    shop = _add_shop(expires_in_min=-10)
    assert st.get_valid_access_token(shop) == "shpat_new1"


def test_refresh_does_not_commit_callers_pending_work(env):
    shop = _add_shop(expires_in_min=1)
    db.session.add(SyncLog(shop_url=SHOP, entity="Job", status="Info", message="pending"))
    st.get_valid_access_token(shop)
    db.session.rollback()
    assert SyncLog.query.filter_by(entity="Job").count() == 0
    assert Shop.query.filter_by(shop_url=SHOP).first().access_token == "shpat_new1"


def test_refresh_failure_keeps_current_token_and_logs(env):
    env["post"].response = lambda i: FakeResp(400, {"error": "invalid_grant"})
    shop = _add_shop(expires_in_min=2)
    assert st.get_valid_access_token(shop) == "shpat_old"
    db.session.expire_all()
    row = Shop.query.filter_by(shop_url=SHOP).first()
    assert row.access_token == "shpat_old" and decrypt_val(row.refresh_token_enc) == "shprt_old"
    log = SyncLog.query.filter_by(entity="Shopify Token").first()
    assert log and "invalid_grant" in log.message and "shprt" not in log.message
    assert len(env["calls"]) == 1  # 4xx is not retried


def test_transient_5xx_is_retried_once(env, monkeypatch):
    monkeypatch.setattr(st.time, "sleep", lambda s: None)
    responses = [FakeResp(502, {}), None]
    default = env["post"].response
    env["post"].response = lambda i: responses[0] if i == 1 else default(i)
    shop = _add_shop(expires_in_min=1)
    assert st.get_valid_access_token(shop) == "shpat_new2"
    assert [c["data"]["refresh_token"] for c in env["calls"]] == ["shprt_old", "shprt_old"]


# --- lock / double refresh ----------------------------------------------
def test_second_caller_with_stale_copy_does_not_refresh_again(env):
    _add_shop(expires_in_min=2)
    # Two processes loaded the row before either refreshed.
    stale_a = Shop.query.filter_by(shop_url=SHOP).first()
    stale_b_expiry = stale_a.access_token_expires_at
    assert st.get_valid_access_token(stale_a) == "shpat_new1"
    # Process B still believes the token expires in 2 minutes.
    db.session.expire_all()
    stale_b = Shop.query.filter_by(shop_url=SHOP).first()
    from sqlalchemy.orm.attributes import set_committed_value
    set_committed_value(stale_b, "access_token", "shpat_old")
    set_committed_value(stale_b, "access_token_expires_at", stale_b_expiry)
    assert st.get_valid_access_token(stale_b) == "shpat_new1"
    assert len(env["calls"]) == 1


def test_busy_lock_means_no_refresh(env):
    _add_shop(expires_in_min=2)
    shop = Shop.query.filter_by(shop_url=SHOP).first()
    held = env["redis"].locks.setdefault(st._lock_name(SHOP), threading.Lock())
    held.acquire()
    try:
        orig = st.LOCK_WAIT_SECONDS
        st.LOCK_WAIT_SECONDS = 0.05
        try:
            assert st.get_valid_access_token(shop) == "shpat_old"
        finally:
            st.LOCK_WAIT_SECONDS = orig
    finally:
        held.release()
    assert env["calls"] == []


def test_concurrent_refreshes_hit_shopify_once(env):
    _add_shop(expires_in_min=2)
    default = env["post"].response

    def slow(i):
        time.sleep(0.2)  # hold the lock while the other thread waits
        return default(i)
    env["post"].response = slow
    app = env["app"]
    results, errors = [], []

    def worker():
        try:
            with app.app_context():
                s = db.session.get(Shop, 1)
                results.append(st.get_valid_access_token(s))
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert len(env["calls"]) == 1
    assert results == ["shpat_new1", "shpat_new1"]


def test_redis_down_still_refreshes(env, monkeypatch):
    def boom():
        raise ConnectionError("redis down")
    monkeypatch.setattr(st, "_redis", boom)
    shop = _add_shop(expires_in_min=1)
    assert st.get_valid_access_token(shop) == "shpat_new1"


# --- 401 path -------------------------------------------------------------
def test_stale_token_forces_refresh(env):
    shop = _add_shop(expires_in_min=50)
    assert st.get_valid_access_token(shop, stale_token="shpat_old") == "shpat_new1"


def test_stale_token_already_replaced_is_not_refreshed_again(env):
    shop = _add_shop(token="shpat_current", expires_in_min=50)
    assert st.get_valid_access_token(shop, stale_token="shpat_previous") == "shpat_current"
    assert env["calls"] == []


# --- OAuth callback payloads ---------------------------------------------
def test_oauth_code_exchange_requests_expiring(env):
    payload = st.exchange_code_for_token(SHOP, "abc")
    assert env["calls"][0]["data"] == {"client_id": "cid", "client_secret": "csecret",
                                       "code": "abc", "expiring": "1"}
    shop = Shop(shop_url=SHOP)
    db.session.add(shop)
    st.store_oauth_token(shop, payload)
    db.session.commit()
    assert shop.access_token == "shpat_new1" and shop.refresh_token == "shprt_new1"
    assert shop.access_token_expires_at is not None


def test_oauth_kill_switch_requests_legacy(env, monkeypatch):
    monkeypatch.setenv("SHOPIFY_EXPIRING_OFFLINE_TOKENS", "0")
    st.exchange_code_for_token(SHOP, "abc")
    assert "expiring" not in env["calls"][0]["data"]


def test_non_expiring_payload_stores_legacy_shape():
    vals = st.token_values_from_payload({"access_token": "shpat_x", "scope": "a"})
    assert vals == {"access_token": "shpat_x", "access_token_expires_at": None,
                    "refresh_token_enc": None, "refresh_token_expires_at": None}


def test_token_calls_refuse_non_shopify_hosts(env):
    with pytest.raises(st.TokenError):
        st.exchange_code_for_token("evil.example.com", "abc")
    assert env["calls"] == []


# --- legacy migration (flagged off by default) ---------------------------
def test_migration_is_off_by_default(env):
    _add_shop("shpat_legacy")
    assert st.migrate_legacy_token(SHOP)["status"] == "skipped"
    assert st.migrate_all_legacy_tokens()[0]["status"] == "skipped"
    assert env["calls"] == []
    assert Shop.query.filter_by(shop_url=SHOP).first().access_token == "shpat_legacy"


def test_migration_when_enabled(env, monkeypatch):
    monkeypatch.setenv("SHOPIFY_MIGRATE_LEGACY_TOKENS", "1")
    _add_shop("shpat_legacy")
    assert st.migrate_legacy_token(SHOP)["status"] == "migrated"
    assert env["calls"][0]["data"] == {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "client_id": "cid", "client_secret": "csecret",
        "subject_token": "shpat_legacy",
        "subject_token_type": "urn:shopify:params:oauth:token-type:offline-access-token",
        "requested_token_type": "urn:shopify:params:oauth:token-type:offline-access-token",
        "expiring": "1",
    }
    db.session.expire_all()
    row = Shop.query.filter_by(shop_url=SHOP).first()
    assert row.access_token == "shpat_new1" and row.refresh_token == "shprt_new1"
    # Second run is a no-op.
    assert st.migrate_legacy_token(SHOP)["status"] == "skipped"
    assert len(env["calls"]) == 1


def test_migration_failure_keeps_legacy_token(env, monkeypatch):
    monkeypatch.setenv("SHOPIFY_MIGRATE_LEGACY_TOKENS", "1")
    env["post"].response = lambda i: FakeResp(400, {"error": "invalid_subject_token"})
    _add_shop("shpat_legacy")
    assert st.migrate_legacy_token(SHOP)["status"] == "error"
    db.session.expire_all()
    row = Shop.query.filter_by(shop_url=SHOP).first()
    assert row.access_token == "shpat_legacy" and row.refresh_token_enc is None


# --- keep-alive -------------------------------------------------------------
def test_keepalive_refreshes_only_aging_refresh_tokens(env):
    shop = _add_shop(expires_in_min=50)
    assert st.refresh_aging_tokens() == {"checked": 0, "refreshed": 0}
    shop.refresh_token_expires_at = datetime.utcnow() + timedelta(days=10)
    db.session.commit()
    assert st.refresh_aging_tokens() == {"checked": 1, "refreshed": 1}


# --- library session hooks ---------------------------------------------------
def test_setup_session_legacy_and_expiring(env):
    import shopify
    import utils
    _add_shop("shpat_legacy")
    assert utils.setup_shopify_session(SHOP)
    assert shopify.ShopifyResource.get_headers()["X-Shopify-Access-Token"] == "shpat_legacy"
    st.ensure_active_session_fresh()  # legacy: no-op
    assert env["calls"] == []

    shop = Shop.query.filter_by(shop_url=SHOP).first()
    shop.access_token = "shpat_exp"
    shop.access_token_expires_at = datetime.utcnow() + timedelta(minutes=30)
    shop.refresh_token = "shprt_old"
    shop.refresh_token_expires_at = datetime.utcnow() + timedelta(days=80)
    db.session.commit()
    assert utils.setup_shopify_session(SHOP)
    assert shopify.ShopifyResource.get_headers()["X-Shopify-Access-Token"] == "shpat_exp"
    gql = shopify.GraphQL()
    # Simulate a long job: the token is now about to expire.
    shop.access_token_expires_at = datetime.utcnow() + timedelta(minutes=1)
    db.session.commit()
    st._active.expires_at = shop.access_token_expires_at
    st.ensure_active_session_fresh()
    assert shopify.ShopifyResource.get_headers()["X-Shopify-Access-Token"] == "shpat_new1"
    assert gql.headers["X-Shopify-Access-Token"] == "shpat_new1"  # same dict, picked up live
    shopify.ShopifyResource.clear_session()
    st.clear_active_session()


def test_uninstall_clears_refresh_token(env):
    from services import gdpr
    _add_shop(expires_in_min=30)
    assert gdpr.mark_uninstalled(SHOP)
    row = Shop.query.filter_by(shop_url=SHOP).first()
    assert row.access_token == "" and row.refresh_token_enc is None
    assert row.access_token_expires_at is None and row.refresh_token_expires_at is None


def test_graphql_401_is_retried_once_with_refreshed_token(env, monkeypatch):
    import io
    import urllib.error
    import shopify
    import utils
    from shopify.resources import graphql as gql_mod
    shop = _add_shop(token="shpat_revoked", expires_in_min=40)
    assert utils.setup_shopify_session(SHOP)
    sent = []

    def urlopen(req):
        tok = req.get_header("X-shopify-access-token")
        sent.append(tok)
        if tok == "shpat_revoked":
            raise urllib.error.HTTPError(req.full_url, 401, "Unauthorized", {}, io.BytesIO(b"{}"))
        return io.BytesIO(b'{"data": {}}')
    monkeypatch.setattr(gql_mod.urllib.request, "urlopen", urlopen)
    assert shopify.GraphQL().execute("{ shop { name } }") == '{"data": {}}'
    assert sent == ["shpat_revoked", "shpat_new1"]

    # Legacy shop: a 401 is raised as before, no refresh attempted.
    db.session.expire_all()
    shop = Shop.query.filter_by(shop_url=SHOP).first()
    shop.access_token = "shpat_revoked"
    shop.refresh_token_enc = None
    shop.access_token_expires_at = None
    db.session.commit()
    assert utils.setup_shopify_session(SHOP)
    sent.clear()
    with pytest.raises(urllib.error.HTTPError):
        shopify.GraphQL().execute("{ shop { name } }")
    assert len(env["calls"]) == 1
    shopify.ShopifyResource.clear_session()
    st.clear_active_session()
