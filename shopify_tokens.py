"""
Shopify offline access tokens: expiring tokens with refresh.

Shopify is retiring non-expiring offline tokens for public apps (delisted and
blocked from new installs on 2027-01-01, Admin API requests rejected from
2027-04-01). An expiring offline token lasts 1 hour and comes with a refresh
token (90 days) that is ROTATED on every refresh.

Docs this follows:
  https://shopify.dev/docs/apps/build/authentication-authorization/migrate-to-expiring-offline-access-tokens
  https://shopify.dev/docs/apps/build/authentication-authorization/access-tokens
  https://shopify.dev/docs/apps/build/authentication-authorization/implement-token-exchange
  https://shopify.dev/changelog/posts/offline-access-tokens-now-support-expiry-and-refresh

Rules that shaped this module:
  * Legacy shops (no refresh token stored) are untouched: get_valid_access_token
    returns Shop.access_token exactly as before.
  * One refresh per store at a time. A Redis lock serialises web, worker and
    clock; after taking it we re-read the row, because another process may
    already have refreshed.
  * Token writes happen in their own DB session so a refresh in the middle of a
    sync job never commits (or rolls back) the job's own pending work.
  * The refresh token is stored encrypted (security_utils, same Fernet key as
    the Odoo password). Tokens are never logged.
"""
import os
import re
import threading
import time
from datetime import datetime, timedelta

import requests
from sqlalchemy.orm import Session as _DbSession
from sqlalchemy.orm.attributes import set_committed_value

from models import db, Shop, SyncLog
from security_utils import encrypt_val, decrypt_val

REFRESH_MARGIN = timedelta(minutes=5)
# Refresh a dormant store's token pair this long before its refresh token dies.
REFRESH_TOKEN_KEEPALIVE = timedelta(days=30)
LOCK_TTL_SECONDS = 60
LOCK_WAIT_SECONDS = 20
HTTP_TIMEOUT = 20

OFFLINE_TOKEN_TYPE = "urn:shopify:params:oauth:token-type:offline-access-token"
TOKEN_EXCHANGE_GRANT = "urn:ietf:params:oauth:grant-type:token-exchange"


class TokenError(Exception):
    """A Shopify token endpoint call failed. Message never contains a token."""


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
def _client_credentials():
    return os.getenv("SHOPIFY_API_KEY"), os.getenv("SHOPIFY_API_SECRET")


def expiring_tokens_enabled():
    """Kill switch for NEW tokens (OAuth callback). Default ON."""
    return os.getenv("SHOPIFY_EXPIRING_OFFLINE_TOKENS", "1").strip().lower() not in ("0", "false", "no", "off")


def legacy_migration_enabled():
    """Gate for exchanging stored non-expiring tokens. Default OFF."""
    return os.getenv("SHOPIFY_MIGRATE_LEGACY_TOKENS", "0").strip().lower() in ("1", "true", "yes", "on")


def _redis():
    from utils import conn  # lazy: utils imports this module
    return conn


def _now():
    return datetime.utcnow()


_SHOP_DOMAIN_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9\-]*\.myshopify\.com$")


def is_valid_shop_domain(shop_url):
    """Every token call POSTs the client secret to https://{shop}; only ever
    send it to a *.myshopify.com host."""
    return bool(shop_url and _SHOP_DOMAIN_RE.match(shop_url))


def _lock_name(shop_url):
    return f"shopify_token_refresh:{shop_url}"


# ---------------------------------------------------------------------------
# Token payload -> Shop row
# ---------------------------------------------------------------------------
def token_values_from_payload(payload, now=None):
    """
    Normalises a token endpoint response into column values. A response
    without refresh_token (non-expiring token) yields NULL expiry/refresh
    columns, which is exactly the legacy shape.
    """
    now = now or _now()
    access_token = payload.get("access_token")
    if not access_token:
        raise TokenError("token response had no access_token")
    refresh_token = payload.get("refresh_token")
    expires_in = payload.get("expires_in")
    rt_expires_in = payload.get("refresh_token_expires_in")
    if not refresh_token:
        return {"access_token": access_token, "access_token_expires_at": None,
                "refresh_token_enc": None, "refresh_token_expires_at": None}
    return {
        "access_token": access_token,
        "access_token_expires_at": now + timedelta(seconds=int(expires_in)) if expires_in else None,
        "refresh_token_enc": encrypt_val(refresh_token),
        "refresh_token_expires_at": now + timedelta(seconds=int(rt_expires_in)) if rt_expires_in else None,
    }


def apply_token_payload(shop, payload, now=None):
    """Writes a token response onto a Shop (caller commits)."""
    for k, v in token_values_from_payload(payload, now).items():
        setattr(shop, k, v)


def _sync_in_memory(shop, values):
    """Mirror freshly committed values onto the caller's Shop object without
    marking it dirty in the caller's session."""
    if shop is None:
        return
    for k, v in values.items():
        set_committed_value(shop, k, v)


def _read_row(shop_url):
    """Reads the token columns in an independent session (never stale)."""
    with _DbSession(db.engine) as s:
        row = s.query(Shop).filter_by(shop_url=shop_url).first()
        if not row:
            return None
        return {
            "access_token": row.access_token,
            "access_token_expires_at": row.access_token_expires_at,
            "refresh_token_enc": row.refresh_token_enc,
            "refresh_token_expires_at": row.refresh_token_expires_at,
        }


def _write_row(shop_url, values):
    with _DbSession(db.engine) as s:
        row = s.query(Shop).filter_by(shop_url=shop_url).first()
        if not row:
            raise TokenError("shop row disappeared during token write")
        for k, v in values.items():
            setattr(row, k, v)
        s.commit()


def _log(shop_url, status, message):
    print(f"[ShopifyToken] {shop_url} {status}: {message}")
    try:
        with _DbSession(db.engine) as s:
            s.add(SyncLog(shop_url=shop_url, entity="Shopify Token", status=status,
                          message=message, timestamp=_now()))
            s.commit()
    except Exception as e:
        print(f"[ShopifyToken] could not write SyncLog: {type(e).__name__}")


# ---------------------------------------------------------------------------
# Shopify token endpoint calls
# ---------------------------------------------------------------------------
def _post_token(shop_url, data, attempts=2):
    """
    POST https://{shop}/admin/oauth/access_token. Retries once on network
    errors / 5xx (Shopify documents refresh as safe to retry with the same
    refresh token). 4xx is final.
    """
    if not is_valid_shop_domain(shop_url):
        raise TokenError("refusing token call to a non-myshopify host")
    url = f"https://{shop_url}/admin/oauth/access_token"
    last = None
    for i in range(attempts):
        try:
            resp = requests.post(url, data=data, headers={"Accept": "application/json"}, timeout=HTTP_TIMEOUT)
        except requests.RequestException as e:
            last = TokenError(f"network error: {type(e).__name__}")
        else:
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError:
                    last = TokenError("token endpoint returned non-JSON")
            elif resp.status_code >= 500:
                last = TokenError(f"HTTP {resp.status_code}")
            else:
                err = ""
                try:
                    err = (resp.json() or {}).get("error", "")
                except ValueError:
                    pass
                raise TokenError(f"HTTP {resp.status_code} {err}".strip())
        if i + 1 < attempts:
            time.sleep(1)
    raise last


def exchange_code_for_token(shop_url, code, expiring=None):
    """Authorization code grant (OAuth callback). expiring=1 per Shopify docs."""
    client_id, client_secret = _client_credentials()
    if expiring is None:
        expiring = expiring_tokens_enabled()
    data = {"client_id": client_id, "client_secret": client_secret, "code": code}
    if expiring:
        data["expiring"] = "1"
    # A code is single use: no retry.
    return _post_token(shop_url, data, attempts=1)


def _request_refresh(shop_url, refresh_token):
    client_id, client_secret = _client_credentials()
    return _post_token(shop_url, {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    })


def _request_legacy_migration(shop_url, legacy_token):
    client_id, client_secret = _client_credentials()
    return _post_token(shop_url, {
        "grant_type": TOKEN_EXCHANGE_GRANT,
        "client_id": client_id,
        "client_secret": client_secret,
        "subject_token": legacy_token,
        "subject_token_type": OFFLINE_TOKEN_TYPE,
        "requested_token_type": OFFLINE_TOKEN_TYPE,
        "expiring": "1",
    })


# ---------------------------------------------------------------------------
# Lock
# ---------------------------------------------------------------------------
class _ShopTokenLock:
    """
    Redis lock per shop. acquired is True/False, or None when Redis itself is
    unreachable (then we proceed unlocked rather than stall every sync).
    """

    def __init__(self, shop_url, wait=None):
        self.shop_url = shop_url
        self.wait = LOCK_WAIT_SECONDS if wait is None else wait
        self.lock = None
        self.acquired = None

    def __enter__(self):
        try:
            self.lock = _redis().lock(_lock_name(self.shop_url), timeout=LOCK_TTL_SECONDS,
                                      blocking_timeout=self.wait)
            self.acquired = bool(self.lock.acquire(blocking=True))
        except Exception as e:
            print(f"[ShopifyToken] lock unavailable for {self.shop_url}: {type(e).__name__}")
            self.lock, self.acquired = None, None
        return self

    def __exit__(self, *exc):
        if self.acquired and self.lock is not None:
            try:
                self.lock.release()
            except Exception:
                pass
        return False


# ---------------------------------------------------------------------------
# Public: the one helper every Shopify call goes through
# ---------------------------------------------------------------------------
def needs_refresh(expires_at, now=None):
    if expires_at is None:
        return False
    return (now or _now()) >= expires_at - REFRESH_MARGIN


def is_expiring(shop):
    return bool(shop is not None and getattr(shop, "refresh_token_enc", None))


def get_valid_access_token(shop, stale_token=None):
    """
    Returns a usable Admin API access token for the shop.

    * Legacy shop (no refresh token): returns shop.access_token unchanged.
    * Expiring shop: returns the stored token, refreshing it first when it is
      within REFRESH_MARGIN of expiry.
    * stale_token: the token a request was just rejected with (401). Forces a
      refresh unless another process has already replaced that token.

    Never raises for refresh failures: it logs and returns the best token it
    has, so a Shopify hiccup degrades to the same 401 the caller would have got.
    """
    if isinstance(shop, str):
        shop = Shop.query.filter_by(shop_url=shop).first()
    if shop is None or not shop.access_token:
        return shop.access_token if shop is not None else None

    if not is_expiring(shop):
        return shop.access_token

    if stale_token is None and not needs_refresh(shop.access_token_expires_at):
        return shop.access_token

    return _refresh_locked(shop, stale_token)


def _refresh_locked(shop, stale_token=None):
    shop_url = shop.shop_url
    with _ShopTokenLock(shop_url) as lk:
        row = _read_row(shop_url)
        if row is None:
            return shop.access_token
        _sync_in_memory(shop, row)

        if lk.acquired is False:
            # Another process held the lock for the whole wait; it has most
            # likely refreshed. Use whatever is stored now.
            return shop.access_token

        if not row["refresh_token_enc"]:
            return shop.access_token  # became legacy / uninstalled meanwhile

        already_fresh = not needs_refresh(row["access_token_expires_at"])
        if stale_token is None and already_fresh:
            return shop.access_token  # another process refreshed first
        if stale_token is not None and row["access_token"] != stale_token:
            return shop.access_token  # someone already replaced the rejected token

        refresh_token = decrypt_val(row["refresh_token_enc"])
        if not refresh_token:
            _log(shop_url, "Error", "Stored refresh token could not be decrypted (ENCRYPTION_KEY changed?). Merchant must reopen the app.")
            return shop.access_token

        try:
            payload = _request_refresh(shop_url, refresh_token)
            values = token_values_from_payload(payload)
            if not values["refresh_token_enc"]:
                # Should not happen; keep the current pair rather than
                # silently downgrading to a token we cannot refresh.
                raise TokenError("refresh response had no refresh_token")
            _write_row(shop_url, values)
        except TokenError as e:
            _log(shop_url, "Error", f"Shopify token refresh failed ({e}). Using the current token; if this keeps failing the merchant must reopen the app.")
            return shop.access_token
        except Exception as e:
            _log(shop_url, "Error", f"Shopify token refresh failed ({type(e).__name__}).")
            return shop.access_token

        _sync_in_memory(shop, values)
        return shop.access_token


def store_oauth_token(shop, payload):
    """
    OAuth callback: write the new token (or pair) onto the Shop under the
    refresh lock, so a refresh running elsewhere cannot overwrite it with a
    pair this acquisition just retired. Caller commits db.session.
    """
    with _ShopTokenLock(shop.shop_url, wait=10):
        apply_token_payload(shop, payload)


def refresh_aging_tokens(now=None):
    """
    Daily keep-alive. A refresh token dies after 90 days; an active store
    refreshes hourly anyway, but a store with no Shopify traffic would not.
    Refreshes any expiring shop whose refresh token ends within 30 days.
    """
    now = now or _now()
    cutoff = now + REFRESH_TOKEN_KEEPALIVE
    shops = Shop.query.filter(
        Shop.is_active.is_(True),
        Shop.refresh_token_enc.isnot(None),
        Shop.refresh_token_expires_at.isnot(None),
        Shop.refresh_token_expires_at < cutoff,
    ).all()
    done = 0
    for shop in shops:
        before = shop.access_token
        if get_valid_access_token(shop, stale_token=before) != before:
            done += 1
    return {"checked": len(shops), "refreshed": done}


# ---------------------------------------------------------------------------
# Optional one-shot migration of legacy (non-expiring) tokens. OFF by default.
# ---------------------------------------------------------------------------
def migrate_legacy_token(shop_url):
    """
    Exchanges a stored non-expiring token for an expiring pair (Shopify's
    documented migration token exchange). Shopify permanently invalidates the
    old token on success, so the new pair is written before anything else.
    The old token is only overwritten after a successful exchange: if the
    response is lost, the old token is still stored and a retry within 7 days
    returns the same pair (Shopify changelog 2026-09-29).
    """
    if not legacy_migration_enabled():
        return {"shop": shop_url, "status": "skipped", "reason": "SHOPIFY_MIGRATE_LEGACY_TOKENS is off"}

    with _ShopTokenLock(shop_url) as lk:
        if lk.acquired is False:
            return {"shop": shop_url, "status": "skipped", "reason": "token lock busy"}
        row = _read_row(shop_url)
        if row is None or not row["access_token"]:
            return {"shop": shop_url, "status": "skipped", "reason": "no stored token"}
        if row["refresh_token_enc"]:
            return {"shop": shop_url, "status": "skipped", "reason": "already expiring"}
        try:
            payload = _request_legacy_migration(shop_url, row["access_token"])
            values = token_values_from_payload(payload)
            if not values["refresh_token_enc"]:
                raise TokenError("migration response had no refresh_token")
            _write_row(shop_url, values)
        except TokenError as e:
            _log(shop_url, "Error", f"Legacy token migration failed ({e}). Old token kept.")
            return {"shop": shop_url, "status": "error", "reason": str(e)}
    _log(shop_url, "Success", "Migrated to an expiring offline access token.")
    return {"shop": shop_url, "status": "migrated"}


def migrate_all_legacy_tokens():
    if not legacy_migration_enabled():
        return [{"status": "skipped", "reason": "SHOPIFY_MIGRATE_LEGACY_TOKENS is off"}]
    shops = Shop.query.filter(Shop.is_active.is_(True), Shop.refresh_token_enc.is_(None)).all()
    return [migrate_legacy_token(s.shop_url) for s in shops if s.access_token]


# ---------------------------------------------------------------------------
# Keep the ShopifyAPI library's session fresh during long jobs
# ---------------------------------------------------------------------------
# setup_shopify_session() activates a token once; a product sync can run for
# up to 2 hours, longer than an expiring token lives. These hooks re-check the
# token before every REST/GraphQL request (a single attribute check for legacy
# shops) and retry once on a 401 with a refreshed token. The library keeps its
# headers in a per-thread dict that both REST and GraphQL read at request
# time, so updating that dict in place switches every caller to the new token.
_active = threading.local()


def track_active_session(shop_url, expires_at):
    """Called by utils.setup_shopify_session after activating a session."""
    _active.shop_url = shop_url
    _active.expires_at = expires_at


def clear_active_session():
    _active.shop_url = None
    _active.expires_at = None


def _set_library_token(token):
    import shopify
    shopify.ShopifyResource.get_headers()["X-Shopify-Access-Token"] = token


def _current_library_token():
    import shopify
    return shopify.ShopifyResource.get_headers().get("X-Shopify-Access-Token")


def _renew_active(stale_token=None):
    """Returns the new token if it changed, else None. Never raises."""
    shop_url = getattr(_active, "shop_url", None)
    if not shop_url:
        return None
    try:
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not is_expiring(shop):
            return None
        token = get_valid_access_token(shop, stale_token=stale_token)
        _active.expires_at = shop.access_token_expires_at
        if token and token != _current_library_token():
            _set_library_token(token)
            return token
    except Exception as e:
        print(f"[ShopifyToken] session renew skipped for {shop_url}: {type(e).__name__}")
    return None


def ensure_active_session_fresh():
    if getattr(_active, "expires_at", None) is None:
        return  # legacy shop or no session: nothing to do
    if needs_refresh(_active.expires_at):
        _renew_active()


def _active_is_expiring():
    return getattr(_active, "shop_url", None) and getattr(_active, "expires_at", None) is not None


_hooks_installed = False


def install_session_hooks():
    global _hooks_installed
    if _hooks_installed:
        return
    import urllib.error
    import shopify
    from shopify.base import ShopifyConnection
    from pyactiveresource.connection import UnauthorizedAccess

    orig_open = ShopifyConnection._open

    def _open(self, method, path, headers=None, data=None):
        ensure_active_session_fresh()
        if headers and "X-Shopify-Access-Token" in headers:
            headers["X-Shopify-Access-Token"] = _current_library_token() or headers["X-Shopify-Access-Token"]
        try:
            return orig_open(self, method, path, headers=headers, data=data)
        except UnauthorizedAccess:
            if not _active_is_expiring():
                raise
            sent = (headers or {}).get("X-Shopify-Access-Token")
            new = _renew_active(stale_token=sent)
            if not new:
                raise
            if headers is not None:
                headers["X-Shopify-Access-Token"] = new
            return orig_open(self, method, path, headers=headers, data=data)

    orig_execute = shopify.GraphQL.execute

    def execute(self, query, variables=None, operation_name=None):
        ensure_active_session_fresh()
        try:
            return orig_execute(self, query, variables=variables, operation_name=operation_name)
        except urllib.error.HTTPError as e:
            if e.code != 401 or not _active_is_expiring():
                raise
            new = _renew_active(stale_token=self.headers.get("X-Shopify-Access-Token"))
            if not new:
                raise
            self.headers["X-Shopify-Access-Token"] = new
            return orig_execute(self, query, variables=variables, operation_name=operation_name)

    ShopifyConnection._open = _open
    shopify.GraphQL.execute = execute
    _hooks_installed = True
