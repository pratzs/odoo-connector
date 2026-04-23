"""
Multipass Login Service
=======================
Handles B2B wholesale customer login via Odoo ID + password.

Flow:
  1. Customer visits /account/login on the Shopify store (B2B tab)
  2. Enters Store ID (Odoo partner ID) and their self-set password
  3. POST to /multipass/login on this connector app
  4. Connector validates credentials, generates a Shopify Multipass token
  5. Customer is redirected into Shopify as authenticated

Environment variables required:
  SHOPIFY_MULTIPASS_SECRET  -- from Shopify Admin > Settings > Customer accounts
  STORE_DOMAIN              -- e.g. worthyproducts.nz (custom storefront domain)
  STORE_URL                 -- e.g. https://worthyproducts.nz (full URL for email links)
  MULTIPASS_SMTP_USER       -- sender email for setup/reset emails
  MULTIPASS_SMTP_PASSWORD   -- SMTP password (falls back to SMTP_PASSWORD)
  MULTIPASS_SMTP_HOST       -- defaults to premium74.web-hosting.com
  MULTIPASS_SMTP_PORT       -- defaults to 465
"""

import os
import json
import hmac
import hashlib
import base64
import secrets
import smtplib
import requests
from datetime import datetime, timedelta
from email.message import EmailMessage

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding as crypto_padding
from werkzeug.security import generate_password_hash, check_password_hash

from models import db, CustomerMap


# ── Shopify Multipass token generation ────────────────────────────────────────

def _derive_keys(secret: str):
    digest = hashlib.sha256(secret.encode("utf-8")).digest()
    return digest[:16], digest[16:]  # enc_key, sig_key


def generate_multipass_token(email: str, shop_url: str, return_to: str = "/account") -> str | None:
    """
    Generates a Shopify Multipass token.
    Returns the full redirect URL on success, or None on failure.
    """
    secret = os.getenv("SHOPIFY_MULTIPASS_SECRET")
    if not secret:
        print("[Multipass] SHOPIFY_MULTIPASS_SECRET not set in environment.")
        return None

    try:
        enc_key, sig_key = _derive_keys(secret)

        payload = json.dumps({
            "email": email,
            "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "return_to": return_to,
        }).encode("utf-8")

        iv = os.urandom(16)
        padder = crypto_padding.PKCS7(128).padder()
        padded = padder.update(payload) + padder.finalize()

        cipher = Cipher(algorithms.AES(enc_key), modes.CBC(iv), backend=default_backend())
        enc = cipher.encryptor()
        ciphertext = enc.update(padded) + enc.finalize()

        message = iv + ciphertext
        sig = hmac.new(sig_key, message, hashlib.sha256).digest()

        token = base64.urlsafe_b64encode(message + sig).decode().rstrip("=")

        store_domain = os.getenv("STORE_DOMAIN", shop_url).replace("https://", "").replace("http://", "").rstrip("/")
        return f"https://{store_domain}/account/login/multipass/{token}"

    except Exception as e:
        print(f"[Multipass] Token generation error: {e}")
        return None


# ── Shopify Storefront password verification ───────────────────────────────────

def _get_storefront_token(shop_url: str) -> str | None:
    from utils import get_config, set_config
    from models import Shop

    cached = get_config("storefront_access_token", "", shop_url=shop_url)
    if cached:
        return cached

    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop or not shop.access_token:
        return None

    try:
        resp = requests.post(
            f"https://{shop_url}/admin/api/2024-01/storefront_access_tokens.json",
            headers={"X-Shopify-Access-Token": shop.access_token, "Content-Type": "application/json"},
            json={"storefront_access_token": {"title": "Connector Multipass Auth"}},
            timeout=10,
        )
        token = (resp.json().get("storefront_access_token") or {}).get("access_token")
        if token:
            set_config("storefront_access_token", token, shop_url=shop_url)
            return token
    except Exception as e:
        print(f"[Multipass] Could not create storefront token: {e}")
    return None


_STOREFRONT_AUTH_QUERY = """
mutation customerAccessTokenCreate($input: CustomerAccessTokenCreateInput!) {
  customerAccessTokenCreate(input: $input) {
    customerAccessToken { accessToken }
    customerUserErrors { code message }
  }
}
"""


def _call_storefront_auth(email: str, password: str, storefront_token: str, shop_url: str):
    """Returns the parsed customerAccessTokenCreate data dict, or None on network/auth error."""
    resp = requests.post(
        f"https://{shop_url}/api/2024-01/graphql.json",
        headers={
            "X-Shopify-Storefront-Access-Token": storefront_token,
            "Content-Type": "application/json",
        },
        json={"query": _STOREFRONT_AUTH_QUERY, "variables": {"input": {"email": email, "password": password}}},
        timeout=10,
    )
    if resp.status_code in (401, 403):
        return None  # token revoked — caller should refresh
    return (resp.json().get("data") or {}).get("customerAccessTokenCreate") or {}


def _clear_storefront_token(shop_url: str):
    from utils import set_config
    set_config("storefront_access_token", "", shop_url=shop_url)


def _verify_shopify_password(email: str, password: str, shop_url: str) -> bool:
    """Verifies email+password against Shopify's own auth system via Storefront API."""
    storefront_token = _get_storefront_token(shop_url)
    if not storefront_token:
        return False

    try:
        result = _call_storefront_auth(email, password, storefront_token, shop_url)

        if result is None:
            # Token was revoked — clear cache, get a fresh one, retry once
            _clear_storefront_token(shop_url)
            storefront_token = _get_storefront_token(shop_url)
            if not storefront_token:
                return False
            result = _call_storefront_auth(email, password, storefront_token, shop_url)

        return bool((result or {}).get("customerAccessToken"))
    except Exception as e:
        print(f"[Multipass] Shopify password verify error: {e}")
        return False


# ── Public service functions ───────────────────────────────────────────────────

def do_multipass_login(odoo_id: str, password: str, shop_url: str, return_to: str = "/account") -> tuple[bool, str]:
    """
    Returns (True, redirect_url) on success or (False, error_message) on failure.
    """
    if not odoo_id or not password:
        return False, "Please enter your Store ID and password."

    try:
        odoo_id_int = int(str(odoo_id).strip())
    except (ValueError, TypeError):
        return False, "Invalid Store ID format."

    cm = CustomerMap.query.filter_by(odoo_partner_id=odoo_id_int, shop_url=shop_url).first()
    if not cm:
        return False, "Invalid Store ID or password."

    if not cm.shopify_customer_id:
        return False, "Your account is not fully set up yet. Please contact admin@worthyproducts.nz."

    # Try existing Shopify password first so customers don't need to reset
    shopify_ok = _verify_shopify_password(cm.email, password, shop_url)

    if not shopify_ok:
        # Fall back to connector-managed password (post-setup flow)
        if not cm.password_hash:
            return False, "Password not set yet. Please check your email for a setup link or contact admin@worthyproducts.nz."
        if not check_password_hash(cm.password_hash, password):
            return False, "Invalid Store ID or password."

    redirect_url = generate_multipass_token(cm.email, shop_url, return_to or "/account")
    if not redirect_url:
        return False, "Login service is temporarily unavailable. Please try again shortly."

    return True, redirect_url


def set_customer_password(token: str, new_password: str) -> tuple[bool, str]:
    """
    Returns (True, success_message) or (False, error_message).
    """
    if not token or not new_password:
        return False, "Missing token or password."

    if len(new_password) < 8:
        return False, "Password must be at least 8 characters."

    cm = CustomerMap.query.filter_by(reset_token=token).first()
    if not cm:
        return False, "This link is invalid or has already been used."

    if not cm.reset_token_expires or datetime.utcnow() > cm.reset_token_expires:
        return False, "This link has expired. Please request a new one."

    cm.password_hash = generate_password_hash(new_password)
    cm.reset_token = None
    cm.reset_token_expires = None
    db.session.commit()

    return True, "Password set successfully. You can now log in with your Store ID."


def request_password_setup(identifier: str, shop_url: str, display_name: str = None) -> tuple[bool, str]:
    """
    Sends a password setup/reset email.
    `identifier` must be a numeric Store ID.
    Returns specific success/error messages — this is a closed B2B platform.
    """
    identifier = str(identifier).strip()
    if not identifier:
        return False, "Please enter your Store ID."

    not_found_msg = (
        "Sorry, that Store ID does not match any account. "
        "Please contact the Worthy Products team at admin@worthyproducts.nz "
        "or call 09 580 4110 to confirm your Store ID."
    )

    try:
        cm = CustomerMap.query.filter_by(
            odoo_partner_id=int(identifier), shop_url=shop_url
        ).first()
    except ValueError:
        return False, not_found_msg

    if not cm:
        return False, not_found_msg

    if not cm.email or "@" not in cm.email or "pos.local" in cm.email:
        return False, (
            "We found your account but no valid email address is on file. "
            "Please contact admin@worthyproducts.nz or call 09 580 4110 to update your details."
        )

    token = secrets.token_urlsafe(40)
    cm.reset_token = token
    cm.reset_token_expires = datetime.utcnow() + timedelta(days=7)
    db.session.commit()

    store_url = os.getenv("STORE_URL", f"https://{shop_url}")
    setup_link = f"{store_url}/pages/set-password?token={token}"

    if display_name is None:
        display_name = _get_customer_display_name(cm, shop_url)

    try:
        _send_setup_email(cm.email, cm.odoo_partner_id, setup_link, shop_url=shop_url, display_name=display_name)
    except Exception as e:
        print(f"[Multipass] Email send failed for odoo_id={cm.odoo_partner_id}: {e}")
        return False, (
            "We found your account but could not send the email right now. "
            "Please try again shortly or contact admin@worthyproducts.nz."
        )

    name_part = f", {display_name}" if display_name else ""
    return True, (
        f"We found your account{name_part}. "
        f"A setup email has been sent to your registered email address."
    )


def send_test_setup_email(odoo_id: int, shop_url: str, force_existing: bool = None) -> tuple[bool, str]:
    """
    Sends the mass-email version to a single Store ID for preview/testing.
    force_existing=True  → always send the existing-customer email
    force_existing=False → always send the new-customer setup email
    force_existing=None  → auto-detect from Shopify customer state
    """
    cm = CustomerMap.query.filter_by(odoo_partner_id=odoo_id, shop_url=shop_url).first()
    if not cm:
        return False, f"No customer found for Store ID {odoo_id}."
    if not cm.email or "@" not in cm.email or "pos.local" in cm.email:
        return False, "Customer has no valid email address on file."

    store_url = os.getenv("STORE_URL", f"https://{shop_url}")

    if force_existing is None:
        display_name, is_existing = _get_customer_shopify_info(cm, shop_url)
    else:
        display_name = _get_customer_display_name(cm, shop_url)
        is_existing = force_existing

    if is_existing:
        login_url = f"{store_url}/account/login"
        _send_setup_email(cm.email, cm.odoo_partner_id, login_url,
                          shop_url=shop_url, display_name=display_name, is_existing=True)
        return True, f"Sent existing-customer email to {cm.email}."
    else:
        token = secrets.token_urlsafe(40)
        cm.reset_token = token
        cm.reset_token_expires = datetime.utcnow() + timedelta(days=7)
        db.session.commit()
        setup_link = f"{store_url}/pages/set-password?token={token}"
        _send_setup_email(cm.email, cm.odoo_partner_id, setup_link,
                          shop_url=shop_url, display_name=display_name, is_existing=False)
        return True, f"Sent new-customer setup email to {cm.email}."


def send_setup_emails_to_all(shop_url: str):
    """
    Background job: sends setup emails to every CustomerMap entry without a password.
    """
    from app import app
    from utils import log_event

    with app.app_context():
        log_event("Multipass", "Info", "Bulk setup email job started.", shop_url=shop_url)

        pending = CustomerMap.query.filter_by(shop_url=shop_url, password_hash=None).all()

        if not pending:
            log_event("Multipass", "Info", "Bulk email: no customers without a password found.", shop_url=shop_url)
            return

        total = len(pending)
        sent = skip = fail = 0

        store_url = os.getenv("STORE_URL", f"https://{shop_url}")

        for customer in pending:
            if not customer.email or "@" not in customer.email or "pos.local" in customer.email:
                skip += 1
                continue
            try:
                display_name, is_existing = _get_customer_shopify_info(customer, shop_url)

                if is_existing:
                    # Customer already has a Shopify password — send an informational email only
                    login_url = f"{store_url}/account/login"
                    _send_setup_email(
                        customer.email, customer.odoo_partner_id, login_url,
                        shop_url=shop_url, display_name=display_name, is_existing=True,
                    )
                    sent += 1
                else:
                    # New customer — generate a setup token and send the setup email
                    ok, _ = request_password_setup(
                        str(customer.odoo_partner_id), shop_url, display_name=display_name
                    )
                    if ok:
                        sent += 1
                    else:
                        fail += 1
            except Exception as e:
                fail += 1
                log_event("Multipass", "Error", f"Bulk email error for {customer.odoo_partner_id}: {e}", shop_url=shop_url)

        log_event("Multipass", "Success",
            f"Bulk email done. Sent: {sent}, Skipped: {skip}, Failed: {fail} of {total}.",
            shop_url=shop_url)


# ── Customer display name lookup ──────────────────────────────────────────────

def _get_customer_shopify_info(cm, shop_url: str) -> tuple[str, bool]:
    """
    Returns (display_name, is_existing).
    is_existing = True when the Shopify customer has an active account (state='enabled'),
    meaning they already have a Shopify password and don't need to set one up.
    """
    try:
        from utils import setup_shopify_session
        import shopify
        if setup_shopify_session(shop_url) and cm.shopify_customer_id:
            c = shopify.Customer.find(int(cm.shopify_customer_id))
            if c:
                is_existing = getattr(c, "state", "") == "enabled"
                addr = getattr(c, "default_address", None)
                company = getattr(addr, "company", None) if addr else None
                if company and company.strip():
                    return company.strip(), is_existing
                first = (getattr(c, "first_name", "") or "").strip()
                last  = (getattr(c, "last_name",  "") or "").strip()
                name  = f"{first} {last}".strip()
                return name, is_existing
    except Exception as e:
        print(f"[Multipass] Could not fetch customer Shopify info: {e}")
    return "", False


def _get_customer_display_name(cm, shop_url: str) -> str:
    name, _ = _get_customer_shopify_info(cm, shop_url)
    return name


# ── Email helper ───────────────────────────────────────────────────────────────

def _send_setup_email(to_email: str, odoo_id: int, setup_link: str, shop_url: str = None, display_name: str = "", is_existing: bool = False):
    from utils import get_config
    from security_utils import decrypt_val

    db_host = get_config("smtp_host", "", shop_url=shop_url) if shop_url else ""
    db_port = get_config("smtp_port", "", shop_url=shop_url) if shop_url else ""
    db_user = get_config("smtp_user", "", shop_url=shop_url) if shop_url else ""
    db_pass_enc = get_config("smtp_pass", "", shop_url=shop_url) if shop_url else ""
    db_pass = decrypt_val(db_pass_enc) if db_pass_enc else ""

    smtp_host = db_host or os.getenv("MULTIPASS_SMTP_HOST", "premium74.web-hosting.com")
    smtp_port = int(db_port or os.getenv("MULTIPASS_SMTP_PORT", "465"))
    smtp_user = db_user or os.getenv("MULTIPASS_SMTP_USER", "admin@worthyproducts.nz")
    smtp_pass = db_pass or os.getenv("MULTIPASS_SMTP_PASSWORD") or os.getenv("SMTP_PASSWORD", "")
    shop_domain = os.getenv("STORE_DOMAIN", "worthyproducts.nz")

    name_str = display_name or "there"
    subject_name = f" - {display_name}" if display_name else ""

    if is_existing:
        subject = f"Your Worthy Products B2B Account is Ready{subject_name} (Store ID: {odoo_id})"
        action_label = "Log In Now"
        intro_text = (
            f"Hello <strong>{name_str}</strong>,<br><br>"
            f"Great news - your Worthy Products B2B wholesale account on <strong>{shop_domain}</strong> is ready to use.<br><br>"
            f"Because you already have a Worthy Products account, you can log in right now using your "
            f"<strong>Store ID</strong> and your <strong>existing password</strong>. No action is needed."
        )
        fallback_section = (
            '<p style="margin:0 0 8px; font-size:13px; color:#888; text-align:center;">'
            "Button not working? Go to:</p>"
            f'<p style="margin:0 0 28px; font-size:12px; color:#2457c2; word-break:break-all; text-align:center;">'
            f"{setup_link}</p>"
        )
        expiry_section = ""
        plain = f"""\
Hi {name_str},

Great news - your Worthy Products B2B wholesale account is ready to use.

Because you already have a Worthy Products account, you can log in right now using your Store ID and your existing password. No action is needed.

Your Store ID: {odoo_id}

Please keep this number safe as you will need it each time you log in.

Log in here: {setup_link}

Need help? Contact our team:
  Email: admin@worthyproducts.nz
  Phone: 09 580 4110

Worthy Products Team
"""
    else:
        subject = f"Set Up Your Worthy Products Password{subject_name} (Store ID: {odoo_id})"
        action_label = "Set Up Your Password"
        intro_text = (
            f"Hello <strong>{name_str}</strong>,<br><br>"
            f"Your Worthy Products B2B wholesale account on <strong>{shop_domain}</strong> is ready to use.<br><br>"
            f"To get started, please create your password using the button below. "
            f"You will need your <strong>Store ID</strong> every time you log in - please keep it safe."
        )
        fallback_section = (
            '<p style="margin:0 0 8px; font-size:13px; color:#888; text-align:center;">'
            "Button not working? Copy and paste this link into your browser:</p>"
            f'<p style="margin:0 0 28px; font-size:12px; color:#2457c2; word-break:break-all; text-align:center;">'
            f"{setup_link}</p>"
        )
        expiry_section = (
            '<table width="100%" cellpadding="0" cellspacing="0"><tr>'
            '<td style="background-color:#fff8e1; border:1px solid #ffe082; border-radius:6px; padding:14px 18px;">'
            '<p style="margin:0; font-size:13px; color:#7d6608;">'
            "<strong>This link expires in 7 days.</strong> "
            "If it expires, return to the login page and request a new one."
            "</p></td></tr></table>"
        )
        plain = f"""\
Hi {name_str},

Your Worthy Products B2B wholesale account is ready to use.

Your Store ID: {odoo_id}

Please keep this number safe as you will need it each time you log in.

To set your password, visit the link below. This link is valid for 7 days.

{setup_link}

If you did not request this, you can safely ignore this email.

Need help? Contact our team:
  Email: admin@worthyproducts.nz
  Phone: 09 580 4110

Worthy Products Team
"""

    html = None
    try:
        template_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "templates", "email_password_setup.html"
        )
        with open(template_path, "r", encoding="utf-8") as f:
            html = f.read().format(
                action_label=action_label,
                intro_text=intro_text,
                fallback_section=fallback_section,
                expiry_section=expiry_section,
                shop_domain=shop_domain,
                store_name=display_name or "your store",
                odoo_id=odoo_id,
                to_email=to_email,
                setup_url=setup_link,
                customer_name=display_name or "there",
            )
    except Exception as e:
        print(f"[Multipass] HTML template load failed, falling back to plain text: {e}")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"Worthy Products <{smtp_user}>"
    msg["To"] = to_email
    msg.set_content(plain)
    if html:
        msg.add_alternative(html, subtype="html")

    # Port 465 = SMTP_SSL, Port 587 = STARTTLS
    if smtp_port == 465:
        with smtplib.SMTP_SSL(smtp_host, smtp_port) as smtp:
            smtp.login(smtp_user, smtp_pass)
            smtp.send_message(msg)
    else:
        with smtplib.SMTP(smtp_host, smtp_port) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(smtp_user, smtp_pass)
            smtp.send_message(msg)
