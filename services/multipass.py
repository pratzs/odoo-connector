# services/multipass.py
"""
Multipass Login Service
=======================
Handles B2B customer login via Odoo ID + password.

Flow:
  1. Customer visits /pages/b2b-login on the Shopify store
  2. Enters Odoo ID (e.g. 35819) + their self-set password
  3. POST to /multipass/login on this connector app
  4. We validate credentials, generate a Shopify Multipass token
  5. Redirect customer into Shopify — authenticated, catalog loaded

Password Setup (first time):
  - Store owner triggers "Send Setup Email" from admin panel
     OR customer clicks "Set up my password" on the login page
  - We find the CustomerMap by Odoo ID, email a one-time link
  - Customer clicks link → POST new password → hash stored

Environment variables required:
  SHOPIFY_MULTIPASS_SECRET  — from Shopify Admin > Settings > Customer accounts
  SMTP_PASSWORD             — already used for inventory alerts
  HOST                      — already set (your Render URL)
"""

import os
import json
import hmac
import hashlib
import base64
import secrets
import smtplib

from datetime import datetime, timedelta
from email.message import EmailMessage

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from werkzeug.security import generate_password_hash, check_password_hash

from models import db, CustomerMap, Shop
from utils import log_event


# ─────────────────────────────────────────────
# 1. MULTIPASS TOKEN GENERATOR
# ─────────────────────────────────────────────

def generate_multipass_token(email: str, shop_url: str, return_to: str = '/account') -> str | None:
    """
    Generates a Shopify Multipass token for the given email.
    Returns the full login URL ready to redirect to.

    Shopify algorithm:
      key_material  = SHA256(MULTIPASS_SECRET)
      enc_key       = key_material[0:16]   (AES-128)
      sig_key       = key_material[16:32]  (HMAC-SHA256)
      iv            = random 16 bytes
      payload       = AES-128-CBC(enc_key, iv, PKCS7(customer_json))
      token         = base64url(iv + payload + HMAC-SHA256(sig_key, iv+payload))
    """
    secret = os.getenv('SHOPIFY_MULTIPASS_SECRET')
    if not secret:
        print("MULTIPASS ERROR: SHOPIFY_MULTIPASS_SECRET not set in environment")
        return None

    try:
        # 1. Derive keys
        key_material = hashlib.sha256(secret.encode('utf-8')).digest()
        enc_key = key_material[:16]
        sig_key = key_material[16:]

        # 2. Build customer payload
        customer_data = {
            'email': email,
            'created_at': datetime.utcnow().isoformat(),
            'return_to': return_to,
        }
        payload_bytes = json.dumps(customer_data).encode('utf-8')

        # 3. PKCS7 pad to 16-byte boundary
        pad_len = 16 - (len(payload_bytes) % 16)
        payload_bytes += bytes([pad_len] * pad_len)

        # 4. AES-128-CBC encrypt
        iv = os.urandom(16)
        cipher = Cipher(algorithms.AES(enc_key), modes.CBC(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(payload_bytes) + encryptor.finalize()
        encrypted = iv + ciphertext

        # 5. HMAC-SHA256 sign
        signature = hmac.new(sig_key, encrypted, hashlib.sha256).digest()

        # 6. Build token + URL
        token = base64.urlsafe_b64encode(encrypted + signature).decode('utf-8')
        clean_shop = shop_url.replace('https://', '').replace('http://', '').rstrip('/')
        return f"https://{clean_shop}/account/login/multipass/{token}"

    except Exception as e:
        print(f"Multipass token generation error: {e}")
        return None


# ─────────────────────────────────────────────
# 2. LOGIN — Odoo ID + Password → Multipass URL
# ─────────────────────────────────────────────

def do_multipass_login(odoo_id: str, password: str, shop_url: str) -> tuple[bool, str]:
    """
    Validates Odoo ID + password.
    Returns (True, multipass_url) on success or (False, error_message) on failure.
    """
    if not odoo_id or not password:
        return False, "Please enter your Store ID and password."

    try:
        odoo_id_int = int(odoo_id.strip())
    except ValueError:
        return False, "Invalid Store ID format."

    # 1. Look up customer
    customer = CustomerMap.query.filter_by(
        odoo_partner_id=odoo_id_int,
        shop_url=shop_url
    ).first()

    if not customer:
        # Generic message — don't reveal whether ID exists or not
        return False, "Invalid Store ID or password."

    # 2. Check password is set
    if not customer.password_hash:
        return False, "Password not set up yet. Please check your email for a setup link, or contact your account manager."

    # 3. Verify password
    if not check_password_hash(customer.password_hash, password):
        log_event('Multipass', 'Warning', f"Failed login attempt for Odoo ID {odoo_id_int}", shop_url=shop_url)
        return False, "Invalid Store ID or password."

    # 4. Generate Multipass token
    multipass_url = generate_multipass_token(customer.email, shop_url)
    if not multipass_url:
        return False, "Login service temporarily unavailable. Please try again shortly."

    log_event('Multipass', 'Success', f"Login successful for Odoo ID {odoo_id_int}", shop_url=shop_url)
    return True, multipass_url


# ─────────────────────────────────────────────
# 3. SET PASSWORD (first time or change)
# ─────────────────────────────────────────────

def set_customer_password(token: str, new_password: str) -> tuple[bool, str]:
    """
    Called when a customer clicks their setup/reset email link and sets a new password.
    Token is a one-time UUID stored in CustomerMap.reset_token.
    """
    if not token or not new_password:
        return False, "Missing token or password."

    if len(new_password) < 8:
        return False, "Password must be at least 8 characters."

    # 1. Find the customer by reset token
    customer = CustomerMap.query.filter_by(reset_token=token).first()

    if not customer:
        return False, "This link is invalid or has already been used."

    # 2. Check expiry (tokens valid for 48 hours)
    if customer.reset_token_expires and datetime.utcnow() > customer.reset_token_expires:
        return False, "This link has expired. Please request a new one."

    # 3. Hash and save the new password
    customer.password_hash = generate_password_hash(new_password)
    customer.reset_token = None           # Invalidate the token immediately
    customer.reset_token_expires = None
    db.session.commit()

    log_event('Multipass', 'Success',
        f"Password set for Odoo ID {customer.odoo_partner_id}",
        shop_url=customer.shop_url)

    return True, "Password set successfully. You can now log in."


# ─────────────────────────────────────────────
# 4. REQUEST PASSWORD SETUP / RESET EMAIL
# ─────────────────────────────────────────────

def request_password_setup(identifier: str, shop_url: str) -> tuple[bool, str]:
    """
    Sends a setup/reset email to the customer.
    `identifier` can be either:
      - An Odoo ID (numeric string, e.g. "31274")
      - An email address (e.g. "pratham@worthy.nz")

    The email sent includes:
      - Their Store ID (so managers always have it)
      - A one-time password setup link via the STORE domain (not the connector)
        e.g. https://vjtrading.myshopify.com/pages/set-password?token=xxx
    """
    identifier = str(identifier).strip()
    if not identifier:
        return False, "Please enter your email address or Store ID."

    # 1. Look up customer by email OR odoo_id
    customer = None

    if '@' in identifier:
        customer = CustomerMap.query.filter_by(
            email=identifier.lower(),
            shop_url=shop_url
        ).first()
    else:
        try:
            odoo_id_int = int(identifier)
            customer = CustomerMap.query.filter_by(
                odoo_partner_id=odoo_id_int,
                shop_url=shop_url
            ).first()
        except ValueError:
            return False, "Please enter a valid email address or Store ID."

    neutral_msg = "If we found a matching account, a setup email has been sent."

    if not customer or not customer.email or '@' not in customer.email:
        return True, neutral_msg

    if 'pos.local' in customer.email:
        return False, "No email address is linked to this account. Please contact your account manager."

    # 2. Generate a secure one-time token
    token = secrets.token_urlsafe(32)
    customer.reset_token = token
    customer.reset_token_expires = datetime.utcnow() + timedelta(hours=48)
    db.session.commit()

    # 3. Build the setup URL — goes through the STORE domain, not the connector
    # The store has a /pages/set-password page that reads ?token= from the URL
    clean_shop = shop_url.replace('https://', '').replace('http://', '').rstrip('/')
    setup_url = f"https://{clean_shop}/pages/set-password?token={token}"

    action_label = "Set Up Your Password" if not customer.password_hash else "Reset Your Password"

    # 4. Get per-shop from-email from AppSettings (falls back to developer email)
    from_email = _get_shop_from_email(shop_url)

    success = _send_setup_email(
        to_email=customer.email,
        odoo_id=customer.odoo_partner_id,
        setup_url=setup_url,
        action_label=action_label,
        shop_domain=clean_shop,
        from_email=from_email
    )

    if success:
        log_event('Multipass', 'Info',
            f"Password setup email sent to {customer.email} (Odoo ID {customer.odoo_partner_id})",
            shop_url=shop_url)
        return True, neutral_msg
    else:
        log_event('Multipass', 'Error',
            f"Failed to send setup email to {customer.email} (Odoo ID {customer.odoo_partner_id})",
            shop_url=shop_url)
        return False, "Failed to send email. Please contact your account manager."


def _get_shop_from_email(shop_url: str) -> str:
    """
    Returns the configured from-email for this shop.
    Store owners set this in Settings as 'multipass_from_email'.
    Falls back to the developer email if not configured.
    """
    try:
        from utils import get_config
        configured = get_config('multipass_from_email', None, shop_url=shop_url)
        if configured and '@' in str(configured):
            return str(configured).strip()
    except Exception:
        pass
    # Fallback — developer email
    return "hello@tripsterdevelopers.com"


def _send_setup_email(to_email, odoo_id, setup_url, action_label, shop_domain, from_email="hello@tripsterdevelopers.com"):
    from utils import get_config

    # Try shop's own SMTP first
    shop_url = f"{shop_domain}"  # already cleaned
    smtp_host = None
    try:
        # Look up shop_url from shop_domain
        from models import Shop
        shop = Shop.query.filter(Shop.shop_url.contains(shop_domain.replace('.myshopify.com',''))).first()
        if shop:
            from utils import get_config as gc
            smtp_host = gc('smtp_host', None, shop_url=shop.shop_url)
            smtp_port = int(gc('smtp_port', 465, shop_url=shop.shop_url))
            smtp_user = gc('smtp_user', None, shop_url=shop.shop_url)
            smtp_pass_enc = gc('smtp_pass', None, shop_url=shop.shop_url)
            from security_utils import decrypt_val
            smtp_pass = decrypt_val(smtp_pass_enc) if smtp_pass_enc else None
    except Exception as e:
        print(f"SMTP config lookup error: {e}")
        smtp_host = None

    # Use shop SMTP if configured, else fall back to developer SMTP
    if smtp_host and smtp_user and smtp_pass:
        SMTP_SERVER   = smtp_host
        SMTP_PORT     = smtp_port
        SENDER_EMAIL  = smtp_user
        SENDER_PASSWORD = smtp_pass
        display_from  = smtp_user  # Sends genuinely FROM their address
    else:
        SMTP_SERVER   = "premium74.web-hosting.com"
        SMTP_PORT     = 465
        SENDER_EMAIL  = "hello@tripsterdevelopers.com"
        SENDER_PASSWORD = os.getenv('SMTP_PASSWORD')
        display_from  = from_email  # Reply-To only

    if not SENDER_PASSWORD:
        print("MULTIPASS EMAIL ERROR: No SMTP password available")
        return False

    subject = f"{action_label} — Your B2B Account Details"
    body = f"""Hello,

You (or your account manager) requested a password {"setup" if "Set Up" in action_label else "reset"} for your B2B ordering account.

━━━━━━━━━━━━━━━━━━━━━━━━━━
YOUR ACCOUNT DETAILS
━━━━━━━━━━━━━━━━━━━━━━━━━━
Store ID : {odoo_id}
Email    : {to_email}
━━━━━━━━━━━━━━━━━━━━━━━━━━

Keep your Store ID handy — you will need it every time you log in.

To {action_label.lower()}, click the link below:

{setup_url}

This link expires in 48 hours.

If you did not request this, you can safely ignore this email.
"""

    try:
        msg = EmailMessage()
        msg.set_content(body)
        msg['Subject'] = subject
        msg['From']    = display_from
        msg['To']      = to_email
        if display_from != SENDER_EMAIL:
            msg['Reply-To'] = display_from

        if SMTP_PORT == 587:
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as smtp:
                smtp.starttls()
                smtp.login(SENDER_EMAIL, SENDER_PASSWORD)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as smtp:
                smtp.login(SENDER_EMAIL, SENDER_PASSWORD)
                smtp.send_message(msg)
        return True
    except Exception as e:
        print(f"Multipass email send error: {e}")
        return False
