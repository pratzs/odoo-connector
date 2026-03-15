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

def request_password_setup(odoo_id: str, shop_url: str) -> tuple[bool, str]:
    """
    Sends a setup/reset email to the customer.
    Called either by the store owner (admin panel) or by the customer
    clicking "Forgot / Set up password" on the login page.
    """
    try:
        odoo_id_int = int(str(odoo_id).strip())
    except ValueError:
        return False, "Invalid Store ID."

    customer = CustomerMap.query.filter_by(
        odoo_partner_id=odoo_id_int,
        shop_url=shop_url
    ).first()

    if not customer or not customer.email:
        # Don't reveal whether the ID exists — security through obscurity
        return True, "If that Store ID exists, a setup email has been sent."

    # 1. Generate a secure one-time token
    token = secrets.token_urlsafe(32)
    customer.reset_token = token
    customer.reset_token_expires = datetime.utcnow() + timedelta(hours=48)
    db.session.commit()

    # 2. Build the setup URL
    app_host = os.getenv('HOST', '').rstrip('/')
    setup_url = f"{app_host}/multipass/reset/{token}"

    # 3. Determine label (first time vs reset)
    action_label = "Set Up Your Password" if not customer.password_hash else "Reset Your Password"
    clean_shop = shop_url.replace('https://', '').replace('http://', '').rstrip('/')

    # 4. Send email
    success = _send_setup_email(
        to_email=customer.email,
        odoo_id=odoo_id_int,
        setup_url=setup_url,
        action_label=action_label,
        shop_domain=clean_shop
    )

    if success:
        log_event('Multipass', 'Info',
            f"Password setup email sent to Odoo ID {odoo_id_int}",
            shop_url=shop_url)
        return True, "If that Store ID exists, a setup email has been sent."
    else:
        return False, "Failed to send email. Please contact your account manager."


def _send_setup_email(to_email: str, odoo_id: int, setup_url: str,
                      action_label: str, shop_domain: str) -> bool:
    """Internal helper — sends the password setup/reset email via Namecheap SMTP."""
    SMTP_SERVER   = "premium74.web-hosting.com"
    SMTP_PORT     = 465
    SENDER_EMAIL  = "hello@tripsterdevelopers.com"
    SENDER_PASSWORD = os.getenv('SMTP_PASSWORD')

    if not SENDER_PASSWORD:
        print("MULTIPASS EMAIL ERROR: SMTP_PASSWORD not set")
        return False

    subject = f"[{shop_domain}] {action_label} — Store ID {odoo_id}"

    body = f"""Hello,

You (or your account manager) requested a password {"setup" if "Set Up" in action_label else "reset"} for your B2B account.

Your Store ID: {odoo_id}

Click the link below to {action_label.lower()}:

{setup_url}

This link expires in 48 hours. If you did not request this, please ignore this email.

— {shop_domain} Support
"""

    try:
        msg = EmailMessage()
        msg.set_content(body)
        msg['Subject'] = subject
        msg['From'] = SENDER_EMAIL
        msg['To'] = to_email

        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as smtp:
            smtp.login(SENDER_EMAIL, SENDER_PASSWORD)
            smtp.send_message(msg)
        return True
    except Exception as e:
        print(f"Multipass email send error: {e}")
        return False


# ─────────────────────────────────────────────
# 5. ADMIN: BULK SEND SETUP EMAILS
# ─────────────────────────────────────────────

def send_setup_emails_to_all(shop_url: str) -> tuple[int, int]:
    """
    Sends setup emails to ALL customers in CustomerMap for this shop
    who do NOT yet have a password set. Returns (sent, skipped).
    Used from the admin dashboard "Send All Setup Emails" button.
    """
    customers = CustomerMap.query.filter_by(
        shop_url=shop_url,
        password_hash=None
    ).filter(CustomerMap.email.isnot(None)).all()

    sent = 0
    skipped = 0

    for c in customers:
        if not c.email or '@' not in c.email or 'pos.local' in c.email:
            skipped += 1
            continue

        ok, _ = request_password_setup(str(c.odoo_partner_id), shop_url)
        if ok:
            sent += 1
        else:
            skipped += 1

    log_event('Multipass', 'Info',
        f"Bulk setup emails: {sent} sent, {skipped} skipped (no email/already set)",
        shop_url=shop_url)

    return sent, skipped
