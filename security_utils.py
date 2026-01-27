import os
import sys
from cryptography.fernet import Fernet
from flask import request, jsonify, session
from functools import wraps

# 1. ENCRYPTION SETUP
# STRICT MODE: We crash if the key is missing to prevent data loss.
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY')

if not ENCRYPTION_KEY:
    # CRITICAL SECURITY CHECK
    # We stop the app startup immediately.
    # This prevents the app from running with a random key that would verify nothing.
    print("CRITICAL ERROR: 'ENCRYPTION_KEY' environment variable is missing!")
    print("The app cannot start because it needs this key to decrypt credentials.")
    sys.exit(1)

try:
    cipher = Fernet(ENCRYPTION_KEY)
except Exception as e:
    print(f"CRITICAL ERROR: Invalid ENCRYPTION_KEY format. It must be a valid Fernet key (32 url-safe base64-encoded bytes). Error: {e}")
    sys.exit(1)

def encrypt_val(value):
    """Encrypts a string value."""
    if not value: return None
    try:
        return cipher.encrypt(value.encode()).decode()
    except Exception as e:
        print(f"Encryption Error: {e}")
        return None

def decrypt_val(value):
    """Decrypts a string value."""
    if not value: return None
    try:
        return cipher.decrypt(value.encode()).decode()
    except Exception as e:
        # This usually happens if the key changed or data is corrupt
        print(f"Decryption Error: {e}")
        return None
        

def get_odoo_connection(shop_url):
    """
    Factory Function: Creates a dynamic Odoo connection for a specific shop.
    """
    try:
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop:
            print(f"Error: No credentials found for {shop_url}")
            return None
        
        # Create a fresh client using the DB credentials
        client = OdooClient(
            url=shop.odoo_url,
            db=shop.odoo_db,
            username=shop.odoo_username,
            password=shop.odoo_password
        )
        return client

    except Exception as e:
        # --- NEW: DETECT AUTH FAILURES ---
        error_str = str(e).lower()
        if "access denied" in error_str or "authentication failed" in error_str:
            log_event('System', 'Error', f"🛑 AUTH FAILED: Odoo credentials invalid. Please update settings.", shop_url=shop_url)
            
            # Optional: Mark shop as inactive to stop hammering the server
            # shop = Shop.query.filter_by(shop_url=shop_url).first()
            # shop.is_active = False 
            # db.session.commit()
        else:
            log_event('System', 'Error', f"Connection failed for {shop_url}: {e}", shop_url=shop_url)
        return None
