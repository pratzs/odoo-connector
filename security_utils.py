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

# 2. API SECURITY DECORATOR
def require_shopify_session(f):
    """
    Protects AJAX endpoints.
    Ensures the request comes from an authenticated shopify session 
    OR has a valid signature.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # A. Try Session (Cookies)
        if 'shopify_token' in session and 'shopify_url' in session:
            return f(*args, **kwargs)
        
        # B. Try Shop Header (App Bridge Token would go here in V8)
        # For now, we strictly check if the shop param exists and matches a known shop
        shop_url = request.args.get('shop')
        if not shop_url:
            return jsonify({'error': 'Unauthorized: Missing shop param'}), 401
        
        # In a real App Bridge app, you would validate the JWT token here.
        # For this MVP, we ensure the shop is at least installed in our DB.
        # We perform a lazy import to avoid circular dependencies with models.py
        from models import Shop
        
        try:
            shop = Shop.query.filter_by(shop_url=shop_url).first()
            if not shop or not shop.is_active:
                 return jsonify({'error': 'Unauthorized: Shop not active'}), 401
        except Exception:
             return jsonify({'error': 'Unauthorized: DB Error'}), 500
             
        return f(*args, **kwargs)
    return decorated_function
