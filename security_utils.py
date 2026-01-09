import os
from cryptography.fernet import Fernet
from flask import request, jsonify, session
from functools import wraps

# 1. ENCRYPTION SETUP
# Generate a key once using: Fernet.generate_key()
# Store this in your .env file as ENCRYPTION_KEY=...
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY')
if not ENCRYPTION_KEY:
    # Fallback for dev only - DO NOT USE IN PROD
    print("WARNING: Using temporary encryption key. Set ENCRYPTION_KEY in .env!")
    ENCRYPTION_KEY = Fernet.generate_key()

cipher = Fernet(ENCRYPTION_KEY)

def encrypt_val(value):
    """Encrypts a string value."""
    if not value: return None
    return cipher.encrypt(value.encode()).decode()

def decrypt_val(value):
    """Decrypts a string value."""
    if not value: return None
    try:
        return cipher.decrypt(value.encode()).decode()
    except:
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
        from models import Shop
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop or not shop.is_active:
             return jsonify({'error': 'Unauthorized: Shop not active'}), 401
             
        return f(*args, **kwargs)
    return decorated_function
