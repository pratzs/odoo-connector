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
