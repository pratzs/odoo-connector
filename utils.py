import os
import json
import redis
import shopify
from datetime import datetime
from flask import request
from rq import Queue
from models import db, AppSetting, SyncLog, Shop
from odoo_client import OdooClient
from contextlib import contextmanager

# 1. SETUP REDIS
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
conn = redis.from_url(redis_url)
q = Queue(connection=conn)

# 2. CONFIG HELPERS
def get_config(key, default=None, shop_url=None):
    if not shop_url:
        try:
            shop_url = request.args.get('shop')
        except:
            pass 
    if not shop_url: return default

    try:
        setting = AppSetting.query.filter_by(shop_url=shop_url, key=key).first()
        if not setting: return default
        try: return json.loads(setting.value)
        except: return setting.value
    except: return default

def set_config(key, value, shop_url=None):
    if not shop_url:
        try:
            shop_url = request.args.get('shop')
        except:
            pass 

    if not shop_url: 
        print(f"Error: set_config failed for '{key}' - No Shop URL provided.")
        return False

    try:
        setting = AppSetting.query.filter_by(shop_url=shop_url, key=key).first()
        if not setting:
            setting = AppSetting(shop_url=shop_url, key=key)
            db.session.add(setting)
        
        if isinstance(value, (list, dict)):
            setting.value = json.dumps(value)
        else:
            if isinstance(value, bool):
                setting.value = str(value).lower() 
            else:
                setting.value = str(value)
            
        db.session.commit()
        return True

    except Exception as e:
        print(f"Config Save Error ({key}): {e}")
        db.session.rollback()
        return False

# 3. LOGGING HELPER
def log_event(entity, status, message, shop_url=None):
    if not shop_url:
        try:
            shop_url = request.args.get('shop') or request.headers.get('X-Shopify-Shop-Domain')
        except:
            pass
    if not shop_url:
        shop_url = 'System'

    try:
        log = SyncLog(shop_url=shop_url, entity=entity, status=status, message=message, timestamp=datetime.utcnow())
        db.session.add(log)
        db.session.commit()
    except Exception as e: 
        print(f"DB LOG ERROR: {e}")
        db.session.rollback()

# 4. REDIS LOCK HELPER
@contextmanager
def acquire_distributed_lock(lock_name, timeout=20):
    lock = conn.lock(lock_name, timeout=timeout, blocking_timeout=1)
    acquired = False
    try:
        acquired = lock.acquire(blocking=True)
        yield acquired
    finally:
        if acquired:
            try:
                lock.release()
            except redis.exceptions.LockError:
                pass

# 5. ODOO CONNECTION HELPER (Moved from app.py)
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
        log_event('System', 'Error', f"Connection failed for {shop_url}: {e}", shop_url=shop_url)
        return None

# 6. SHOPIFY SESSION HELPER (Moved from app.py)
def setup_shopify_session(shop_url=None):
    """
    Activates a Shopify session for a specific shop from the Database.
    """
    if not shop_url:
        try:
            shop_url = request.args.get('shop')
        except:
            return False

    if not shop_url: return False

    try:
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop: return False
        
        # Get Version from Env or Default
        api_version = os.getenv('SHOPIFY_API_VERSION', '2025-10')
        
        session = shopify.Session(shop.shop_url, api_version, shop.access_token)
        shopify.ShopifyResource.activate_session(session)
        return True
    except Exception as e:
        print(f"Shopify Session Error: {e}")
        return False

# 7. Automated Webhook Registrations

def automate_webhook_registration(shop_url):
    """
    Called automatically during Auth or Settings save.
    Registers all required webhooks without user intervention.
    """
    if not setup_shopify_session(shop_url):
        return False

    app_host = os.getenv('HOST')
    target_address = f"{app_host}/webhooks/shopify"
    
    required_topics = [
        'orders/create', 'orders/updated', 'orders/cancelled',
        'products/create', 'refunds/create', 'inventory_levels/update'
    ]

    try:
        existing_hooks = shopify.Webhook.find()
        for topic in required_topics:
            # Check if already registered
            match = next((h for h in existing_hooks if h.topic == topic and h.address == target_address), None)
            if not match:
                new_hook = shopify.Webhook()
                new_hook.topic = topic
                new_hook.address = target_address
                new_hook.format = 'json'
                new_hook.save()
        return True
    except Exception as e:
        print(f"Auto-Webhook Error for {shop_url}: {e}")
        return False

# 8. Setup Shopify Sessions
def setup_shopify_session(shop_url):
    """
    Utility: Loads a shop's access token and activates the Shopify session.
    Essential for Multi-tenant operations.
    """
    try:
        # 1. Look up the shop in your Supabase database
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        
        if not shop or not shop.access_token:
            print(f"❌ Session Setup Failed: {shop_url} not found in DB.")
            return False
        
        # 2. Create the session object
        # Note: SHOPIFY_API_VERSION should be something like '2024-01' or '2025-01'
        session = shopify.Session(shop.shop_url, SHOPIFY_API_VERSION, shop.access_token)
        
        # 3. Tell the library to use this session for all subsequent calls
        shopify.ShopifyResource.activate_session(session)
        return True
        
    except Exception as e:
        print(f"❌ Critical Session Error for {shop_url}: {e}")
        return False
