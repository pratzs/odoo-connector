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

# 1. SETUP REDIS & QUEUES
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
conn = redis.from_url(redis_url)

# q_default = Slow Lane (Products, Images, heavy stuff) -> Timeout: 2 Hours
q_default = Queue('default', connection=conn, default_timeout=7200)

# q_critical = Fast Lane (Inventory, Orders) -> Timeout: 10 Minutes
q_critical = Queue('critical', connection=conn, default_timeout=600)

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

# 3. LOGGING HELPER (Context-Aware)
def log_event(entity, status, message, shop_url=None):
    if not shop_url:
        try:
            shop_url = request.args.get('shop') or request.headers.get('X-Shopify-Shop-Domain')
        except:
            pass
    if not shop_url:
        shop_url = 'System'

    # Inner function to perform the DB write
    def write_log():
        try:
            log = SyncLog(shop_url=shop_url, entity=entity, status=status, message=message, timestamp=datetime.utcnow())
            db.session.add(log)
            db.session.commit()
        except Exception as e:
            print(f"DB LOG ERROR: {e}")
            db.session.rollback()

    # Check if we are inside an application context
    from flask import current_app
    if current_app:
        write_log()
    else:
        # If no context (e.g., inside RQ worker), create one manually
        from app import app
        with app.app_context():
            write_log()

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

# --- 6. SHOPIFY SESSION HELPER ---
def setup_shopify_session(shop_url=None):
    """
    Activates a Shopify session for a specific shop from the Database.
    Clears any existing global session to prevent 401/403 leakage.
    """
    if not shop_url:
        try:
            shop_url = request.args.get('shop')
        except:
            return False

    if not shop_url: return False

    try:
        # 1. Clear any old session from library memory
        shopify.ShopifyResource.clear_session()

        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop or not shop.access_token: 
            print(f"❌ DB Lookup Failed for {shop_url}")
            return False
        
        # 2. Use the version from app.py or fallback to stable
        # Note: Ensure SHOPIFY_API_VERSION is imported or defined
        api_version = os.getenv('SHOPIFY_API_VERSION', '2025-01')
        
        session = shopify.Session(shop.shop_url, api_version, shop.access_token)
        shopify.ShopifyResource.activate_session(session)
        
        # 3. Optional: Quick smoke test to verify token
        # shopify.Shop.current() 
        
        return True
    except Exception as e:
        print(f"Shopify Session Error for {shop_url}: {e}")
        return False

# 7. Automated Webhook Registrations

def automate_webhook_registration(shop_url):
    """
    Called automatically during Auth or Settings save.
    Registers all required webhooks, including the uninstall hook.
    """
    if not setup_shopify_session(shop_url):
        return False

    app_host = os.getenv('HOST')
    
    # Define the general receiver and the specific uninstall receiver
    general_address = f"{app_host}/webhooks/shopify"
    uninstall_address = f"{app_host}/webhooks/app_uninstalled"
    
    # Topic -> Target mapping
    webhook_targets = {
    'orders/create': general_address,
    'orders/updated': general_address,
    'orders/cancelled': general_address,
    'products/create': general_address,
    'refunds/create': general_address,
    'inventory_levels/update': general_address,
    'app/uninstalled': uninstall_address
}

    try:
        existing_hooks = shopify.Webhook.find()
        
        for topic, target in webhook_targets.items():
            # Check if this topic is already registered to THIS specific address
            match = next((h for h in existing_hooks if h.topic == topic and h.address == target), None)
            
            if not match:
                new_hook = shopify.Webhook()
                new_hook.topic = topic
                new_hook.address = target
                new_hook.format = 'json'
                new_hook.save()
                print(f"✅ Registered {topic} for {shop_url}")
                
        return True
    except Exception as e:
        print(f"Auto-Webhook Error for {shop_url}: {e}")
        return False


#8. We need to catch xmlrpc.client.ProtocolError
def get_odoo_connection(shop_url):
    try:
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop:
            print(f"Error: No credentials found for {shop_url}")
            return None
        
        client = OdooClient(
            url=shop.odoo_url,
            db=shop.odoo_db,
            username=shop.odoo_username,
            password=shop.odoo_password
        )
        return client

    except Exception as e:
        # --- IMPROVED ERROR HANDLING ---
        error_str = str(e).lower()
        if "access denied" in error_str or "authentication failed" in error_str:
            log_event('System', 'Error', f"🛑 AUTH FAILED: Check Odoo password/username.", shop_url=shop_url)
        else:
            log_event('System', 'Error', f"Connection failed: {e}", shop_url=shop_url)
        return None
