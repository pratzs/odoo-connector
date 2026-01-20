# utils.py
import os
import json
import redis
from datetime import datetime
from flask import request
from rq import Queue
from models import db, AppSetting, SyncLog
from contextlib import contextmanager

# 1. SETUP REDIS
redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
conn = redis.from_url(redis_url)
q = Queue(connection=conn)

# 2. CONFIG HELPER
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
