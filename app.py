import os
import logging
import socket
import hmac
import hashlib
import base64
import json
import schedule
import time
import shopify
import concurrent.futures
import requests
import random
import xmlrpc.client
import ssl
import gc
import smtplib
from urllib.parse import urlparse, parse_qs

from flask import Flask, request, jsonify, render_template, session, url_for, render_template_string, redirect
from datetime import datetime, timedelta
from functools import wraps
from sqlalchemy import text
from email.message import EmailMessage
from rq import Retry

# --- CUSTOM MODULES ---
from models import db, ProductMap, SyncLog, AppSetting, CustomerMap, ProcessedOrder, Shop
from odoo_client import OdooClient
# from security_utils import require_shopify_session 

# --- UTILS (Merged & Clean) ---
from utils import (
    conn, 
    q_default,
    q_critical,
    get_config, 
    set_config, 
    log_event, 
    acquire_distributed_lock,
    get_odoo_connection, 
    setup_shopify_session, 
    automate_webhook_registration
)

# --- SERVICES ---
from services.orders import process_order_data
from services.products import (
    sync_products_master, 
    process_product_data, 
    archive_shopify_duplicates, 
    cleanup_shopify_products,
    find_shopify_product_by_sku
)
from services.customers import sync_customers_master
from services.refunds import process_refund_data
from services.returns import sync_odoo_returns

try:
    from services.cancellations import process_cancellation, sync_odoo_cancellations
    from services.fulfillments import sync_odoo_fulfillments
except ImportError:
    print("⚠️ Warning: Cancellation or Fulfillment services missing. Skipping import.")

socket.setdefaulttimeout(60)
        
# --- DECORATOR: ENSURE SESSION IS VALID ---
def require_shopify_session(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 1. Try URL parameters (standard)
        shop_url = request.args.get('shop')
        
        # 2. Try Form Data (for the HTML Connect Form)
        if not shop_url:
            shop_url = request.form.get('shop') or request.form.get('shop_url')
            
        # 3. Try JSON Body (for Dashboard AJAX calls)
        if not shop_url and request.is_json:
            try:
                data = request.get_json(silent=True)
                if data:
                    shop_url = data.get('shop') or data.get('shop_url')
            except: pass

        # 🛑 BLOCK IF STILL MISSING
        if not shop_url:
            print("❌ Security Block: Request missing 'shop' parameter.")
            return jsonify({"error": "Missing shop parameter"}), 400
            
        # 4. Activate Session
        if not setup_shopify_session(shop_url):
            return jsonify({"error": "Invalid or expired Shopify Session"}), 401
            
        return f(*args, **kwargs)
    return decorated_function

# --- PUBLIC APP CONFIG ---
SHOPIFY_API_KEY = os.getenv('SHOPIFY_API_KEY')
SHOPIFY_API_SECRET = os.getenv('SHOPIFY_API_SECRET')
APP_URL = os.getenv('HOST')
SHOPIFY_API_VERSION = os.getenv('SHOPIFY_API_VERSION', '2025-10')

SCOPES = (
    "read_products,write_products,"
    "read_product_listings,write_product_listings,"
    "read_customers,write_customers,"
    "read_orders,write_orders,read_all_orders,"
    "read_draft_orders,write_draft_orders,"
    "read_inventory,write_inventory,"
    "read_locations,write_locations,"
    "read_shipping,write_shipping,"
    "read_assigned_fulfillment_orders,write_assigned_fulfillment_orders,"
    "read_merchant_managed_fulfillment_orders,write_merchant_managed_fulfillment_orders,"
    "read_third_party_fulfillment_orders,write_third_party_fulfillment_orders,"
    "read_fulfillments,write_fulfillments," # Added for sync consistency
    "read_files,write_files,"
    "read_reports,write_reports,"
    "read_price_rules,write_price_rules,"
    "read_discounts,write_discounts,"
    "read_returns,write_returns,"
    "read_companies,write_companies,"
    "write_payments"
)

FREE_STORES = [
    "vjtrading.myshopify.com",
    "your-dev-store.myshopify.com"
]

# [ADD THIS BLOCK] -> This tells the library your keys globally
shopify.Session.setup(api_key=SHOPIFY_API_KEY, secret=SHOPIFY_API_SECRET)

# --- FIX: PATCH SHOPIFY LIBRARY FOR 2025-01 API ---
try:
    from shopify.resources.fulfillment_order import FulfillmentOrder
except ImportError:
    # 1. Define missing FulfillmentOrder class (Fixes "has no attribute 'FulfillmentOrder'")
    class FulfillmentOrder(shopify.ShopifyResource):
        _prefix_source = "/orders/$order_id/"
        _plural = "fulfillment_orders"
    shopify.FulfillmentOrder = FulfillmentOrder

# 2. CRITICAL FIX FOR 406 ERROR:
# The library defaults to the old URL: /orders/:id/fulfillments.json (Blocked by Shopify)
# This line forces it to use the new URL: /fulfillments.json
shopify.Fulfillment._prefix_source = "/"
# ---------------------------------------------------

app = Flask(__name__)

app.secret_key = os.getenv('FLASK_SECRET_KEY')

# --- CONFIGURATION ---
database_url = os.getenv('DATABASE_URL', 'sqlite:///local.db')

if database_url:
    # 1. Fix Render's "postgres://" to standard "postgresql://"
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql://", 1)
        
    # 2. IMPORTANT: Do NOT add +pg8000. 
    # SQLAlchemy will automatically use the new 'psycopg2' driver.

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# 3. SET SSL MODE FOR PSYCOPG2 (Standard Driver)
# FIX: Robust DB connection settings to prevent "SSL Decryption" errors
app.config['SQLALCHEMY_ENGINE_OPTIONS'] = {
    "pool_pre_ping": True,      # <--- Checks if connection is alive before using it
    "pool_recycle": 300,        # <--- Refreshes connection every 5 minutes
    "pool_timeout": 30,
    "pool_size": 10,
    "max_overflow": 20,
    "connect_args": {
        "sslmode": "require",
        "keepalives": 1,
        "keepalives_idle": 30,
        "keepalives_interval": 10,
        "keepalives_count": 5
    }
}

SHOPIFY_LOCATION_ID = int(os.getenv('SHOPIFY_WAREHOUSE_ID', '0'))

db.init_app(app)

# --- DB INIT ---
with app.app_context():
    try: 
        db.create_all()
        print("Database tables created/verified.")
    except Exception as e: 
        print(f"CRITICAL DB INIT ERROR: {e}")


def verify_shopify(data, hmac_header):
    # FIX: Use the consistent variable name
    secret = os.getenv('SHOPIFY_API_SECRET') 
    if not secret: return True 
    if not hmac_header: return False
    digest = hmac.new(secret.encode('utf-8'), data, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), hmac_header)



def send_inventory_alert(shop_url, email_address, discrepancies):
    """Sends a summary email of all flagged inventory differences."""
    msg = EmailMessage()
    msg['Subject'] = f"⚠️ Inventory Alert: {shop_url}"
    msg['From'] = os.getenv('EMAIL_USER')
    msg['To'] = email_address

    content = f"Inventory Sync for {shop_url} found items exceeding your threshold:\n\n"
    for item in discrepancies:
        content += f"SKU: {item['sku']} | Odoo: {item['odoo']} | Shopify: {item['shopify']} | Difference: {item['diff']}\n"
    
    msg.set_content(content)

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(os.getenv('EMAIL_USER'), os.getenv('EMAIL_PASS'))
            smtp.send_message(msg)
    except Exception as e:
        print(f"Failed to send alert email: {e}")
        

def sync_categories_only(shop_url):
    """
    Optimized ONE-TIME import of Categories from Shopify to Odoo.
    STRATEGY: Reverse-Linking to bypass POS crashes.
    FIX: Now passes shop_url to get_config so Company ID is found.
    """
    with app.app_context():
        # --- DYNAMIC CONNECT ---
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): 
            log_event('System', 'Error', "Category Sync Failed: Connection Error", shop_url=shop_url)
            return
        # -----------------------

        log_event('System', 'Info', "Starting eCommerce Category Sync (Reverse-Link Mode)...", shop_url=shop_url)
        
        # FIX: Added shop_url=shop_url here
        company_id = get_config('odoo_company_id', shop_url=shop_url)
        
        # 1. Load Odoo Data
        try:
            # We need product_tmpl_id for the reverse link
            domain = [['active', '=', True]]
            if company_id: domain.append(['company_id', '=', int(company_id)])

            odoo_prods = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'search_read',
                [domain],
                {'fields': ['default_code', 'product_tmpl_id', 'public_categ_ids']}
            )
            odoo_map = {p['default_code']: p for p in odoo_prods if p.get('default_code')}
            
            cat_map = {}
            cats = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                'product.public.category', 'search_read', [[]], {'fields': ['id', 'name']})
            for c in cats: cat_map[c['name']] = c['id']
            
            log_event('System', 'Info', f"Loaded {len(odoo_map)} Products and {len(cat_map)} eCommerce Categories.", shop_url=shop_url)
        except Exception as e: 
            log_event('System', 'Error', f"Category Setup Failed: {e}", shop_url=shop_url)
            return

        updated_count = 0
        processed_count = 0
        
        page = shopify.Product.find(limit=50)
        
        while page:
            for sp in page:
                processed_count += 1
                if processed_count % 50 == 0:
                    log_event('System', 'Info', f"Scanned {processed_count} Shopify products...", shop_url=shop_url)

                if not sp.product_type: continue
                
                variant = sp.variants[0] if sp.variants else None
                if not variant or not variant.sku: continue
                sku = variant.sku
                if sku.endswith('-UNIT'): sku = sku.replace('-UNIT', '')

                odoo_prod = odoo_map.get(sku)
                
                if not odoo_prod: continue
                if odoo_prod.get('public_categ_ids'): continue # Already has category

                try:
                    cat_name = sp.product_type.strip()
                    if not cat_name: continue

                    # Find or Create Category
                    cat_id = cat_map.get(cat_name)
                    if not cat_id:
                        log_event('System', 'Info', f"Creating new eCommerce Category: '{cat_name}'", shop_url=shop_url)
                        cat_id = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                            'product.public.category', 'create', [{'name': cat_name}])
                        cat_map[cat_name] = cat_id
                    
                    # --- THE FIX: Write to the CATEGORY, not the Product ---
                    tmpl_id = odoo_prod['product_tmpl_id'][0]
                    
                    odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                        'product.public.category', 'write', 
                        [[cat_id], {'product_tmpl_ids': [(4, tmpl_id)]}]
                    )
                    
                    updated_count += 1
                    odoo_prod['public_categ_ids'] = [cat_id]
                    log_event('System', 'Success', f"Linked {sku} -> '{cat_name}' (via Category)", shop_url=shop_url)

                except Exception as e:
                    log_event('System', 'Warning', f"Skipped {sku} due to Odoo Lock: {e}", shop_url=shop_url)

            if page.has_next_page(): 
                try: page = page.next_page()
                except: break
            else: break
        
        log_event('System', 'Success', f"Category Sync Finished. Updated {updated_count} products.", shop_url=shop_url)

        
def perform_inventory_sync(shop_url):
    discrepancy_list = []
    """
    Runs inside the Worker Process.
    STRATEGY: ID-Based Sync + Active Only Filter.
    """
    with app.app_context():
        log_event('Inventory', 'Info', "Starting Inventory Sync...", shop_url=shop_url)
        
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): return

        # --- AUTO-DETECT SHOPIFY LOCATION ---
        shopify_location_id = None
        try:
            saved_id = get_config('shopify_target_location_id', None, shop_url=shop_url)
            
            if saved_id:
                shopify_location_id = int(saved_id)
            else:
                locs = shopify.Location.find()
                active_locs = [l for l in locs if l.active]
                
                if active_locs:
                    shopify_location_id = active_locs[0].id
                    log_event('Inventory', 'Info', f"Auto-selected Shopify Location: {active_locs[0].name} (ID: {shopify_location_id})", shop_url=shop_url)
                else:
                    log_event('Inventory', 'Error', "No active locations found in Shopify!", shop_url=shop_url)
                    return
        except Exception as e:
            log_event('Inventory', 'Error', f"Failed to detect Shopify Location: {e}", shop_url=shop_url)
            return

        # Load Configs
        target_locations = get_config('inventory_locations', [], shop_url=shop_url)
        target_field = get_config('inventory_field', 'qty_available', shop_url=shop_url)
        sync_zero = get_config('sync_zero_stock', False, shop_url=shop_url)
        alert_threshold = int(get_config('alert_threshold', 50, shop_url=shop_url))
        alert_email = get_config('alert_email', None, shop_url=shop_url)

        # 1. FETCH SHOPIFY VARIANTS (ACTIVE ONLY)
        shopify_variants = {} 
        try:
            # FIX: Added status='active' to ignore Archived/Draft products
            page = shopify.Product.find(limit=250, status='active')
            
            while page:
                for p in page:
                    for v in p.variants:
                        if v.sku:
                            shopify_variants[v.sku] = v
                if page.has_next_page(): 
                    page = page.next_page()
                else: 
                    break
        except Exception as e:
            log_event('Inventory', 'Error', f"Shopify Fetch Failed: {e}", shop_url=shop_url)
            return

        total_shopify = len(shopify_variants)
        
        # 2. RESOLVE ODOO IDs LOCALLY
        sku_to_odoo_id = {}
        all_skus = list(shopify_variants.keys())
        
        mappings = ProductMap.query.filter(ProductMap.shop_url == shop_url, ProductMap.sku.in_(all_skus)).all()
        for m in mappings:
            sku_to_odoo_id[m.sku] = m.odoo_product_id
            
        missing_skus = [sku for sku in all_skus if sku not in sku_to_odoo_id]
        
        if missing_skus:
            # Only log this once at start, not repeatedly
            log_event('Inventory', 'Info', f"Found {len(missing_skus)} unmapped items. Searching Odoo...", shop_url=shop_url)
            try:
                CHUNK = 10
                found_skus = set() 

                for i in range(0, len(missing_skus), CHUNK):
                    chunk = missing_skus[i:i+CHUNK]
                    domain = [['default_code', 'in', chunk], ['active', '=', True]]
                    res = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                        'product.product', 'search_read', [domain], {'fields': ['default_code']})
                    
                    for r in res:
                        sku = r['default_code']
                        sku_to_odoo_id[sku] = r['id']
                        found_skus.add(sku)
                        
                        # SAVE MAPPING
                        if not ProductMap.query.filter_by(shop_url=shop_url, sku=sku).first():
                            db.session.add(ProductMap(
                                shop_url=shop_url, 
                                sku=sku, 
                                odoo_product_id=r['id'],
                                shopify_variant_id='0' 
                            ))

                db.session.commit()

                # --- IGNORE LOGIC ---
                # Save failures as -1 so we don't ask Odoo again
                for sku in missing_skus:
                    if sku not in found_skus:
                        if not ProductMap.query.filter_by(shop_url=shop_url, sku=sku).first():
                            db.session.add(ProductMap(
                                shop_url=shop_url, 
                                sku=sku, 
                                odoo_product_id=-1,
                                shopify_variant_id='0'
                            ))
                
                db.session.commit()

            except Exception as e:
                db.session.rollback()
                log_event('Inventory', 'Warning', f"Fallback search failed: {e}", shop_url=shop_url)

        # 3. BATCH SYNC
        map_items = list(sku_to_odoo_id.items())
        updates = 0
        processed = 0
        BATCH_SIZE = 50
        
        for i in range(0, len(map_items), BATCH_SIZE):
            batch = map_items[i:i+BATCH_SIZE]
            batch_ids = [item[1] for item in batch if item[1] > 0] # Skip ignored items (-1)
            batch_skus = {item[1]: item[0] for item in batch if item[1] > 0}
            
            if not batch_ids: continue

            try:
                # Read Odoo Stock
                qty_map = {pid: 0 for pid in batch_ids}
                for loc_id in target_locations:
                    ctx = {'location': loc_id}
                    stock_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                        'product.product', 'read', [batch_ids], {'fields': [target_field], 'context': ctx})
                    for record in stock_data:
                        qty_map[record['id']] += record.get(target_field, 0)
                
                # Update Shopify
                for pid, total_qty in qty_map.items():
                    sku = batch_skus[pid]
                    sp_variant = shopify_variants.get(sku)
                    
                    if not sp_variant: continue
                    if sync_zero and total_qty <= 0: continue
                    
                    current_shopify_qty = int(sp_variant.inventory_quantity) if sp_variant.inventory_quantity else 0

                    diff = abs(int(total_qty) - current_shopify_qty)
                    if diff >= alert_threshold:
                        discrepancy_list.append({
                            'sku': sku, 'odoo': int(total_qty), 
                            'shopify': current_shopify_qty, 'diff': diff
                        })
                    
                    if int(total_qty) != current_shopify_qty:
                        try:
                            shopify.InventoryLevel.set(
                                location_id=shopify_location_id, 
                                inventory_item_id=sp_variant.inventory_item_id,
                                available=int(total_qty)
                            )
                            updates += 1
                        except Exception as e:
                            print(f"Failed to update {sku}: {e}")

                processed += len(batch)
                    
            except Exception as e:
                log_event('Inventory', 'Error', f"Batch Error: {e}", shop_url=shop_url)

        if discrepancy_list and alert_email:
            send_inventory_alert(shop_url, alert_email, discrepancy_list)

        current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        set_config('last_inventory_sync_success', current_time, shop_url=shop_url)

        # Final Summary Log
        log_event('Inventory', 'Success', f"Sync Complete. Checked {total_shopify} active items. Updated {updates} products.", shop_url=shop_url)


def scheduled_inventory_sync(shop_url):
    with app.app_context():
        # FIX: Just call the function. It handles its own logging now.
        perform_inventory_sync(shop_url) 
        
        try:
            shop = Shop.query.filter_by(shop_url=shop_url).first()
            if shop:
                shop.last_inventory_sync_success = datetime.utcnow()
                db.session.commit()
        except Exception as e:
            print(f"Error updating inventory timestamp: {e}")

# ==========================================
# SHOPIFY OAUTH ROUTES
# ==========================================
@app.route('/install')
def install():
    """Step 1: Redirect merchant to Shopify Permissions Screen."""
    shop = request.args.get('shop')
    if not shop:
        return "Missing 'shop' parameter. Launch this app from Shopify Admin.", 400
    
    auth_url = (f"https://{shop}/admin/oauth/authorize?"
                f"client_id={SHOPIFY_API_KEY}&"
                f"scope={SCOPES}&"
                f"redirect_uri={APP_URL}/auth/callback")
    return redirect(auth_url)

@app.route('/auth/callback')
def auth_callback():
    shop_url = request.args.get('shop')
    
    # 1. Convert Flask params to a standard dictionary (Fixes HMAC format issues)
    params = dict(request.args)
    
    # 2. Security Check: Validate the request actually came from Shopify
    try:
        if not shopify.Session.validate_params(params):
            return "Auth Failed: Invalid HMAC (Signature Mismatch). Check Client Secret.", 400
    except Exception as e:
        return f"Validation Error: {e}", 400

   # 3. Exchange Code for Token
    try:
        session = shopify.Session(shop_url, SHOPIFY_API_VERSION)
        access_token = session.request_token(params) 
    except Exception as e:
        return f"Token Exchange Failed: {e}", 400

    # 4. Save to Database
    existing_shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not existing_shop:
        new_shop = Shop(shop_url=shop_url, access_token=access_token)
        db.session.add(new_shop)
    else:
        existing_shop.access_token = access_token
        existing_shop.is_active = True
    
    db.session.commit()

    # 5. AUTO-CONFIG: Register Webhooks immediately
    automate_webhook_registration(shop_url)
    
    # 6. AUTO-CONFIG: Queue an initial discovery sync
    # ✅ FIX: Use q_default (Master syncs are heavy)
    q_default.enqueue(sync_products_master, shop_url)

    # 7. Redirect to Dashboard
    return redirect(f"https://{shop_url}/admin/apps/{SHOPIFY_API_KEY}")

#Billing 
@app.route('/billing/create')
@require_shopify_session
def create_billing():
    shop_url = request.args.get('shop')
    shopify_shop = shopify.Shop.current()
    raw_plan = shopify_shop.plan_name.lower()
    
    # 1. Bypass check (Dev Stores)
    if any(x in raw_plan for x in ['affiliate', 'staff', 'partner_test', 'trial']):
        return redirect(url_for('home', shop=shop_url))
    
    # 2. Tiered Pricing Logic
    if 'plus' in raw_plan:
        price, display_name = 89.0, "Plus Plan"
    elif 'advanced' in raw_plan:
        price, display_name = 49.0, "Advanced Plan"
    else:
        # This covers 'basic', 'shopify' (standard), and any other paid plans
        price, display_name = 25.0, "Standard Plan"

    charge = shopify.RecurringApplicationCharge.create({
        "name": f"Odoo Connector - {display_name}",
        "price": price,
        "return_url": f"{os.getenv('HOST')}/billing/confirm?shop={shop_url}",
        "test": True, # KEEP TRUE until App Store submission
        "trial_days": 7
    })
    return redirect(charge.confirmation_url)


@app.route('/billing/confirm')
@require_shopify_session
def confirm_billing():
    shop_url = request.args.get('shop')
    charge_id = request.args.get('charge_id')
    
    if not charge_id:
        return redirect(url_for('home', shop=shop_url))

    charge = shopify.RecurringApplicationCharge.find(charge_id)
    
    if charge.activate():
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if shop:
            shop.charge_id = str(charge_id)
            shop.plan_name = charge.name
            db.session.commit()
        return redirect(url_for('home', shop=shop_url))
    
    return "Billing activation failed. Please contact support.", 400
    

# -----------------------------------------------------------------
# SHOPIFY WEBHOOK RECEIVER
# -----------------------------------------------------------------
# In app.py

@app.route('/webhooks/shopify', methods=['POST'])
def shopify_webhook():
    """
    Receives automated notifications from Shopify.
    Handles Orders, Products, Cancellations, and Refunds.
    """
    topic = request.headers.get('X-Shopify-Topic')
    shop_url = request.headers.get('X-Shopify-Shop-Domain')
    data = request.get_json()

    if not shop_url or not data:
        return "Missing data", 400

    # SAFETY CHECK: Explicitly block product updates to prevent loops
    if topic == 'products/update':
        return "Ignored (Odoo is Master)", 200

    # 1. Handle Orders (Create, Pay, Update) -> CRITICAL QUEUE
    if topic in ['orders/create', 'orders/updated', 'orders/paid']:
        q_critical.enqueue(
            background_order_sync, 
            shop_url, 
            data,
            retry=Retry(max=5, interval=[60, 120, 240, 600, 1200])
        )
        return "Order Received", 200

    # 2. Handle Cancellations -> CRITICAL QUEUE
    elif topic == 'orders/cancelled':
        q_critical.enqueue(
            process_cancellation, 
            data, 
            shop_url,
            retry=Retry(max=5, interval=[60, 120, 240, 600, 1200])
        )
        return "Cancellation Received", 200

    # 3. Handle Refunds -> CRITICAL QUEUE
    elif topic == 'refunds/create':
        q_critical.enqueue(
            background_refund_sync, 
            shop_url, 
            data,
            retry=Retry(max=5, interval=[60, 120, 240, 600, 1200])
        )
        return "Refund Received", 200

    # 4. Handle Products -> DEFAULT QUEUE (Because it's heavy/less urgent)
    elif topic == 'products/create': 
       # q_default.enqueue(background_product_sync, shop_url, data)
        return "Product Received", 200

    return "Topic ignored", 200


@app.route('/webhooks/app_uninstalled', methods=['POST'])
def app_uninstalled():
    """
    Triggered when a merchant deletes the app.
    We mark the shop as inactive to stop background syncs.
    """
    data = request.get_json()
    shop_url = request.headers.get('X-Shopify-Shop-Domain')

    try:
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if shop:
            shop.is_active = False
            shop.access_token = None  # Remove token for security
            db.session.commit()
            print(f"👋 Shop Uninstalled: {shop_url}. Marked as inactive.")
            
            # Optional: Log a system-wide event
            log_event('System', 'Notice', f"App uninstalled by {shop_url}", shop_url=shop_url)
            
        return "OK", 200
    except Exception as e:
        print(f"Error handling uninstall for {shop_url}: {e}")
        return "Error", 500

@app.route('/', methods=['GET'])
@app.route('/settings', methods=['GET'])
@app.route('/maintenance', methods=['GET'])
def home():
    shop_url = request.args.get('shop')
    if not shop_url: 
        return "No shop provided."
    
    shop = Shop.query.filter_by(shop_url=shop_url).first()
    if not shop: 
        return redirect(url_for('install', shop=shop_url))

    # --- 1. BILLING CHECK ---
    is_free_store = shop_url in FREE_STORES
    
    # We only run the billing logic if they are NOT a white-listed free store
    if not is_free_store and not shop.charge_id:
        try:
            shopify_shop = shopify.Shop.current()
            raw_plan = shopify_shop.plan_name.lower()
            
            # Development stores (affiliate/staff) are free
            is_dev = any(x in raw_plan for x in ['affiliate', 'staff', 'partner_test'])
            
            if not is_dev:
                # This triggers the tiered pricing logic we wrote in /billing/create
                return redirect(url_for('create_billing', shop=shop_url))
        except Exception as e:
            print(f"Billing bypass error: {e}")

    mode = request.args.get('mode')

    # --- 2. SHOW CONNECT FORM (If credentials missing OR user requested edit) ---
    if not shop.odoo_url or not shop.odoo_password or mode == 'connect':
        html = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Connect Odoo</title>
            <script 
              src="https://cdn.shopify.com/static/frontend/app-bridge-next/latest/app-bridge.js"
              data-api-key="{{ api_key }}"
              data-shop-origin="{{ shop_url }}"
            ></script>
            <style>
                body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; padding: 40px; background: #f4f6f8; }
                .card { background: white; border: 1px solid #dfe3e8; padding: 30px; border-radius: 8px; max-width: 500px; margin: 0 auto; box-shadow: 0 4px 12px rgba(0,0,0,0.05); }
                input { width: 100%; padding: 12px; margin: 8px 0; box-sizing: border-box; border: 1px solid #ccc; border-radius: 4px; }
                button { background: #008060; color: white; border: none; padding: 12px 24px; border-radius: 4px; cursor: pointer; font-size: 16px; margin-top: 15px; width: 100%; font-weight: bold; }
                button:hover { background: #004c3f; }
                label { font-weight: 600; display: block; margin-top: 15px; color: #202223; }
                .back-link { display: block; text-align: center; margin-top: 20px; color: #5c5f62; text-decoration: none; }
                .back-link:hover { text-decoration: underline; }
            </style>
        </head>
        <body>
            <div class="card">
                <h2>🔌 Connect Odoo to Shopify</h2>
                <p style="text-align: center; color: #6d7175;">Update your credentials below.</p>
                <hr style="border: 0; border-top: 1px solid #eee; margin: 20px 0;">
                <form action="/save_settings" method="POST">
                    <input type="hidden" name="shop_url" value="{{ shop_url }}">
                    <label>Odoo URL</label>
                    <input type="text" name="odoo_url" value="{{ odoo_url }}" placeholder="https://..." required>
                    <label>Database Name</label>
                    <input type="text" name="odoo_db" value="{{ odoo_db }}" required>
                    <label>Username (Email)</label>
                    <input type="text" name="odoo_user" value="{{ odoo_user }}" required>
                    <label>Password (Leave empty to keep unchanged)</label>
                    <input type="password" name="odoo_pass" placeholder="••••••••">
                    <button type="submit">Save & Connect</button>
                </form>
                {% if has_creds %}
                <a href="/?shop={{ shop_url }}" class="back-link">← Back to Dashboard</a>
                {% endif %}
            </div>
        </body>
        </html>
        """
        return render_template_string(html, 
            api_key=SHOPIFY_API_KEY, 
            shop_url=shop.shop_url,
            odoo_url=shop.odoo_url or '',
            odoo_db=shop.odoo_db or '',
            odoo_user=shop.odoo_username or '',
            has_creds=(shop.odoo_url is not None)
        )

    # --- 3. SHOW MAIN DASHBOARD (Tabbed Interface) ---
    config = {
        'odoo_url': shop.odoo_url,
        'odoo_db': shop.odoo_db,
        'odoo_username': shop.odoo_username,
        'odoo_company_id': shop.odoo_company_id,
        'charge_id': shop.charge_id,
        'plan_name': shop.plan_name or "Development Store",
        'is_free_store': is_free_store,
        'sync_start_date': shop.sync_start_date,
        'last_inventory_sync_success': shop.last_inventory_sync_success,
        'last_order_sync_success': shop.last_order_sync_success,
        'last_return_sync_success': shop.last_return_sync_success
    }

    settings = AppSetting.query.filter_by(shop_url=shop_url).all()
    for s in settings:
        try:
            config[s.key] = json.loads(s.value)
        except:
            config[s.key] = s.value

    clean_shop = shop_url.replace("https://", "").replace("http://", "").split('/')[0]
    return render_template('dashboard.html', 
                           shop_url=shop_url, 
                           shop_origin=clean_shop, 
                           api_key=SHOPIFY_API_KEY, 
                           config=config)

    
@app.route('/live_logs')
def live_logs():
    return render_template('live_logs.html')

@app.route('/api/logs/live', methods=['GET'])
def api_live_logs():
    shop_url = request.args.get('shop')
    try:
        # FIX: Filter logs by the current shop OR global system messages
        logs = SyncLog.query.filter(
            (SyncLog.shop_url == shop_url) | (SyncLog.shop_url == 'System')
        ).order_by(SyncLog.timestamp.desc()).limit(500).all()
        
        data = []
        for log in logs:
            msg_type = 'info'
            status_lower = (log.status or '').lower()
            if 'error' in status_lower or 'fail' in status_lower: msg_type = 'error'
            elif 'success' in status_lower: msg_type = 'success'
            elif 'warning' in status_lower or 'skip' in status_lower: msg_type = 'warning'
            
            iso_ts = log.timestamp.isoformat()
            if not iso_ts.endswith('Z'): iso_ts += 'Z'
            
            data.append({
                'id': log.id, 
                'timestamp': iso_ts, 
                'message': f"[{log.entity}] {log.message}", 
                'type': msg_type, 
                'details': log.status
            })
        return jsonify(data)
    except: return jsonify([])


@app.route('/api/odoo/refresh_locations', methods=['GET'])
@require_shopify_session 
def api_refresh_locations():
    shop_url = request.args.get('shop')
    if not shop_url: 
        return jsonify({'error': 'Missing shop param'}), 400

    # 1. Connect to Odoo
    odoo = get_odoo_connection(shop_url)
    if not odoo: 
        return jsonify({'error': 'Could not connect to Odoo'}), 500

    try:
        # 2. Fetch Locations from Odoo
        company_id = get_config('odoo_company_id', shop_url=shop_url)
        locations = odoo.get_locations(company_id=company_id)

        # 3. Cache them in the Database
        set_config('available_locations', locations)

        return jsonify({
            "message": f"Success! Found {len(locations)} locations in Odoo.",
            "locations": locations
        })

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/maintenance/wipe_logs', methods=['GET'])
def maintenance_wipe_logs():
    """Deletes ALL logs to give a clean slate."""
    with app.app_context():
        try:
            num_deleted = db.session.query(SyncLog).delete()
            db.session.commit()
            return jsonify({"message": f"SUCCESS: Deleted {num_deleted} old log entries."})
        except Exception as e:
            db.session.rollback()
            return jsonify({"error": str(e)})

@app.route('/maintenance/force_tax_all', methods=['GET'])
def force_tax_all():
    shop_url = request.args.get('shop')
    if not setup_shopify_session(shop_url): 
        return jsonify({"error": "Auth Failed"}), 401
    
    count = 0
    # Fetch all customers (250 at a time)
    page = shopify.Customer.find(limit=250)
    
    while page:
        for cust in page:
            # If they are currently exempt, make them taxable
            if cust.tax_exempt is True:
                cust.tax_exempt = False
                cust.save()
                count += 1
        
        if page.has_next_page():
            page = page.next_page()
        else:
            break
            
    log_event('Maintenance', 'Success', f"Force-restored GST collection for {count} customers.", shop_url=shop_url)
    return jsonify({"message": f"Successfully fixed {count} customers. GST is now being collected."})

@app.route('/maintenance/clear_queue', methods=['POST'])
def clear_background_queue():
    """
    Emergency tool to wipe all pending tasks.
    """
    shop_url = request.args.get('shop')
    try:
        # ✅ FIX: Empty BOTH queues
        q_critical.empty()
        q_default.empty()
        
        log_event('System', 'Warning', "All background queues manually cleared.", shop_url=shop_url)
        return jsonify({"message": "Background queues cleared successfully."}), 200
    except Exception as e:
        return jsonify({"error": f"Failed to clear queue: {str(e)}"}), 500
        

@app.route('/sync/inventory', methods=['GET'])
@require_shopify_session        
def sync_inventory_endpoint():
    shop_url = request.args.get('shop')
    if not shop_url: return jsonify({"error": "Missing shop parameter"}), 400
    
    # ✅ FIX: Use q_critical
    job = q_critical.enqueue(perform_inventory_sync, shop_url, job_timeout=3600)
    
    return jsonify({"message": f"Full Inventory Sync Queued (Job ID: {job.get_id()})"})


def task_force_name_repair(shop_url):
    """
    ONE-TIME REPAIR (PAGINATED): 
    1. Removes "." from Last Names.
    2. Applies Group/Site logic.
    3. AUTOMATICALLY DELETES individual contacts unless whitelisted.
    4. PAGINATION: Processes 1,000 records at a time to prevent memory crashes.
    """
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): return

        company_id = get_config('odoo_company_id', shop_url=shop_url)
        
        # Config
        raw_groups = get_config('group_companies_list', '', shop_url=shop_url)
        group_whitelist = [g.strip().lower() for g in raw_groups.split(',') if g.strip()]

        log_event('Repair', 'Info', f"Starting Paginated Repair. Whitelist: {group_whitelist}", shop_url=shop_url)

        # Domain
        domain = [['active', '=', True], ['type', '!=', 'private']]
        if company_id: domain.append(['company_id', '=', int(company_id)])

        # --- PAGINATION LOOP ---
        limit = 1000
        offset = 0
        total_processed = 0
        deleted_count = 0
        fixed_count = 0
        
        while True:
            try:
                # Fetch Batch
                customers = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                    'res.partner', 'search_read', [domain], 
                    {'fields': ['id', 'name', 'email', 'parent_id'], 'limit': limit, 'offset': offset}
                )
            except Exception as e:
                log_event('Repair', 'Error', f"Fetch Error at offset {offset}: {e}", shop_url=shop_url)
                break

            if not customers:
                break # No more records

            for p in customers:
                email = p.get('email')
                if not email or "@" not in email: continue

                parent_info = p.get('parent_id')
                
                # --- LOGIC: SHOULD THIS EXIST IN SHOPIFY? ---
                should_exist = True
                if parent_info:
                    parent_name = parent_info[1]
                    is_whitelisted = any(g in parent_name.lower() for g in group_whitelist)
                    if not is_whitelisted:
                        should_exist = False # It's an unwanted contact

                # --- EXECUTE ---
                try:
                    results = shopify.Customer.search(query=f"email:{email}")
                    
                    if not should_exist:
                        # DELETE
                        if results:
                            for cust in results:
                                cust.destroy()
                                deleted_count += 1
                                log_event('Repair', 'Warning', f"🗑️ Deleted: {email}", shop_url=shop_url)
                        continue 

                    # UPDATE
                    if results:
                        cust = results[0]
                        final_display_name = p.get('name') 
                        if parent_info:
                            final_display_name = p.get('name') # Use Child Name
                        else:
                            final_display_name = p.get('name') 

                        new_first_name = final_display_name
                        new_last_name = "" # Empty string

                        if cust.first_name != new_first_name or cust.last_name != new_last_name:
                            cust.first_name = new_first_name
                            cust.last_name = new_last_name
                            cust.save()
                            fixed_count += 1
                            
                except Exception as e:
                    print(f"Repair Error {email}: {e}")

            # Prepare for next batch
            offset += limit
            total_processed += len(customers)
            log_event('Repair', 'Info', f"Batch Done. Processed: {total_processed}, Fixed: {fixed_count}, Deleted: {deleted_count}...", shop_url=shop_url)

        log_event('Repair', 'Success', f"Job Complete. Scanned {total_processed}. Fixed {fixed_count}. Deleted {deleted_count}.", shop_url=shop_url)


# --- ROUTES FOR MANUAL TOOLS ---

@app.route('/maintenance/force_name_repair', methods=['GET'])
@require_shopify_session
def trigger_name_repair():
    shop_url = request.args.get('shop')
    if not shop_url: return jsonify({"error": "Missing shop parameter"}), 400
    
    # SLOW LANE
    q_default.enqueue(task_force_name_repair, shop_url, job_timeout=1800)
    
    return jsonify({"message": "Global Name Repair Queued. Check Live Logs."})

@app.route('/maintenance/purge_junk', methods=['GET'])
@require_shopify_session
def trigger_purge():
    shop_url = request.args.get('shop')
    if not shop_url: return jsonify({"error": "Missing shop parameter"}), 400
    
    # SLOW LANE
    q_default.enqueue(emergency_purge_junk_products, shop_url, job_timeout=600)
    return jsonify({"message": "Emergency Purge Queued."})

@app.route('/sync/images/manual', methods=['GET'])
def trigger_manual_image_sync():
    shop_url = request.args.get('shop')
    if not shop_url: return jsonify({"error": "Missing shop parameter"}), 400
    
    # SLOW LANE (Images take a long time)
    q_default.enqueue(sync_images_only_manual, shop_url, job_timeout=1800)
    return jsonify({"message": "Image Sync Queued."})

@app.route('/maintenance/diagnose_categories', methods=['GET'])
def trigger_diagnose():
    shop_url = request.args.get('shop')
    if not shop_url: return jsonify({"error": "Missing shop parameter"}), 400
    
    # SLOW LANE
    q_default.enqueue(check_for_corrupted_categories, shop_url, job_timeout=300)
    return jsonify({"message": "Diagnostic Queued."})

@app.route('/maintenance/fix_variants', methods=['POST'])
@require_shopify_session
def trigger_fix_variants():
    shop_url = request.args.get('shop')
    if not shop_url: return jsonify({"error": "Missing shop parameter"}), 400
    
    # SLOW LANE
    q_default.enqueue(fix_variant_mess_task, shop_url, job_timeout=900)
    return jsonify({"message": "Variant Cleanup Queued."})


@app.route('/maintenance/register_webhooks', methods=['GET'])
@require_shopify_session
def register_webhooks_manual():
    shop_url = request.args.get('shop')
    if not setup_shopify_session(shop_url):
        return jsonify({"error": "Auth failed"}), 401

    app_host = os.getenv('HOST') 
    if not app_host:
        return jsonify({"error": "HOST env var missing"}), 500

    target_address = f"{app_host}/webhooks/shopify"
    
    required_topics = [
        'orders/create',
        'orders/updated',
        'orders/cancelled',
        'products/create',
        'refunds/create',
        # 'products/update',
        'inventory_levels/update' 
    ]

    results = []
    existing_hooks = shopify.Webhook.find()

    for topic in required_topics:
        match = next((h for h in existing_hooks if h.topic == topic and h.address == target_address), None)
        
        if not match:
            new_hook = shopify.Webhook()
            new_hook.topic = topic
            new_hook.address = target_address
            new_hook.format = 'json'
            try:
                if new_hook.save():
                    results.append(f"✅ Created {topic}")
                else:
                    results.append(f"❌ Failed {topic}: {new_hook.errors.full_messages()}")
            except Exception as e:
                results.append(f"❌ Error {topic}: {str(e)}")
        else:
            results.append(f"⏭️ Exists {topic}")

    return jsonify({"message": "Webhook Registration Complete", "details": results})


@app.route('/maintenance/clear_product_map', methods=['GET', 'POST'])
def clear_product_map():
    shop_url = request.args.get('shop')
    if not shop_url:
        return jsonify({"error": "Missing shop parameter"}), 400
        
    try:
        with app.app_context():
            ProductMap.query.filter_by(shop_url=shop_url).delete()
            db.session.commit()
            
        log_event('Maintenance', 'Success', "Product ID map cleared. Next sync will re-link all items.", shop_url=shop_url)
        return jsonify({"message": "Product map cleared successfully."})
    except Exception as e:
        db.session.rollback() # Always rollback on error
        return jsonify({"error": str(e)}), 500

@app.route('/sync/fulfillments', methods=['GET'])
def trigger_fulfillment_sync():
    shop_url = request.args.get('shop')
    if not shop_url: return jsonify({"error": "Missing shop parameter"}), 400

    # FAST LANE (Fulfillments are critical/fast)
    q_critical.enqueue(sync_odoo_fulfillments, shop_url, job_timeout=600)
    return jsonify({"message": "Started checking for shipments (Queued)."})

@app.route('/sync/cancellations', methods=['GET'])
def trigger_cancellation_sync():
    shop_url = request.args.get('shop')
    if not shop_url: return jsonify({"error": "Missing shop parameter"}), 400
    
    # Critical Queue (Fast)
    q_critical.enqueue(sync_odoo_cancellations, shop_url, job_timeout=600)
    return jsonify({"message": "Odoo Cancellation Sync Queued"})

@app.route('/sync/returns', methods=['GET'])
def trigger_return_sync():
    shop_url = request.args.get('shop')
    if not shop_url: return jsonify({"error": "Missing shop parameter"}), 400
    
    # Critical Queue (Fast)
    q_critical.enqueue(sync_odoo_returns, shop_url, job_timeout=600)
    return jsonify({"message": "Odoo Return Sync Queued"})

@app.route('/sync/categories/run_initial_import', methods=['GET'])
def run_initial_category_import():
    shop_url = request.args.get('shop')
    if not shop_url: return jsonify({"error": "Missing shop parameter"}), 400

    # SLOW LANE
    q_default.enqueue(sync_categories_only, shop_url, job_timeout=600)
    return jsonify({"message": "Category Sync Job Queued"})


# --- 1. NEW HELPER: Background Job for Products ---
# def background_product_sync(shop_url, product_data):
#     """
#     Runs inside the Worker Process. 
#     UPDATED: Aggregates success logs to Redis instead of spamming the DB.
#     """
#     with app.app_context():
#         # Connect
#         odoo = get_odoo_connection(shop_url)
#         if not odoo:
#             log_event('Product', 'Error', "Auto Sync Failed: No Odoo Connection.", shop_url=shop_url)
#             return

#         product_title = product_data.get('title', 'Unknown')
#         try:
#             # Sync Logic
#             process_product_data(product_data, odoo, shop_url=shop_url)
            
#             # --- AGGREGATION LOGIC ---
#             # Key format: log_buffer_products_{shop_url}
#             redis_key = f"log_buffer_products_{shop_url}"
#             conn.incr(redis_key)
#             # -------------------------
            
#         except Exception as e:
#             # We still log errors immediately because they are important
#             log_event('Product', 'Error', f"Webhook Failed for '{product_title}': {str(e)}", shop_url=shop_url)

def background_product_sync(shop_url, product_data):
    """
    Runs inside the Worker Process. 
    DISABLED: Odoo is Master. We ignore incoming Shopify product data.
    """
    # Simply return without doing anything. 
    # This keeps the worker queue fast and clean.
    return


# --- 2. UPDATED ROUTE: Product Webhook (Now uses Queue) ---
@app.route('/webhook/products/create', methods=['POST'])
@app.route('/webhook/products/update', methods=['POST'])
def product_webhook():
    # 1. Verify
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): 
        return "Unauthorized", 401
    
    # 2. Identify Shop
    shop_url = request.headers.get('X-Shopify-Shop-Domain')
    if not shop_url: return "Missing Shop Header", 400

    # 3. Queue the Job
    data = request.json
    
    # q_default.enqueue(background_product_sync, shop_url, data, job_timeout=300)
    
    return "Queued", 200


# --- 3. UPDATED ROUTE: Master Sync ---
@app.route('/sync/products/master', methods=['POST'])
@require_shopify_session
def trigger_master_sync():
    shop_url = request.args.get('shop') 
    if not shop_url: return jsonify({"message": "Error: Missing shop parameter"}), 400

    # SLOW LANE (This is the heavy one!)
    job = q_default.enqueue(sync_products_master, shop_url, job_timeout=1200)
    
    return jsonify({"message": f"Started Sync for {shop_url} (Job ID: {job.get_id()})"})

@app.route('/sync/customers/master', methods=['POST'])
@require_shopify_session
def trigger_customer_master_sync():
    shop_url = request.args.get('shop')
    
    # SLOW LANE
    job = q_default.enqueue(sync_customers_master, shop_url, job_timeout=3600)
    return jsonify({"message": f"Customer Sync Queued (ID: {job.get_id()})"})

@app.route('/sync/products/archive_duplicates', methods=['POST'])
def trigger_duplicate_scan():
    shop_url = request.args.get('shop')
    
    # SLOW LANE
    job = q_default.enqueue(archive_shopify_duplicates, shop_url, job_timeout=1200)
    return jsonify({"message": f"Duplicate Scan Queued (ID: {job.get_id()})"})
    

@app.route('/sync/orders/manual', methods=['GET'])
@require_shopify_session
def manual_order_fetch():
    shop_url = request.args.get('shop')
    
    # 1. Setup Connections
    if not setup_shopify_session(shop_url): 
        return jsonify({"orders": [], "error": "Auth Failed: Could not refresh session"})
    
    odoo = get_odoo_connection(shop_url)
    if not odoo:
        return jsonify({"orders": [], "error": "Could not connect to Odoo"})

    # --- FIX: Get Company ID ---
    company_id = get_config('odoo_company_id', shop_url=shop_url)
    
    # 2. Fetch Recent Shopify Orders
    try:
        orders = shopify.Order.find(limit=20, status='any')
    except Exception as e:
        return jsonify({"orders": [], "error": f"API Error: {str(e)}"})
    
    mapped_orders = []
    for o in orders:
        status = "Not Synced"
        try:
            # Smart Search Logic
            client_ref = f"ONLINE_{o.name}"
            plain_name = o.name
            
            # --- FIX: Strict Company Domain ---
            # Search for the order refs AND ensure it belongs to THIS company
            domain = [
                '|', '|', '|',
                ['client_order_ref', '=', client_ref],
                ['client_order_ref', '=', plain_name],
                ['origin', '=', client_ref],
                ['origin', '=', plain_name]
            ]
            
            if company_id:
                domain.append(['company_id', '=', int(company_id)])
            
            exists = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                'sale.order', 'search', [domain])
            
            if exists: status = "Synced"

        except Exception as e: 
            print(f"Check Error: {e}")
        
        if getattr(o, 'cancelled_at', None): status = "Cancelled"
        
        mapped_orders.append({
            'id': o.id, 
            'name': o.name, 
            'date': o.created_at, 
            'total': o.total_price, 
            'odoo_status': status
        })
        
    return jsonify({"orders": mapped_orders})
    

from urllib.parse import urlparse, parse_qs # Import at top if missing

@app.route('/sync/orders/import_batch', methods=['POST'])
@require_shopify_session
def import_selected_orders():
    # 1. Get data from JSON body
    data = request.json
    ids = data.get('order_ids', [])
    
    # --- ROBUST SHOP RETRIEVAL ---
    # 1. Try URL Query Param
    shop_url = request.args.get('shop')
    
    # 2. Try JSON Body
    if not shop_url and data:
        shop_url = data.get('shop') or data.get('shop_url')

    # 3. Try Referer Header (The "Current Page" URL)
    if not shop_url:
        referer = request.headers.get("Referer")
        if referer:
            try:
                parsed = urlparse(referer)
                params = parse_qs(parsed.query)
                if 'shop' in params:
                    shop_url = params['shop'][0]
            except: pass

    # 4. Fail if still missing
    if not shop_url:
        return jsonify({"message": "Error: Missing shop parameter. Please reload the page."}), 400

    # 5. Setup Shopify Session
    if not setup_shopify_session(shop_url):
        return jsonify({"message": "Auth Failed: Could not refresh session"}), 401

    odoo = get_odoo_connection(shop_url)
    if not odoo:
        return jsonify({"message": "Odoo connection failed"}), 500

    synced = 0
    log_event('System', 'Info', f"Manual Trigger: Importing {len(ids)} orders...", shop_url=shop_url)

    for oid in ids:
        try:
            # 6. Fetch Order
            order = shopify.Order.find(oid)
            
            # 7. Process
            result = process_order_data(order.to_dict(), odoo, shop_url=shop_url)
            
            if isinstance(result, tuple):
                success = result[0]
            else:
                success = result
                
            if success: synced += 1
            
        except Exception as e:
            print(f"Error importing order {oid}: {e}")
            log_event('System', 'Error', f"Import failed for order {oid}: {str(e)}", shop_url=shop_url)

    return jsonify({"message": f"Batch Complete. Synced: {synced}"})


@app.route('/sync/status')
def get_sync_status():
    from utils import q_default
    
    # Get counts from the Redis queue
    queued_count = q_default.count  # Tasks waiting to start
    started_jobs = q_default.started_job_registry.count # Tasks currently running
    failed_count = q_default.failed_job_registry.count # Tasks that crashed
    
    return jsonify({
        "status": "Online",
        "pending_batches": queued_count,
        "active_workers": started_jobs,
        "failed_tasks": failed_count,
        "queue_name": q_default.name
    })


def background_refund_sync(shop_url, refund_data):
    with app.app_context():
        success, message = process_refund_data(refund_data, shop_url)
        if success:
            log_event('Refund', 'Success', message, shop_url=shop_url)
        else:
            log_event('Refund', 'Error', message, shop_url=shop_url)
            

def background_order_sync(shop_url, order_data):
    """
    Runs inside the Worker Process.
    """
    with app.app_context():
        # 1. Connect
        odoo = get_odoo_connection(shop_url)
        if not odoo:
            log_event('Order', 'Error', "Auto Sync Failed: Could not connect to Odoo.", shop_url=shop_url)
            return

        # 2. Sync with CRASH PROTECTION
        try:
            # FIX: Passed shop_url explicitly so get_config works inside the helper
            result = process_order_data(order_data, odoo, shop_url=shop_url)
            
            # Handle tuple return (success, message) vs boolean (True/False)
            if isinstance(result, tuple):
                success, msg = result
            else:
                success, msg = result, "Processed"
            
            # 3. Log Result
            if success:
                 if "Synced" in msg:
                     log_event('Order', 'Success', f"Auto Sync: {msg}", shop_url=shop_url)
                     
                     current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
                     set_config('last_order_sync_success', current_time, shop_url=shop_url)
                     
                 elif "Skipped" in msg:
                     pass 
            else:
                 log_event('Order', 'Error', f"Auto Sync Failed: {msg}", shop_url=shop_url)

        except Exception as e:
            log_event('Order', 'Error', f"Worker Crash: {str(e)}", shop_url=shop_url)
    

@app.route('/webhook/orders', methods=['POST'])
@app.route('/webhook/orders/updated', methods=['POST'])
@app.route('/webhook/orders/cancelled', methods=['POST'])
def order_webhook():
    # 1. Security Check
    hmac_header = request.headers.get('X-Shopify-Hmac-Sha256')
    if not verify_shopify(request.get_data(), hmac_header):
        return "Unauthorized", 401

    # 2. Extract Info
    topic = request.headers.get('X-Shopify-Topic', '')
    shop_url = request.headers.get('X-Shopify-Shop-Domain')
    data = request.json
    topic = request.headers.get('X-Shopify-Topic', '')
    order_num = data.get('name')

    # 3. Handle Cancellation
    if topic == 'orders/cancelled':
        # FAST LANE
        q_critical.enqueue(
            process_cancellation, 
            data, 
            shop_url,
            retry=Retry(max=5, interval=[60, 120, 240, 600, 1200])
        ) 
        return "Cancellation Queued", 200

   # 4. QUEUE THE SYNC
    if topic in ['orders/create', 'orders/paid', 'orders/updated']:
        # FAST LANE
        q_critical.enqueue(
            background_order_sync, 
            shop_url, 
            data, 
            job_timeout=300,
            retry=Retry(max=5, interval=[60, 120, 240, 600, 1200])
        )
        return "Queued", 200

    return "Ignored", 200

@app.route('/webhook/refunds', methods=['POST'])
def refund_webhook():
    # ... (Keep auth logic) ...
    shop_url = request.headers.get('X-Shopify-Shop-Domain')
    data = request.json

    # FAST LANE
    q_critical.enqueue(
        background_refund_sync, 
        shop_url, 
        data, 
        job_timeout=300,
        retry=Retry(max=5, interval=[60, 120, 240, 600, 1200])
    )
    
    return "Refund Received & Queued", 200

@app.route('/test/simulate_order', methods=['POST'])
def test_sim_dummy():
     shop_url = request.args.get('shop')
     log_event('System', 'Success', "Test Connection Successful (Logs Working)", shop_url=shop_url)
     return jsonify({"message": "Connection OK - Check Live Logs tab."})

# --- API: Fetch Companies (Dynamic & Secured) ---
@app.route('/api/odoo/companies')
@require_shopify_session # <--- Security Gate
def api_get_companies():
    shop_url = request.args.get('shop')
    
    # Connect dynamically using your utility
    odoo = get_odoo_connection(shop_url)
    
    if not odoo:
        # We return a 401 so the frontend knows it's an Auth/Connection issue
        return jsonify({
            'error': 'Connection Failed. Please check your Odoo URL and Credentials.'
        }), 401

    try:
        # Use the client's built-in helper method
        companies = odoo.get_companies()
        return jsonify(companies)
    except Exception as e:
        # Log the actual error for you, return a clean message for the user
        print(f"Odoo Company Fetch Error for {shop_url}: {e}")
        return jsonify({'error': 'Odoo server is reachable, but failed to fetch company list.'}), 500

# --- API: Fetch Locations (Dynamic) ---
@app.route('/api/odoo/locations')
def api_get_locations():
    shop_url = request.args.get('shop')
    company_id = request.args.get('company_id')
    
    if not shop_url: return jsonify({'error': 'Missing shop param'})

    # Connect dynamically
    odoo = get_odoo_connection(shop_url)
    if not odoo: return jsonify({'error': 'Could not connect to Odoo'})

    try:
        # FIX: Use the client's built-in helper method
        locs = odoo.get_locations(company_id=company_id)
        return jsonify(locs)
    except Exception as e:
        return jsonify({'error': str(e)})



@app.route('/settings/save', methods=['POST'])
@app.route('/api/settings/save', methods=['POST'])
@app.route('/save_settings', methods=['POST'])
@require_shopify_session
def save_settings():
    # 1. Get Data FIRST so we can check it for the shop URL
    data = request.get_json(silent=True)
    if not data:
        data = request.form.to_dict()
    
    if not data: 
        return jsonify({"message": "Error: No data provided"}), 400

    # 2. ROBUST SHOP RETRIEVAL
    # Try URL first, then fall back to the data body (where the HTML form puts it)
    shop_url = request.args.get('shop')
    if not shop_url:
        shop_url = data.get('shop_url') or data.get('shop')

    if not shop_url:
        return jsonify({"message": "Error: Missing shop parameter"}), 400

    try:
        # 3. Query Database
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop: 
            return jsonify({"message": f"Shop {shop_url} not found"}), 404

        # 4. Save Core Fields
        if 'odoo_company_id' in data: shop.odoo_company_id = int(data['odoo_company_id'])
        if 'company_id' in data: shop.odoo_company_id = int(data['company_id'])
        if 'odoo_url' in data: shop.odoo_url = data['odoo_url']
        if 'odoo_db' in data: shop.odoo_db = data['odoo_db']
        if 'odoo_user' in data: shop.odoo_username = data['odoo_user']
        # Only update password if user typed one (HTML forms send empty string if untouched)
        if data.get('odoo_pass') and data['odoo_pass'].strip(): 
            shop.odoo_password = data['odoo_pass']
            
        if 'sync_start_date' in data: shop.sync_start_date = data['sync_start_date']

        # 5. Save App Settings
        ignore_fields = ['odoo_company_id', 'company_id', 'sync_start_date', 'shop', 'shop_url', 'hmac', 'timestamp', 'odoo_url', 'odoo_db', 'odoo_user', 'odoo_pass']
        
        # --- ROBUST BOOLEAN HANDLING ---
        # Explicitly list keys that are checkboxes (True/False)
        boolean_keys = [
            'prod_sync_title', 'prod_sync_price', 'prod_sync_cost', 
            'prod_sync_desc', 'prod_sync_tags', 'prod_sync_images', 
            'prod_sync_vendor', 'prod_sync_barcode', 
            'prod_auto_create', 'prod_auto_publish',  # <--- CRITICAL ONES
            'prod_sync_meta_original_price', 'prod_sync_meta_vendor_code'
        ]

        # Check if we are saving Product Settings (detect by checking if one exists)
        # This prevents overwriting settings if we are saving a different form (like Customers)
        if 'prod_sync_price' in data or 'prod_auto_create' in data:
             for b_key in boolean_keys:
                 if b_key not in data:
                     # If missing from form, user unchecked it -> Set to False
                     data[b_key] = False  
                 else:
                     # If present, convert "on"/"true"/"1" string to Python Boolean
                     raw = str(data[b_key]).lower()
                     data[b_key] = raw in ['true', 'on', '1', 'yes']

        for key, value in data.items():
            if key in ignore_fields: continue
            
            # Save as JSON string
            val_str = json.dumps(value) 
            
            setting = AppSetting.query.filter_by(shop_url=shop_url, key=key).first()
            if setting: setting.value = val_str
            else: db.session.add(AppSetting(shop_url=shop_url, key=key, value=val_str))

        db.session.commit()
        
        automate_webhook_registration(shop_url)
        log_event('Settings', 'Success', 'Configuration saved successfully', shop_url=shop_url)
        
        # --- FIXED RESPONSE LOGIC ---
        # 1. If the REQUEST was JSON (Dashboard), return JSON
        if request.is_json:
             return jsonify({"success": True, "message": "Configuration Saved"})
        
        # 2. Otherwise, it was a standard HTML Form (Connect Page), so Redirect
        return f"✅ Settings Saved! <script>window.location.href='/?shop={shop_url}';</script>"

    except Exception as e:
        db.session.rollback()
        return jsonify({"success": False, "message": str(e)}), 500
        

@app.route('/test/odoo_health', methods=['GET'])
@require_shopify_session
def test_odoo_health():
    shop_url = request.args.get('shop')
    if not shop_url: return "Missing shop param"
    
    import time
    log = []
    
    def add_log(msg):
        print(msg)
        log.append(msg + "<br>")

    add_log(f"--- DIAGNOSTIC START FOR {shop_url} ---")
    
    try:
        # 1. Test Connection & Auth
        start = time.time()
        odoo = get_odoo_connection(shop_url)
        if not odoo:
            return "FAILED: Could not authenticate (Check credentials)."
        auth_time = round(time.time() - start, 2)
        add_log(f"✅ Authentication successful ({auth_time}s)")
        
        # 2. Test Version
        version = odoo.common.version()
        add_log(f"✅ Odoo Ping: Alive (Version: {version.get('server_version')})")

        # 3. Test Search
        ids = odoo.models.execute_kw(
            odoo.db, odoo.uid, odoo.password,
            'product.product', 'search', [[['active', '=', True]]], {'limit': 1}
        )
        if ids:
            add_log(f"✅ DB Read successful (Found ID: {ids[0]})")

        # --- NEW: AUTO-REFRESH LOCATIONS DURING DIAGNOSTIC ---
        add_log("<b>Refreshing Warehouse Locations...</b>")
        odoo_locations = odoo.models.execute_kw(
            odoo.db, odoo.uid, odoo.password,
            'stock.location', 'search_read',
            [[['usage', '=', 'internal']]],
            {'fields': ['complete_name', 'usage']}
        )
        
        new_loc_list = [{"id": loc['id'], "name": loc['complete_name'], "type": loc['usage']} for loc in odoo_locations]
        
        # Update Database Config (Fixed for AppSetting)
        # We just use the helper function you already wrote!
        import json
        
        # set_config handles looking up the row, creating it if missing, 
        # and converting the list to JSON automatically.
        set_config('available_locations', new_loc_list, shop_url=shop_url)
        
        # If you prefer manual SQLalchemy for some reason:
        # setting = AppSetting.query.filter_by(shop_url=shop_url, key='available_locations').first()
        # val_str = json.dumps(new_loc_list)
        # if setting:
        #     setting.value = val_str
        # else:
        #     db.session.add(AppSetting(shop_url=shop_url, key='available_locations', value=val_str))
        
        db.session.commit()
        add_log(f"✅ Successfully refreshed {len(new_loc_list)} locations in database.")
        # ----------------------------------------------------

        add_log("--- DIAGNOSTIC PASSED ---")
        add_log("<i>You can now go back to settings to see WPW01/PICK</i>")
        return "".join(log)

    except Exception as e:
        add_log(f"❌ CRITICAL FAILURE: {str(e)}")
        return "".join(log)



# 1. Define Cleanup Function FIRST
def cleanup_old_logs():
    """Deletes logs older than 14 days to keep DB light."""
    with app.app_context():
        cutoff = datetime.utcnow() - timedelta(days=14)
        try:
            deleted = SyncLog.query.filter(SyncLog.timestamp < cutoff).delete()
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            print(f"Maintenance Error: {e}")

# --- SCHEDULER LOGIC (Runs in Clock Process) ---
def run_schedule():
    """
    Robust Scheduler: Uses Redis keys to track job timing.
    Includes Log Flusher (10m Interval) to aggregate webhook logs.
    """
    print("🕒 Scheduler Started")
    
    while True:
        with app.app_context():
            # 1. Fetch all active shops
            active_shops = Shop.query.filter_by(is_active=True).all()
            
            for shop in active_shops:
                shop_url = shop.shop_url
                
                # --- LOG FLUSHER (Runs every 10 mins = 600s) ---
                if not conn.get(f"last_log_flush_{shop_url}"):
                    try:
                        count_key = f"log_buffer_products_{shop_url}"
                        pending_count = conn.get(count_key)
                        
                        if pending_count and int(pending_count) > 0:
                            log_event('Product', 'Success', f"Webhook Batch: Updated {int(pending_count)} products in the last 10 minutes.", shop_url=shop_url)
                            conn.delete(count_key)
                        
                        conn.setex(f"last_log_flush_{shop_url}", 600, "done")
                        
                    except Exception as e:
                        print(f"Log Flush Error: {e}")

                # --- A. HIGH FREQUENCY TASKS ---
                
                # 1. Inventory Sync (Every 30 mins)
                if not conn.get(f"last_inv_{shop_url}"):
                    # CRITICAL LANE (Fast)
                    q_critical.enqueue(scheduled_inventory_sync, shop_url, job_timeout=3600)
                    conn.setex(f"last_inv_{shop_url}", 1800, "done") 
                    print(f"⏰ Triggered Inventory Sync for {shop_url}")

                # 2. Fulfillment Sync
                if not conn.get(f"last_ful_{shop_url}"):
                    # CRITICAL LANE
                    q_critical.enqueue(sync_odoo_fulfillments, shop_url, job_timeout=600)
                    conn.setex(f"last_ful_{shop_url}", 3600, "done")
                    
                # 3. Cancellation Sync
                if not conn.get(f"last_cancel_{shop_url}"):
                    # CRITICAL LANE
                    q_critical.enqueue(sync_odoo_cancellations, shop_url, job_timeout=600)
                    conn.setex(f"last_cancel_{shop_url}", 300, "done")

                # 4. Return Sync
                if not conn.get(f"last_ret_sync_{shop_url}"):
                    # CRITICAL LANE
                    q_critical.enqueue(sync_odoo_returns, shop_url, job_timeout=600)
                    conn.setex(f"last_ret_sync_{shop_url}", 3600, "done")

                # --- B. DAILY TASKS (24 Hours = 86400s) ---
                
               # 5. Master Customer Sync
                if not conn.get(f"last_cust_sync_{shop_url}"):
                    # SLOW LANE
                    q_default.enqueue(sync_customers_master, shop_url, job_timeout=1200)
                    conn.setex(f"last_cust_sync_{shop_url}", 86400, "done")

                # 6. Master Product Sync
                if not conn.get(f"last_prod_sync_{shop_url}"):
                    # SLOW LANE
                    q_default.enqueue(sync_products_master, shop_url, job_timeout=3600)
                    conn.setex(f"last_prod_sync_{shop_url}", 86400, "done")

            # --- C. GLOBAL MAINTENANCE ---
            
            # 7. Cleanup Logs (Once per day)
            if not conn.get("last_log_cleanup"):
                cleanup_old_logs()
                conn.setex("last_log_cleanup", 86400, "done") 

        # Heartbeat sleep
        time.sleep(10)

def sync_images_only_manual(shop_url):
    """
    OPTIMIZED Image Sync: Groups variants by Template.
    FIX: Now correctly fetches Company ID from the Shop table.
    """
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): return
        
        # FIX 1: Fetch Company ID directly from Shop Table (skipping get_config helper)
        shop_record = Shop.query.filter_by(shop_url=shop_url).first()
        company_id = shop_record.odoo_company_id if shop_record else None
        
        log_event('Image Sync', 'Info', f"Starting Optimized Image Sync for Company ID: {company_id}...", shop_url=shop_url)
        
        # FIX 2: Strict Domain
        domain = [['type', 'in', ['product', 'consu']], ['active', '=', True], ['sale_ok', '=', True]]
        if company_id: domain.append(['company_id', '=', int(company_id)])
        
        # LOG THE DOMAIN (So we can debug if it happens again)
        print(f"DEBUG: Image Sync Domain: {domain}")

        try:
            # Get all IDs
            all_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'search', [domain])
        except Exception as e:
            log_event('Image Sync', 'Error', f"Odoo Search Failed: {e}", shop_url=shop_url)
            return

        total_variants = len(all_ids)
        log_event('Image Sync', 'Info', f"Scanned {total_variants} sellable products. Grouping by Template...", shop_url=shop_url)

        # 2. Process in Batches to Group by Template
        BATCH_SIZE = 500  # Larger batch for metadata
        unique_templates = {} # Map: tmpl_id -> {sku, id}
        
        for i in range(0, total_variants, BATCH_SIZE):
            batch_ids = all_ids[i:i + BATCH_SIZE]
            try:
                # Fetch minimal data to group them
                data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                    'product.product', 'read', [batch_ids], {'fields': ['default_code', 'product_tmpl_id']})
                
                for p in data:
                    sku = p.get('default_code')
                    if not sku: continue
                    
                    # Group by Template ID
                    tmpl_id = p['product_tmpl_id'][0] if p.get('product_tmpl_id') else p['id']
                    if tmpl_id not in unique_templates:
                        unique_templates[tmpl_id] = p['id'] # Store the Product ID to fetch image later
            except: continue

        real_count = len(unique_templates)
        log_event('Image Sync', 'Info', f"Consolidated to {real_count} Unique Products. Downloading images...", shop_url=shop_url)

        # 3. Download & Sync Images for Unique Templates ONLY
        processed = 0
        updates = 0
        final_ids = list(unique_templates.values())
        
        # Process images in small batches (20) to prevent "IncompleteRead"
        IMG_BATCH = 20
        for i in range(0, len(final_ids), IMG_BATCH):
            chunk = final_ids[i:i + IMG_BATCH]
            try:
                prod_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                    'product.product', 'read', [chunk], {'fields': ['default_code', 'image_1920']})
            except: continue

            for p in prod_data:
                sku = p.get('default_code')
                img_b64 = p.get('image_1920')
                if not sku or not img_b64: continue

                # Decode & Clean
                if isinstance(img_b64, bytes): img_str = img_b64.decode('utf-8')
                else: img_str = img_b64
                img_str = img_str.replace("\n", "")

                # Check Hash
                current_hash = hashlib.md5(img_str.encode('utf-8')).hexdigest()
                pm = ProductMap.query.filter_by(sku=sku).first()
                if pm and pm.image_hash == current_hash: continue

                # Sync to Shopify
                sid = find_shopify_product_by_sku(sku, shop_url=shop_url)
                if sid:
                    try:
                        sp = shopify.Product.find(sid)
                        image = shopify.Image(prefix_options={'product_id': sp.id})
                        image.attachment = img_str
                        sp.images = [image]
                        sp.save()

                        if not pm:
                            pm = ProductMap(sku=sku, odoo_product_id=p['id'], shopify_variant_id='0')
                            db.session.add(pm)
                        pm.image_hash = current_hash
                        db.session.commit()
                        updates += 1
                    except: db.session.rollback()
            
            processed += len(chunk)
            if processed % 50 == 0:
                log_event('Image Sync', 'Info', f"Synced images for {processed}/{real_count} products...", shop_url=shop_url)

        log_event('Image Sync', 'Success', f"Done. Updated {updates} images.", shop_url=shop_url)

def emergency_purge_junk_products(shop_url):
    """
    EMERGENCY TOOL: Destroys ACTIVE products in Shopify not found in Odoo.
    UPDATED: Now ignores 'Archived' and 'Draft' products to protect history.
    """
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): 
            log_event('Cleanup', 'Error', "Connection failed. Aborting.")
            return

        company_id = get_config('odoo_company_id', shop_url=shop_url)
        if not company_id:
            log_event('Cleanup', 'Error', "No Company ID set. Aborting.")
            return

        log_event('Cleanup', 'Info', f"Fetching valid SKUs for Company {company_id}...")
        
        domain = [
            ['type', 'in', ['product', 'consu']],
            ['company_id', '=', int(company_id)]
        ]
        
        try:
            valid_products = odoo.models.execute_kw(
                odoo.db, odoo.uid, odoo.password,
                'product.product', 'search_read', [domain], 
                {'fields': ['default_code']}
            )
        except Exception as e:
            log_event('Cleanup', 'Error', f"Odoo Error: {e}")
            return

        valid_skus = set()
        for p in valid_products:
            if p.get('default_code'):
                valid_skus.add(p['default_code'])
                valid_skus.add(f"{p['default_code']}-UNIT")

        if len(valid_skus) < 5:
            log_event('Cleanup', 'Error', "Safety Stop: Too few products found. Aborting.")
            return

        log_event('Cleanup', 'Info', f"Found {len(valid_skus)} valid SKUs. Scanning Active Shopify Products...")

        # FIX: Added status='active' to protect Archived/Draft items
        page = shopify.Product.find(limit=250, status='active')
        deleted_count = 0
        
        while page:
            for sp in page:
                sku = sp.variants[0].sku if sp.variants else None
                
                # If SKU is missing or NOT in our valid list, delete it
                if not sku or sku not in valid_skus:
                    try:
                        sp.destroy()
                        deleted_count += 1
                        if deleted_count % 50 == 0:
                            log_event('Cleanup', 'Warning', f"Purged {deleted_count} active junk products...")
                    except Exception as e:
                        print(f"Failed to delete {sp.id}: {e}")
            
            if page.has_next_page(): page = page.next_page()
            else: break
        
        log_event('Cleanup', 'Success', f"Purge Complete. Deleted {deleted_count} active junk products.")

def check_for_corrupted_categories(shop_url):
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo: 
            log_event('Diagnostic', 'Error', "No Odoo connection.")
            return

        log_event('Diagnostic', 'Info', "--- STARTING SCAN ---")
        try:
            cats = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                'pos.category', 'search_read', [[]], {'fields': ['id', 'name', 'parent_id']}
            )
        except Exception as e:
            log_event('Diagnostic', 'Error', f"Scan failed: {e}")
            return

        found_issues = 0
        for c in cats:
            c_id = c.get('id')
            c_name = c.get('name')
            
            if not c_name or str(c_name) == 'False':
                log_event('Diagnostic', 'Error', f"CORRUPTED: ID {c_id} has NO NAME.")
                found_issues += 1
            
            parent = c.get('parent_id')
            if parent and (not parent[1] or str(parent[1]) == 'False'):
                log_event('Diagnostic', 'Error', f"CORRUPTED PARENT: Category '{c_name}' (ID {c_id})")
                found_issues += 1

        if found_issues == 0:
            log_event('Diagnostic', 'Success', "✅ No corruption found in POS Categories.")
        else:
            log_event('Diagnostic', 'Warning', f"❌ Found {found_issues} corrupted records.")

def fix_variant_mess_task(shop_url):
    with app.app_context():
        # 1. Setup
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): 
            log_event('Cleanup', 'Error', "Startup Failed: No Odoo/Shopify Connection", shop_url=shop_url)
            return
        
        company_id = get_config('odoo_company_id', shop_url=shop_url)
        log_event('Cleanup', 'Info', f"Starting Strict Variant Repair for Company {company_id}...", shop_url=shop_url)
        
        # 2. Fetch Odoo Data (Including Cost & Pack Qty)
        try:
            domain = [['sale_ok', '=', True], ['type', 'in', ['product', 'consu']]]
            if company_id: domain.append(['company_id', '=', int(company_id)])
            
            fields = ['default_code', 'name', 'list_price', 'standard_price', 'sh_is_secondary_unit', 'qty_per_pack']
            odoo_products = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'search_read', [domain], {'fields': fields})
            
            odoo_map = {p.get('default_code'): p for p in odoo_products if p.get('default_code')}
        except Exception as e:
            log_event('Cleanup', 'Error', f"Odoo Data Fetch Failed: {e}", shop_url=shop_url)
            return

        # 3. Scan Shopify Products
        page = 1
        processed = 0
        repaired = 0
        
        while True:
            try:
                products = shopify.Product.find(limit=50, page=page)
            except: break
            
            if not products: break
            
            for sp in products:
                # Identify Parent SKU
                ref_sku = sp.variants[0].sku if sp.variants else None
                if ref_sku and "-UNIT" in ref_sku: ref_sku = ref_sku.replace("-UNIT", "")
                
                if not ref_sku or ref_sku not in odoo_map:
                    continue

                p = odoo_map[ref_sku]
                
                # --- RULE 1: DETERMINE IF PACK ---
                is_pack = False
                qty_pack = float(p.get('qty_per_pack', 0.0))
                
                if p.get('sh_is_secondary_unit') is True and qty_pack > 1.0:
                    is_pack = True

                # --- RULE 2: CALCULATE PRICES ---
                pack_price = float(p.get('list_price', 0.0))
                pack_cost = float(p.get('standard_price', 0.0))
                
                unit_price = 0.0
                unit_cost = 0.0
                
                if is_pack:
                    unit_price = round(pack_price / qty_pack, 2)
                    unit_cost = round(pack_cost / qty_pack, 2)

                # --- RULE 3: DEFINE DESIRED VARIANTS ---
                desired_variants = []

                if is_pack:
                    # Enforce Option Name
                    sp.options = [{'name': 'Pack Size'}]
                    
                    # 1. Primary (The Pack)
                    desired_variants.append({
                        'sku': ref_sku,
                        'option1': f"{int(qty_pack)} per pack", # Primary Name
                        'price': str(pack_price),
                        'cost': str(pack_cost)
                    })
                    
                    # 2. Secondary (The Unit)
                    desired_variants.append({
                        'sku': f"{ref_sku}-UNIT",
                        'option1': "Unit",
                        'price': str(unit_price),
                        'cost': str(unit_cost)
                    })
                else:
                    # Not a pack? Reset to simple product
                    if sp.options and sp.options[0].name != 'Title':
                        sp.options = [{'name': 'Title', 'values': ['Default Title']}]
                    
                    desired_variants.append({
                        'sku': ref_sku,
                        'option1': "Default Title",
                        'price': str(pack_price),
                        'cost': str(pack_cost)
                    })

                # --- RULE 4: SYNC & CLEANUP ---
                current_variants = sp.variants
                final_list = []
                dirty = False

                for target in desired_variants:
                    # Find existing variant by SKU
                    match = next((v for v in current_variants if v.sku == target['sku']), None)
                    
                    # Or fallback to first variant if we are resetting to "Default Title"
                    if not match and target['option1'] == 'Default Title' and len(current_variants) > 0:
                        match = current_variants[0]

                    if not match:
                        # Create New
                        print(f"Repair: Creating missing variant {target['sku']}")
                        match = shopify.Variant({'product_id': sp.id})
                        match.inventory_management = 'shopify'
                        dirty = True

                    # Update Values
                    if match.option1 != target['option1']:
                        match.option1 = target['option1']
                        dirty = True
                    if match.price != target['price']:
                        match.price = target['price']
                        dirty = True
                    if match.sku != target['sku']:
                        match.sku = target['sku']
                        dirty = True
                    
                    final_list.append(match)

                # --- RULE 5: DETECT DELETIONS ---
                # If current list has more items than final list, we are deleting junk.
                if len(current_variants) > len(final_list):
                    print(f"Repair: Deleting {len(current_variants) - len(final_list)} junk variants for {ref_sku}")
                    dirty = True
                
                if dirty:
                    try:
                        sp.variants = final_list
                        sp.save()
                        
                        # Update Costs (Requires InventoryItem API)
                        for v in sp.variants:
                            target = next((t for t in desired_variants if t['sku'] == v.sku), None)
                            if target and v.inventory_item_id:
                                try:
                                    ii = shopify.InventoryItem.find(v.inventory_item_id)
                                    ii.cost = target['cost']
                                    ii.save()
                                except: pass
                                
                        repaired += 1
                    except Exception as e:
                        print(f"Save Failed for {ref_sku}: {e}")

                processed += 1
            
            page += 1
            log_event('Cleanup', 'Info', f"Scanned {processed} products, Repaired {repaired}...", shop_url=shop_url)

        log_event('Cleanup', 'Success', f"Done. Scanned {processed}, Repaired {repaired}.", shop_url=shop_url)


# --- GDPR WEBHOOKS ---
@app.route('/gdpr/customers/data_request', methods=['POST'])
def gdpr_customer_data_request():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')):
        return "Unauthorized", 401
    # You are supposed to email the merchant data here. 
    # For now, acknowledge receipt.
    return "Acknowledged", 200

@app.route('/gdpr/customers/redact', methods=['POST'])
def gdpr_customer_redact():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')):
        return "Unauthorized", 401
    
    # Logic: Remove customer mapping from your DB if it exists
    try:
        data = request.json
        shop_url = request.headers.get('X-Shopify-Shop-Domain')
        shopify_customer_id = str(data.get('customer', {}).get('id'))
        
        with app.app_context():
            CustomerMap.query.filter_by(shop_url=shop_url, shopify_customer_id=shopify_customer_id).delete()
            db.session.commit()
    except: pass
    
    return "Acknowledged", 200

@app.route('/gdpr/shop/redact', methods=['POST'])
def gdpr_shop_redact():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')):
        return "Unauthorized", 401
    
    # Logic: Mark shop as inactive or delete config
    try:
        shop_url = request.headers.get('X-Shopify-Shop-Domain')
        with app.app_context():
            shop = Shop.query.filter_by(shop_url=shop_url).first()
            if shop:
                shop.is_active = False # Soft delete
                db.session.commit()
    except: pass

    return "Acknowledged", 200

@app.route('/api/diagnostics/unmapped_products', methods=['GET'])
def api_get_unmapped_products():
    shop_url = request.args.get('shop')
    
    # --- FIX START ---
    # We use setup_shopify_session (which exists), NOT verify_shopify_session (which does not).
    if not setup_shopify_session(shop_url): 
        return jsonify({'error': 'Unauthorized: Could not connect to Shopify'}), 401
    # --- FIX END ---

    try:
        with app.app_context():
            # 1. Get list of ALREADY mapped Odoo IDs to ignore
            mapped_records = ProductMap.query.filter(ProductMap.shop_url == shop_url, ProductMap.odoo_product_id > 0).all()
            mapped_skus = {m.sku for m in mapped_records}

            # 2. Fetch all Shopify products
            unmapped_items = []
            page = shopify.Product.find(limit=250, fields="id,title,variants,images")
            
            while page:
                for p in page:
                    image_url = p.images[0].src if p.images else ""
                    for v in p.variants:
                        # If variant has a SKU, but that SKU is NOT in our database...
                        if v.sku and v.sku not in mapped_skus:
                            unmapped_items.append({
                                'shopify_id': p.id,
                                'title': p.title,
                                'variant_title': v.title,
                                'sku': v.sku,
                                'image': image_url
                            })
                
                if page.has_next_page():
                    page = page.next_page()
                else:
                    break

            return jsonify({
                'count': len(unmapped_items), 
                'items': unmapped_items
            })

    except Exception as e:
        print(f"CRITICAL DIAGNOSTIC ERROR: {str(e)}")
        return jsonify({'error': str(e)}), 500
        print(f"CRITICAL DIAGNOSTIC ERROR: {str(e)}")
        return jsonify({'error': str(e)}), 500
        

@app.route('/api/diagnose/<sku>', methods=['GET'])
@require_shopify_session
def api_diagnose_product(sku):
    shop_url = request.args.get('shop')
    
    # Capture output
    import io, sys
    from contextlib import redirect_stdout
    
    f = io.StringIO()
    with redirect_stdout(f):
        # We manually run the logic from diagnose.py here
        print(f"--- DIAGNOSTIC FOR {sku} ---")
        
        # 1. DB Map
        pm = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
        if pm: print(f"✅ DB MAP: Found (Odoo ID: {pm.odoo_product_id})")
        else: print("❌ DB MAP: Not found")
        
        # 2. Odoo
        odoo = get_odoo_connection(shop_url)
        if odoo:
            p = odoo.search_product_by_sku(sku)
            if p: print(f"✅ ODOO: Found Product ID {p}")
            else: print("❌ ODOO: Not found")
            
    return f.getvalue().replace('\n', '<br>')


@app.route('/api/jobs/<status>', methods=['GET'])
@require_shopify_session
def api_inspect_jobs(status):
    """
    Returns JSON data for the requested job state (failed, active, pending).
    Limits to 50 items to prevent UI crashes.
    """
    from rq.registry import FailedJobRegistry, StartedJobRegistry
    from utils import q_default, q_critical
    
    data = []
    
    # Check both queues
    for q_name, queue in [('Critical (Fast)', q_critical), ('Default (Slow)', q_default)]:
        job_ids = []
        
        if status == 'failed':
            registry = FailedJobRegistry(queue=queue)
            job_ids = registry.get_job_ids()
        elif status == 'active':
            registry = StartedJobRegistry(queue=queue)
            job_ids = registry.get_job_ids()
        elif status == 'pending':
            job_ids = queue.get_job_ids()
            
        # --- SORTING LOGIC ---
        # 1. Pending: Keep standard order (Oldest/Next-to-run first)
        # 2. Failed/Active: Reverse to show Newest first
        target_ids = job_ids[:50] 
        if status in ['failed', 'active']:
            target_ids = job_ids[-50:] # Grab the LAST 50 (Newest)
            target_ids.reverse()       # Flip them so Newest is at top

        for jid in target_ids:
            try:
                job = queue.fetch_job(jid)
                if not job: continue
                
                # Format arguments nicely
                args_display = str(job.args)
                if len(args_display) > 50: args_display = args_display[:50] + "..."
                
                # Format error (if any)
                error_msg = ""
                if job.exc_info:
                    # Get the last line of the traceback (usually the actual error)
                    error_msg = job.exc_info.strip().split('\n')[-1]

                data.append({
                    'id': job.id,
                    'queue': q_name,
                    'function': job.func_name.split('.')[-1], # Show only function name
                    'args': args_display,
                    'created_at': job.created_at.strftime('%H:%M:%S') if job.created_at else '--',
                    'error': error_msg
                })
            except: pass

    return jsonify({'jobs': data, 'count': len(data)})


@app.route('/api/jobs/<action>/<job_id>', methods=['POST'])
@require_shopify_session
def api_job_action(action, job_id):
    from rq.job import Job
    from utils import conn
    from rq.registry import FailedJobRegistry
    
    try:
        job = Job.fetch(job_id, connection=conn)
    except:
        return jsonify({'success': False, 'message': 'Job not found'})

    try:
        if action == 'retry':
            # Retry a failed job
            job.requeue()
            return jsonify({'success': True, 'message': 'Job Requeued'})
            
        elif action == 'cancel':
            # Delete a pending or failed job
            job.delete()
            return jsonify({'success': True, 'message': 'Job Cancelled/Deleted'})
            
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)})
        
    return jsonify({'success': False, 'message': 'Invalid Action'})

# Start scheduler thread
# t = threading.Thread(target=run_schedule, daemon=True)
# t.start()
    
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
