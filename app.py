import os
import hmac
import hashlib
import base64
import json
import threading
import schedule
import time
import shopify 
from flask import Flask, request, jsonify, render_template
from models import db, ProductMap, SyncLog, AppSetting, CustomerMap
from odoo_client import OdooClient
import requests
from datetime import datetime, timedelta
import random
import xmlrpc.client

app = Flask(__name__)

# --- CONFIGURATION ---
database_url = os.getenv('DATABASE_URL', 'sqlite:///local.db')
if database_url:
    if database_url.startswith("postgres://"):
        database_url = database_url.replace("postgres://", "postgresql+pg8000://", 1)
    elif database_url.startswith("postgresql://"):
        database_url = database_url.replace("postgresql://", "postgresql+pg8000://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = database_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

SHOPIFY_LOCATION_ID = int(os.getenv('SHOPIFY_WAREHOUSE_ID', '0'))

db.init_app(app)

odoo = None
try:
    odoo = OdooClient(
        url=os.getenv('ODOO_URL'),
        db=os.getenv('ODOO_DB'),
        username=os.getenv('ODOO_USERNAME'),
        password=os.getenv('ODOO_PASSWORD')
    )
except Exception as e:
    print(f"Odoo Startup Error: {e}")

# --- DB INIT ---
with app.app_context():
    try: 
        db.create_all()
        print("Database tables created/verified.")
    except Exception as e: 
        print(f"CRITICAL DB INIT ERROR: {e}")

# --- GLOBAL LOCKS FOR CONCURRENCY CONTROL ---
order_processing_lock = threading.Lock()
active_processing_ids = set()

# --- HELPERS ---
def get_config(key, default=None):
    """Safely retrieve config with session management"""
    try:
        setting = AppSetting.query.get(key)
        if not setting:
            return default
        try: 
            return json.loads(setting.value)
        except: 
            return setting.value
    except Exception as e:
        print(f"Config Read Error ({key}): {e}")
        return default

def set_config(key, value):
    """Safely save config with rollback support"""
    try:
        setting = AppSetting.query.get(key)
        if not setting:
            setting = AppSetting(key=key)
            db.session.add(setting)
        setting.value = json.dumps(value)
        db.session.commit()
        return True
    except Exception as e:
        print(f"Config Save Error ({key}): {e}")
        db.session.rollback()
        return False

def verify_shopify(data, hmac_header):
    secret = os.getenv('SHOPIFY_SECRET')
    if not secret: return True 
    if not hmac_header: return False
    digest = hmac.new(secret.encode('utf-8'), data, hashlib.sha256).digest()
    return hmac.compare_digest(base64.b64encode(digest).decode(), hmac_header)

def log_event(entity, status, message):
    try:
        log = SyncLog(
            entity=entity, 
            status=status, 
            message=message, 
            timestamp=datetime.utcnow()
        )
        db.session.add(log)
        db.session.commit()
    except Exception as e: 
        print(f"DB LOG ERROR: {e}")
        db.session.rollback()

def extract_id(res):
    if isinstance(res, list) and len(res) > 0:
        return res[0]
    return res

def setup_shopify_session():
    """Initializes the Shopify Session"""
    shop_url = os.getenv('SHOPIFY_URL')
    token = os.getenv('SHOPIFY_TOKEN')
    if not shop_url or not token: return False
    session = shopify.Session(shop_url, '2024-01', token)
    shopify.ShopifyResource.activate_session(session)
    return True

# --- GRAPHQL HELPERS ---
def find_shopify_product_by_sku(sku):
    if not setup_shopify_session(): return None
    query = """
    {
      productVariants(first: 1, query: "sku:%s") {
        edges {
          node {
            product {
              legacyResourceId
            }
          }
        }
      }
    }
    """ % sku
    try:
        client = shopify.GraphQL()
        result = client.execute(query)
        data = json.loads(result)
        edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
        if edges:
            return edges[0]['node']['product']['legacyResourceId']
    except Exception as e:
        print(f"GraphQL Error: {e}")
    return None

def get_shopify_variant_inv_by_sku(sku):
    if not setup_shopify_session(): return None
    query = """
    {
      productVariants(first: 1, query: "sku:%s") {
        edges {
          node {
            legacyResourceId
            inventoryItem {
              legacyResourceId
            }
            inventoryQuantity
          }
        }
      }
    }
    """ % sku
    try:
        client = shopify.GraphQL()
        result = client.execute(query)
        data = json.loads(result)
        edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
        if edges:
            node = edges[0]['node']
            return {
                'variant_id': node['legacyResourceId'],
                'inventory_item_id': node['inventoryItem']['legacyResourceId'],
                'qty': node['inventoryQuantity']
            }
    except Exception as e:
        print(f"GraphQL Inv Error: {e}")
    return None

# --- CORE LOGIC ---

def process_product_data(data):
    """
    Handles Shopify Product Webhooks (Update Only).
    """
    product_type = data.get('product_type', '')
    cat_id = None
    if product_type:
        try:
            cat_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.public.category', 'search', [[['name', '=', product_type]]])
            if cat_ids:
                cat_id = cat_ids[0]
            else:
                cat_id = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                    'product.public.category', 'create', [{'name': product_type}])
        except Exception as e:
            print(f"Category Logic Error: {e}")

    variants = data.get('variants', [])
    processed_count = 0
    company_id = get_config('odoo_company_id')
    
    for v in variants:
        sku = v.get('sku')
        if not sku: continue
        product_id = odoo.search_product_by_sku(sku, company_id)
        
        if product_id:
            # --- UPDATE LOGIC (Category Only) ---
            if cat_id:
                try:
                    current_prod = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                        'product.product', 'read', [[product_id]], {'fields': ['public_categ_ids']})
                    current_cat_ids = current_prod[0].get('public_categ_ids', [])
                    if cat_id not in current_cat_ids:
                        odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                            'product.product', 'write', [[product_id], {'public_categ_ids': [(4, cat_id)]}])
                        log_event('Product', 'Info', f"Webhook: Updated Category for {sku} to '{product_type}'")
                        processed_count += 1
                except Exception as e:
                    err_msg = str(e)
                    if "pos.category" in err_msg or "CacheMiss" in err_msg or "KeyError" in err_msg:
                        pass
                    else:
                        print(f"Webhook Update Error: {e}")
        else:
            pass # Skip creation from webhook

    return processed_count

def process_order_data(data):
    """Syncs order. Forces B2B Structure: Main Partner is ALWAYS a Company. Invoice/Delivery are Child Contacts."""
    shopify_id = str(data.get('id', ''))
    shopify_name = data.get('name')
    
    with order_processing_lock:
        if shopify_id in active_processing_ids:
            log_event('Order', 'Skipped', f"Order {shopify_name} skipped (Concurrent process detected).")
            return False, "Skipped"
        active_processing_ids.add(shopify_id)

    try:
        email = data.get('email') or data.get('contact_email')
        client_ref = f"ONLINE_{shopify_name}"
        company_id = get_config('odoo_company_id')
        
        # Resolve Company Context
        if not company_id and odoo:
            try:
                user_info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                    'res.users', 'read', [[odoo.uid]], {'fields': ['company_id']})
                if user_info: company_id = user_info[0]['company_id'][0]
            except: pass

        # Check for Existing Order
        existing_order_id = None
        try:
            existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
            if existing_ids:
                existing_order_id = existing_ids[0]
        except Exception as e: return False, f"Odoo Error: {str(e)}"

        # --- PARTNER LOGIC START ---
        partner = odoo.search_partner_by_email(email)
        
        # A) Create Main Partner (Force Company)
        if not partner:
            cust_data = data.get('customer', {})
            def_address = data.get('billing_address') or data.get('shipping_address') or {}
            
            # Name Logic: Use Company Name if available, otherwise Person Name
            # (But effectively treat Person Name as a "Company" name in Odoo)
            company_name = def_address.get('company')
            person_name = f"{cust_data.get('first_name', '')} {cust_data.get('last_name', '')}".strip()
            final_name = company_name if company_name else (person_name or email)

            # VAT Extraction
            vat_number = None
            for attr in data.get('note_attributes', []):
                if attr.get('name', '').lower() in ['vat', 'vat_number', 'tax_id']:
                    vat_number = attr.get('value')

            vals = {
                'name': final_name,
                'email': email,
                'phone': cust_data.get('phone'),
                'street': def_address.get('address1'),
                'city': def_address.get('city'),
                'zip': def_address.get('zip'),
                'country_code': def_address.get('country_code'),
                'vat': vat_number,
                
                # CRITICAL: Force Company Type
                'is_company': True,
                'company_type': 'company'
            }
            if company_id: vals['company_id'] = int(company_id)
            
            try:
                partner_id = odoo.create_partner(vals)
                partner = {'id': partner_id, 'name': final_name}
                log_event('Customer', 'Success', f"Created Company Partner: {final_name}")
                
                # Link Map
                if shopify_id:
                    c_id = str(data.get('customer', {}).get('id'))
                    if c_id:
                        cust_map = CustomerMap(shopify_customer_id=c_id, odoo_partner_id=partner_id, email=email)
                        db.session.add(cust_map)
                        db.session.commit()
            except Exception as e:
                return False, f"Customer Creation Error: {e}"
        
        # Ensure we have the Main Company ID (if search found a child contact, resolve to parent)
        partner_id = extract_id(partner['parent_id'][0] if partner.get('parent_id') else partner['id'])

        # B) Handle Child Addresses (Invoice & Delivery)
        # We always check/create these to ensure the order has distinct addresses linked
        bill_addr = data.get('billing_address') or {}
        ship_addr = data.get('shipping_address') or {}
        
        # Helper to map Shopify address to Odoo Child Data
        def prep_child_addr(addr_data, label):
            display_name = addr_data.get('name') or partner['name']
            # If the name is just the company name, append the label to distinguish contacts
            # e.g. "Worthy Products (Invoice)"
            if display_name == partner['name']:
                display_name = f"{display_name} ({label})"
            
            return {
                'name': display_name,
                'street': addr_data.get('address1'),
                'city': addr_data.get('city'),
                'zip': addr_data.get('zip'),
                'country_code': addr_data.get('country_code'),
                'phone': addr_data.get('phone'),
                'email': email # Inherit email from main
            }

        # Resolve Invoice Contact
        if bill_addr:
            inv_data = prep_child_addr(bill_addr, "Invoice")
            invoice_id = odoo.find_or_create_child_address(partner_id, inv_data, type='invoice')
        else:
            invoice_id = partner_id # Fallback to main

        # Resolve Delivery Contact
        if ship_addr:
            ship_data = prep_child_addr(ship_addr, "Delivery")
            shipping_id = odoo.find_or_create_child_address(partner_id, ship_data, type='delivery')
        else:
            shipping_id = partner_id # Fallback to main
        
        # Sales Rep Resolution
        sales_rep_id = odoo.get_partner_salesperson(partner_id)
        if not sales_rep_id: sales_rep_id = odoo.uid
        # --- PARTNER LOGIC END ---

        # --- LINE ITEMS ---
        lines = []
        for item in data.get('line_items', []):
            sku = item.get('sku')
            if not sku: continue
            product_id = odoo.search_product_by_sku(sku, company_id)
            if not product_id:
                archived_id = odoo.check_product_exists_by_sku(sku, company_id)
                if archived_id:
                    log_event('Order', 'Warning', f"Skipped SKU {sku}: Product is Archived.")
                    continue 
                log_event('Product', 'Info', f"SKU {sku} missing on Order. Creating...")
                try:
                    new_p_vals = {
                        'name': item['name'], 'default_code': sku, 'list_price': float(item.get('price', 0)), 'type': 'product'
                    }
                    if company_id: new_p_vals['company_id'] = int(company_id)
                    odoo.create_product(new_p_vals)
                    product_id = odoo.search_product_by_sku(sku, company_id) 
                except Exception as e:
                    log_event('Product', 'Error', f"Failed to create SKU {sku}: {e}")

            if product_id:
                price = float(item.get('price', 0))
                qty = int(item.get('quantity', 1))
                disc = float(item.get('total_discount', 0))
                pct = (disc / (price * qty)) * 100 if price > 0 else 0.0
                lines.append((0, 0, {'product_id': product_id, 'product_uom_qty': qty, 'price_unit': price, 'name': item['name'], 'discount': pct}))
            else:
                log_event('Order', 'Warning', f"Skipped line {sku}: Product not found/created.")

        # --- SHIPPING LINES ---
        for ship_line in data.get('shipping_lines', []):
            try:
                cost = float(ship_line.get('price', 0.0))
            except: cost = 0.0
            
            ship_title = ship_line.get('title', 'Shipping')
            if cost >= 0:
                ship_product_id = None
                if ship_title:
                    ship_product_id = odoo.search_product_by_name(ship_title, company_id)
                if not ship_product_id:
                    ship_product_id = odoo.search_product_by_sku("SHIP_FEE", company_id)
                if not ship_product_id:
                    ship_product_id = odoo.search_product_by_name("Shopify Shipping", company_id)
                
                if not ship_product_id:
                    log_event('Product', 'Info', f"Creating new Shipping Service: {ship_title}")
                    try:
                        sp_vals = {
                            'name': ship_title if ship_title else "Shopify Shipping", 
                            'type': 'service', 
                            'list_price': 0.0, 
                            'default_code': 'SHIP_FEE' if not odoo.search_product_by_sku("SHIP_FEE", company_id) else None 
                        }
                        if company_id: sp_vals['company_id'] = int(company_id)
                        odoo.create_product(sp_vals)
                        if sp_vals.get('default_code'):
                             ship_product_id = odoo.search_product_by_sku("SHIP_FEE", company_id)
                        else:
                             ship_product_id = odoo.search_product_by_name(sp_vals['name'], company_id)
                    except Exception as e:
                        log_event('Product', 'Error', f"Failed to create Shipping Product: {e}")

                if ship_product_id:
                    lines.append((0, 0, {
                        'product_id': ship_product_id,
                        'product_uom_qty': 1,
                        'price_unit': cost,
                        'name': ship_title,
                        'discount': 0.0
                    }))
                else:
                    log_event('Order', 'Warning', "Shipping line skipped: Could not find/create valid shipping product.")

        if not lines: return False, "No valid lines"
        
        # --- PAYMENT METHOD & NOTES ---
        gateway = data.get('gateway') or (data.get('payment_gateway_names')[0] if data.get('payment_gateway_names') else 'Shopify')
        note_text = f"Payment Gateway: {gateway}"

        if existing_order_id:
            # --- UPDATE PATH ---
            order_info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'read', [[existing_order_id]], {'fields': ['state', 'note', 'order_line']})
            if not order_info: return False, "Order not found in Odoo"
            
            current_order = order_info[0]
            state = current_order['state']
            if state in ['done', 'cancel']:
                log_event('Order', 'Skipped', f"Order {client_ref} is {state}. Update skipped.")
                return True, "Skipped"
            
            # Change Detection
            has_changes = False
            existing_note = current_order.get('note') or ''
            if note_text != existing_note: has_changes = True

            if not has_changes:
                current_line_ids = current_order.get('order_line', [])
                if len(current_line_ids) != len(lines):
                    has_changes = True
                elif current_line_ids:
                    current_lines = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                        'sale.order.line', 'read', [current_line_ids], {'fields': ['product_id', 'product_uom_qty', 'price_unit', 'discount']})
                    
                    curr_set = []
                    for l in current_lines:
                        pid = l['product_id'][0] if isinstance(l['product_id'], (list, tuple)) else l['product_id']
                        curr_set.append((pid, float(l['product_uom_qty']), float(l['price_unit']), float(l['discount'])))
                    curr_set.sort()

                    new_set = []
                    for l in lines:
                        d = l[2] 
                        new_set.append((d['product_id'], float(d['product_uom_qty']), float(d['price_unit']), float(d['discount'])))
                    new_set.sort()
                    
                    # Fuzzy float comparison
                    if len(curr_set) != len(new_set): has_changes = True
                    else:
                        for c, n in zip(curr_set, new_set):
                            if c[0] != n[0] or abs(c[1]-n[1])>0.001 or abs(c[2]-n[2])>0.01 or abs(c[3]-n[3])>0.01:
                                has_changes = True
                                break
            
            if not has_changes:
                log_event('Order', 'Info', f"Order {client_ref} is up to date. Skipped update.")
                return True, "Up to Date"

            update_vals = {
                'order_line': [(5, 0, 0)] + lines,
                'partner_shipping_id': shipping_id, # Updated to use Delivery Contact
                'partner_invoice_id': invoice_id,   # Updated to use Invoice Contact
                'note': note_text 
            }
            try:
                odoo.update_sale_order(existing_order_id, update_vals)
                odoo.post_message(existing_order_id, f"Order Updated via Shopify Sync. {note_text}")
                log_event('Order', 'Success', f"Updated {client_ref} (Revision)")
                return True, "Updated"
            except Exception as e:
                log_event('Order', 'Error', f"Update Failed: {e}")
                return False, str(e)
        else:
            # --- CREATE PATH ---
            vals = {
                'name': client_ref, 
                'client_order_ref': client_ref, 
                'partner_id': partner_id,           # Main Company
                'partner_invoice_id': invoice_id,   # Child Contact
                'partner_shipping_id': shipping_id, # Child Contact
                'order_line': lines, 
                'user_id': sales_rep_id, 
                'state': 'draft',
                'note': note_text
            }
            if company_id: vals['company_id'] = int(company_id)
            try:
                odoo.create_sale_order(vals, context={'manual_price': True})
                log_event('Order', 'Success', f"Synced {client_ref}")
                return True, "Synced"
            except Exception as e:
                log_event('Order', 'Error', str(e))
                return False, str(e)
    finally:
        with order_processing_lock:
            if shopify_id in active_processing_ids:
                active_processing_ids.remove(shopify_id)

def sync_products_master():
    """Odoo -> Shopify Product Sync"""
    with app.app_context():
        if not odoo or not setup_shopify_session(): 
            log_event('System', 'Error', "Product Sync Failed: Connection Error")
            return

        company_id = get_config('odoo_company_id')
        odoo_products = odoo.get_all_products(company_id)
        active_odoo_skus = set()
        
        # Load Field Configs
        sync_title = get_config('prod_sync_title', True)
        sync_desc = get_config('prod_sync_desc', True)
        sync_price = get_config('prod_sync_price', True)
        sync_type = get_config('prod_sync_type', True)
        sync_vendor = get_config('prod_sync_vendor', True)
        
        log_event('Product Sync', 'Info', f"Found {len(odoo_products)} products. Starting Master Sync...")
        
        synced = 0
        for p in odoo_products:
            sku = p.get('default_code')
            if not sku: continue

            if not p.get('active', True):
                shopify_id = find_shopify_product_by_sku(sku)
                if shopify_id:
                    try:
                        sp = shopify.Product.find(shopify_id)
                        if sp.status != 'archived':
                            sp.status = 'archived'
                            sp.save()
                            log_event('Product Sync', 'Info', f"Archived {sku} in Shopify.")
                    except: pass
                continue 

            active_odoo_skus.add(sku)
            shopify_id = find_shopify_product_by_sku(sku)
            try:
                if shopify_id: sp = shopify.Product.find(shopify_id)
                else: sp = shopify.Product()
                product_changed = False
                
                # Title
                if sync_title and sp.title != p['name']:
                    sp.title = p['name']
                    product_changed = True
                
                # Description
                if sync_desc:
                    odoo_desc = p.get('description_sale') or ''
                    if (sp.body_html or '') != odoo_desc:
                        sp.body_html = odoo_desc
                        product_changed = True
                
                # Category Mapping
                odoo_categ_ids = p.get('public_categ_ids', [])
                if not odoo_categ_ids and sp.product_type:
                    # Init logic (Shopify -> Odoo) always runs to fill gaps
                    try:
                        cat_name = sp.product_type
                        cat_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'search', [[['name', '=', cat_name]]])
                        cat_id = cat_ids[0] if cat_ids else None
                        if not cat_id:
                            cat_id = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'create', [{'name': cat_name}])
                        odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'write', [[p['id']], {'public_categ_ids': [(4, cat_id)]}])
                        log_event('Product Sync', 'Info', f"Initialized Odoo Category for {sku}: {cat_name}")
                    except Exception as e:
                        err_msg = str(e)
                        if "pos.category" in err_msg or "CacheMiss" in err_msg or "KeyError" in err_msg:
                             pass
                        else:
                             print(f"Category Import Error: {e}")

                elif odoo_categ_ids and sync_type:
                    odoo_cat_name = odoo.get_public_category_name(odoo_categ_ids)
                    if odoo_cat_name and sp.product_type != odoo_cat_name:
                        sp.product_type = odoo_cat_name
                        product_changed = True

                # Vendor Mapping
                if sync_vendor:
                    product_title = p.get('name', '')
                    target_vendor = product_title.split()[0] if product_title else 'Odoo Master'
                    if sp.vendor != target_vendor:
                        sp.vendor = target_vendor
                        product_changed = True

                if sp.status != 'active':
                    sp.status = 'active'
                    product_changed = True
                
                if product_changed or not shopify_id:
                    sp.save()
                    if not shopify_id:
                        sp = shopify.Product.find(sp.id)
                
                if sp.variants: 
                    variant = sp.variants[0]
                else: 
                    variant = shopify.Variant(prefix_options={'product_id': sp.id})
                
                variant_changed = False
                if variant.sku != sku:
                    variant.sku = sku
                    variant_changed = True
                
                if sync_price:
                    target_price = str(p['list_price'])
                    if variant.price != target_price:
                        variant.price = target_price
                        variant_changed = True

                target_barcode = p.get('barcode', 0) or ''
                if str(variant.barcode or '') != str(target_barcode):
                    variant.barcode = str(target_barcode)
                    variant_changed = True
                
                try:
                    if float(variant.weight or 0) != float(p.get('weight', 0)):
                        variant.weight = p.get('weight', 0)
                        variant_changed = True
                except:
                    variant.weight = p.get('weight', 0)
                    variant_changed = True

                if variant.inventory_management != 'shopify':
                    variant.inventory_management = 'shopify'
                    variant_changed = True
                
                v_product_id = getattr(variant, 'product_id', None)
                if not v_product_id: 
                    if variant.attributes: v_product_id = variant.attributes.get('product_id')
                
                if str(v_product_id) != str(sp.id):
                    variant.product_id = sp.id
                    variant_changed = True

                if variant_changed: variant.save()
                
                if SHOPIFY_LOCATION_ID and variant.inventory_item_id:
                    qty = int(p.get('qty_available', 0))
                    try:
                         current_inv = get_shopify_variant_inv_by_sku(sku)
                         if current_inv and int(current_inv['qty']) != qty:
                             shopify.InventoryLevel.set(location_id=SHOPIFY_LOCATION_ID, inventory_item_id=variant.inventory_item_id, available=qty)
                             log_event('Product Sync', 'Info', f"Updated Stock for {sku}: {qty}")
                    except: pass

                # Cost Price Sync
                if variant.inventory_item_id:
                    try:
                        cost = float(p.get('standard_price', 0.0))
                        inv_item = shopify.InventoryItem.find(variant.inventory_item_id)
                        if float(inv_item.cost or 0) != cost:
                            inv_item.cost = cost
                            inv_item.save()
                    except: pass

                # Image Sync
                if get_config('prod_sync_images', False):
                    try:
                        img_data = odoo.get_product_image(p['id'])
                        if img_data and not sp.images:
                            if isinstance(img_data, bytes):
                                img_data = img_data.decode('utf-8')
                            image = shopify.Image(prefix_options={'product_id': sp.id})
                            image.attachment = img_data
                            image.save()
                            log_event('Product Sync', 'Info', f"Synced Image for {sku}")
                    except Exception as img_e:
                        log_event('Product Sync', 'Warning', f"Image Sync Failed for {sku}: {img_e}")

                # Metafield Sync
                if get_config('prod_sync_meta_vendor_code', False):
                    vendor_code = odoo.get_vendor_product_code(p['product_tmpl_id'][0])
                    if vendor_code:
                        metafield = shopify.Metafield({
                            'key': 'vendor_product_code', 'value': vendor_code, 'type': 'single_line_text_field',
                            'namespace': 'custom', 'owner_resource': 'product', 'owner_id': sp.id
                        })
                        metafield.save()
                synced += 1
            except Exception as e:
                err_msg = str(e)
                if "pos.category" in err_msg or "CacheMiss" in err_msg:
                    pass 
                else:
                    log_event('Product Sync', 'Error', f"Failed {sku}: {e}")
        
        cleanup_shopify_products(active_odoo_skus)
        log_event('Product Sync', 'Success', f"Master Sync Complete. Processed {synced} active products.")

def sync_customers_master():
    """
    Odoo -> Shopify Customer Sync (Master). 
    - Pushes VAT and Company Name.
    - Merges Odoo Tags (Preserves existing Shopify tags).
    - Maps Odoo Salesperson -> Shopify 'custom.salesrep' Metafield.
    """
    with app.app_context():
        if not odoo or not setup_shopify_session(): 
            log_event('System', 'Error', "Customer Sync Failed: Connection Error")
            return

        # 1. Configuration
        direction = get_config('cust_direction', 'bidirectional')
        if direction == 'shopify_to_odoo':
            log_event('Customer Sync', 'Skipped', "Sync direction is set to Shopify -> Odoo only.")
            return

        company_id = get_config('odoo_company_id')
        # We still use these to DECIDE if we should sync, but we will sync ALL tags found on the customer
        whitelist = [t.strip() for t in get_config('cust_whitelist_tags', '').split(',') if t.strip()]
        blacklist = [t.strip() for t in get_config('cust_blacklist_tags', '').split(',') if t.strip()]
        use_tags_filter = get_config('cust_sync_tags', False)

        # 2. Fetch Odoo Customers (Active Companies & Individuals)
        # Using a far past date to simulate "Get All" for the manual trigger
        last_run = "2000-01-01 00:00:00" 
        odoo_customers = odoo.get_changed_customers(last_run, company_id)
        
        log_event('Customer Sync', 'Info', f"Found {len(odoo_customers)} customers in Odoo. Processing...")
        
        synced_count = 0
        
        for p in odoo_customers:
            email = p.get('email')
            if not email or "@" not in email: continue # Shopify requires valid email

            # Get Odoo Tags Names
            odoo_tags = odoo.get_tag_names(p.get('category_id', []))

            # 3. Filter Logic (Should we sync this customer?)
            if use_tags_filter:
                if blacklist and any(t in odoo_tags for t in blacklist): continue
                if whitelist and not any(t in odoo_tags for t in whitelist): continue

            try:
                # 4. Find or Init Shopify Customer
                shopify_cust = shopify.Customer.search(query=f"email:{email}")
                if shopify_cust:
                    c = shopify_cust[0]
                else:
                    c = shopify.Customer()
                    c.email = email
                
                # 5. Map Basic Fields
                c.first_name = p.get('name', '').split(' ')[0]
                c.last_name = ' '.join(p.get('name', '').split(' ')[1:]) or 'Customer'
                c.phone = p.get('phone') or p.get('mobile')
                c.verified_email = True
                
                # 6. Map Address & Company
                address_data = {
                    'address1': p.get('street') or '',
                    'city': p.get('city') or '',
                    'zip': p.get('zip') or '',
                    'country_code': p.get('country_id')[1] if p.get('country_id') else '', 
                    'company': p.get('name') if p.get('is_company') else (p.get('parent_id')[1] if p.get('parent_id') else ''),
                    'phone': c.phone,
                    'first_name': c.first_name,
                    'last_name': c.last_name,
                    'default': True
                }
                c.addresses = [shopify.Address(address_data)]
                
                # 7. TAG SYNC (Merge Strategy)
                # Get current Shopify tags as a list
                current_shopify_tags = [t.strip() for t in c.tags.split(',')] if c.tags else []
                
                # Combine Odoo tags with existing Shopify tags (Set union removes duplicates)
                # This ensures we add new Odoo tags without deleting manual Shopify tags
                final_tag_list = list(set(current_shopify_tags + odoo_tags))
                c.tags = ",".join(final_tag_list)

                # 8. PREPARE METAFIELDS
                metafields_to_save = []

                # VAT Metafield
                vat = p.get('vat')
                if vat:
                    c.note = f"VAT Number: {vat}"
                    metafields_to_save.append(shopify.Metafield({
                        'key': 'vat_number',
                        'value': vat,
                        'type': 'single_line_text_field',
                        'namespace': 'custom'
                    }))
                    c.tax_exempt = True 

                # SALESPERSON Metafield (New Logic)
                # Odoo returns user_id as [id, "Name"]
                salesperson_field = p.get('user_id')
                if salesperson_field:
                    rep_name = salesperson_field[1] # Get the name string
                    metafields_to_save.append(shopify.Metafield({
                        'key': 'salesrep',
                        'value': rep_name,
                        'type': 'single_line_text_field',
                        'namespace': 'custom'
                    }))

                # Assign accumulated metafields
                if metafields_to_save:
                    c.metafields = metafields_to_save

                c.save()
                
                # 9. Link in DB
                if not CustomerMap.query.filter_by(shopify_customer_id=str(c.id)).first():
                    new_map = CustomerMap(shopify_customer_id=str(c.id), odoo_partner_id=p['id'], email=email)
                    db.session.add(new_map)
                    db.session.commit()
                
                synced_count += 1

            except Exception as e:
                log_event('Customer Sync', 'Error', f"Failed {email}: {e}")

        log_event('Customer Sync', 'Success', f"Sync Complete. Processed {synced_count} customers.")

def archive_shopify_duplicates():
    """Scans Shopify for duplicate SKUs and archives the older ones."""
    if not setup_shopify_session(): return

    log_event('Duplicate Scan', 'Info', "Starting scan for duplicate SKUs...")
    
    sku_map = {} # SKU -> List of Products
    page = shopify.Product.find(limit=250)
    
    # 1. Build Map
    while page:
        for product in page:
            if product.status == 'archived': continue
            
            # Use first variant's SKU for identification
            sku = product.variants[0].sku if product.variants else None
            if sku:
                if sku not in sku_map: sku_map[sku] = []
                sku_map[sku].append(product)
        
        if page.has_next_page(): page = page.next_page()
        else: break
    
    # 2. Process Duplicates
    archived_count = 0
    for sku, products in sku_map.items():
        if len(products) > 1:
            # Sort by created_at (keep the newest)
            # Format: 2024-10-05T12:00:00-04:00
            products.sort(key=lambda x: x.created_at, reverse=True)
            
            # Keep the first one (index 0), archive the rest
            to_archive = products[1:]
            for p in to_archive:
                try:
                    p.status = 'archived'
                    p.save()
                    archived_count += 1
                    log_event('Duplicate Scan', 'Warning', f"Archived Duplicate: {p.title} (SKU: {sku})")
                except Exception as e:
                    print(f"Failed to archive {p.id}: {e}")

    log_event('Duplicate Scan', 'Success', f"Scan Complete. Archived {archived_count} duplicates.")

def sync_categories_only():
    """Optimized ONE-TIME import of Categories from Shopify to Odoo."""
    with app.app_context():
        if not odoo or not setup_shopify_session(): 
            log_event('System', 'Error', "Category Sync Failed: Connection Error")
            return

        log_event('System', 'Info', "Starting Optimized Category Sync...")
        company_id = get_config('odoo_company_id')
        odoo_prods = odoo.get_all_products(company_id)
        odoo_map = {p['default_code']: p for p in odoo_prods if p.get('default_code')}
        
        cat_map = {}
        try:
            cats = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'search_read', [[]], {'fields': ['id', 'name']})
            for c in cats: cat_map[c['name']] = c['id']
        except Exception as e: print(f"Cache Error: {e}")

        updated_count = 0
        page = shopify.Product.find(limit=250)
        while page:
            for sp in page:
                if not sp.product_type: continue
                variant = sp.variants[0] if sp.variants else None
                if not variant or not variant.sku: continue
                sku = variant.sku
                
                odoo_prod = odoo_map.get(sku)
                if not odoo_prod or odoo_prod.get('public_categ_ids') or not odoo_prod.get('active', True): continue

                try:
                    cat_name = sp.product_type
                    cat_id = cat_map.get(cat_name)
                    if not cat_id:
                        cat_id = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'create', [{'name': cat_name}])
                        cat_map[cat_name] = cat_id
                    
                    odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'write', [[odoo_prod['id']], {'public_categ_ids': [(4, cat_id)]}])
                    updated_count += 1
                    odoo_prod['public_categ_ids'] = [cat_id] 
                except Exception as e:
                    err_msg = str(e)
                    if "pos.category" in err_msg or "CacheMiss" in err_msg:
                        pass 
                    else:
                        print(f"Error syncing category for {sku}: {e}")

            if page.has_next_page(): page = page.next_page()
            else: break
        
        log_event('System', 'Success', f"Category Sync Finished. Updated {updated_count} products.")

def cleanup_shopify_products(odoo_active_skus):
    if not setup_shopify_session(): return
    seen_skus = set()
    products = shopify.Product.find(limit=250)
    page = products
    archived_count = 0
    try:
        while page:
            for sp in page:
                variant = sp.variants[0] if sp.variants else None
                if not variant or not variant.sku: continue
                sku = variant.sku
                needs_archive = False
                if sku not in odoo_active_skus: needs_archive = True
                elif sku in seen_skus: needs_archive = True
                
                if needs_archive:
                    if sp.status != 'archived':
                        sp.status = 'archived'
                        sp.save()
                        archived_count += 1
                else: seen_skus.add(sku)
            if page.has_next_page(): page = page.next_page()
            else: break
    except: pass
    if archived_count > 0: log_event('System', 'Success', f"Cleanup Complete. Archived {archived_count} products.")
def perform_inventory_sync(lookback_minutes):
    """Checks Odoo for recent stock moves and updates Shopify."""
    if not odoo or not setup_shopify_session(): return 0, 0
    
    target_locations = get_config('inventory_locations', [])
    target_field = get_config('inventory_field', 'qty_available')
    sync_zero = get_config('sync_zero_stock', False)
    company_id = get_config('odoo_company_id', None)
    
    if not company_id:
        try:
            u = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'res.users', 'read', [[odoo.uid]], {'fields': ['company_id']})
            if u: company_id = u[0]['company_id'][0]
        except: pass

    # --- UPDATED: Use Stock Moves instead of Product Write Date ---
    last_run = datetime.utcnow() - timedelta(minutes=lookback_minutes)
    try: 
        product_ids = odoo.get_product_ids_with_recent_stock_moves(str(last_run), company_id)
    except Exception as e: 
        print(f"Inventory Crawl Error: {e}")
        return 0, 0
    
    count = 0
    updates = 0
    for p_id in product_ids:
        total_odoo = int(odoo.get_total_qty_for_locations(p_id, target_locations, field_name=target_field))
        if sync_zero and total_odoo <= 0: continue
        
        p_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'read', [p_id], {'fields': ['default_code']})
        sku = p_data[0].get('default_code')
        if not sku: continue
        
        shopify_info = get_shopify_variant_inv_by_sku(sku)
        if not shopify_info: continue
        
        if int(shopify_info['qty']) != total_odoo:
            try:
                shopify.InventoryLevel.set(location_id=SHOPIFY_LOCATION_ID, inventory_item_id=shopify_info['inventory_item_id'], available=total_odoo)
                updates += 1
                log_event('Inventory', 'Info', f"Updated SKU {sku}: {shopify_info['qty']} -> {total_odoo}")
            except Exception as e: print(f"Inv Error {sku}: {e}")
        count += 1
    return count, updates


def scheduled_inventory_sync():
    with app.app_context():
        c, u = perform_inventory_sync(lookback_minutes=35)
        if u > 0: log_event('Inventory', 'Success', f"Auto-Sync: Checked {c}, Updated {u}")

@app.route('/')
def dashboard():
    try:
        logs_orders = SyncLog.query.filter(SyncLog.entity.in_(['Order', 'Order Cancel'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_inventory = SyncLog.query.filter_by(entity='Inventory').order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_products = SyncLog.query.filter(SyncLog.entity.in_(['Product', 'Product Sync', 'Duplicate Scan'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_customers = SyncLog.query.filter(SyncLog.entity.in_(['Customer', 'Customer Sync'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
        logs_system = SyncLog.query.filter(SyncLog.entity.notin_(['Order', 'Order Cancel', 'Inventory', 'Customer', 'Product', 'Product Sync', 'Duplicate Scan', 'Customer Sync'])).order_by(SyncLog.timestamp.desc()).limit(20).all()
    except:
        logs_orders = logs_inventory = logs_products = logs_customers = logs_system = []
    
    current_settings = {
        "odoo_company_id": get_config('odoo_company_id', None),
        "locations": get_config('inventory_locations', []), 
        "field": get_config('inventory_field', 'qty_available'),
        "sync_zero": get_config('sync_zero_stock', False),
        "combine_committed": get_config('combine_committed', False),
        "cust_direction": get_config('cust_direction', 'bidirectional'),
        "cust_auto_sync": get_config('cust_auto_sync', True),
        "cust_sync_tags": get_config('cust_sync_tags', False),
        "cust_whitelist_tags": get_config('cust_whitelist_tags', ''),
        "cust_blacklist_tags": get_config('cust_blacklist_tags', ''),
        
        "prod_auto_create": get_config('prod_auto_create', False),
        "prod_auto_publish": get_config('prod_auto_publish', False),
        "prod_sync_images": get_config('prod_sync_images', False),
        "prod_sync_tags": get_config('prod_sync_tags', False),
        "prod_sync_meta_vendor_code": get_config('prod_sync_meta_vendor_code', False),
        "order_sync_tax": get_config('order_sync_tax', False),
        
        "prod_sync_price": get_config('prod_sync_price', True),
        "prod_sync_title": get_config('prod_sync_title', True),
        "prod_sync_desc": get_config('prod_sync_desc', True),
        "prod_sync_type": get_config('prod_sync_type', True),
        "prod_sync_vendor": get_config('prod_sync_vendor', True)
    }
    odoo_status = True if odoo else False
    return render_template('dashboard.html', 
                           logs_orders=logs_orders, logs_inventory=logs_inventory, logs_products=logs_products,
                           logs_customers=logs_customers, logs_system=logs_system,
                           odoo_status=odoo_status, current_settings=current_settings)

@app.route('/live_logs')
def live_logs():
    return render_template('live_logs.html')

@app.route('/api/logs/live', methods=['GET'])
def api_live_logs():
    try:
        logs = SyncLog.query.order_by(SyncLog.timestamp.desc()).limit(100).all()
        data = []
        for log in logs:
            msg_type = 'info'
            status_lower = (log.status or '').lower()
            if 'error' in status_lower or 'fail' in status_lower: msg_type = 'error'
            elif 'success' in status_lower: msg_type = 'success'
            elif 'warning' in status_lower or 'skip' in status_lower: msg_type = 'warning'
            iso_ts = log.timestamp.isoformat()
            if not iso_ts.endswith('Z'): iso_ts += 'Z'
            data.append({'id': log.id, 'timestamp': iso_ts, 'message': f"[{log.entity}] {log.message}", 'type': msg_type, 'details': log.status})
        return jsonify(data)
    except: return jsonify([])

@app.route('/sync/inventory', methods=['GET'])
def sync_inventory_endpoint():
    log_event('System', 'Info', 'Manual Trigger: Starting Inventory Sync (Full Scan)...')
    with app.app_context():
        c, u = perform_inventory_sync(lookback_minutes=525600)
        log_event('Inventory', 'Success', f"Manual Sync Complete. Checked {c}, Updated {u}")
        return jsonify({"synced": c, "updates": u})

@app.route('/sync/categories/run_initial_import', methods=['GET'])
def run_initial_category_import():
    threading.Thread(target=sync_categories_only).start()
    return jsonify({"message": "Job Started"})

@app.route('/webhook/products/create', methods=['POST'])
@app.route('/webhook/products/update', methods=['POST'])
def product_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    with app.app_context(): process_product_data(request.json)
    return "Received", 200

@app.route('/sync/products/master', methods=['POST'])
def trigger_master_sync():
    threading.Thread(target=sync_products_master).start()
    return jsonify({"message": "Started"})

@app.route('/sync/customers/master', methods=['POST'])
def trigger_customer_master_sync():
    threading.Thread(target=sync_customers_master).start()
    return jsonify({"message": "Started"})

@app.route('/sync/products/archive_duplicates', methods=['POST'])
def trigger_duplicate_scan():
    threading.Thread(target=archive_shopify_duplicates).start()
    return jsonify({"message": "Started"})

@app.route('/sync/orders/manual', methods=['GET'])
def manual_order_fetch():
    url = f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders.json?limit=10"
    headers = {"X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN')}
    try:
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            return jsonify({"orders": [], "error": f"Shopify API Error: {res.status_code}"})
        orders = res.json().get('orders', [])
    except Exception as e:
        return jsonify({"orders": [], "error": str(e)})
    
    mapped_orders = []
    for o in orders:
        status = "Not Synced"
        try:
            client_ref = f"ONLINE_{o['name']}"
            exists = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
            if exists: status = "Synced"
        except: pass
        if o.get('cancelled_at'): status = "Cancelled"
        mapped_orders.append({
            'id': o['id'], 'name': o['name'], 'date': o['created_at'], 'total': o['total_price'], 'odoo_status': status
        })
    return jsonify({"orders": mapped_orders})

@app.route('/sync/orders/import_batch', methods=['POST'])
def import_selected_orders():
    ids = request.json.get('order_ids', [])
    headers = {"X-Shopify-Access-Token": os.getenv('SHOPIFY_TOKEN')}
    synced = 0
    log_event('System', 'Info', f"Manual Trigger: Importing {len(ids)} orders...")
    for oid in ids:
        res = requests.get(f"https://{os.getenv('SHOPIFY_URL')}/admin/api/2025-10/orders/{oid}.json", headers=headers)
        if res.status_code == 200:
            success, _ = process_order_data(res.json().get('order'))
            if success: synced += 1
    return jsonify({"message": f"Batch Complete. Synced: {synced}"})

@app.route('/webhook/orders', methods=['POST'])
@app.route('/webhook/orders/updated', methods=['POST'])
def order_webhook():
    if not verify_shopify(request.get_data(), request.headers.get('X-Shopify-Hmac-Sha256')): return "Unauthorized", 401
    with app.app_context(): process_order_data(request.json)
    return "Received", 200

@app.route('/webhook/orders/cancelled', methods=['POST'])
def order_cancelled_webhook(): return "Received", 200

@app.route('/webhook/refunds', methods=['POST'])
def refund_webhook(): return "Received", 200

@app.route('/test/simulate_order', methods=['POST'])
def test_sim_dummy():
     log_event('System', 'Success', "Test Connection Triggered by User")
     return jsonify({"message": "OK"})

@app.route('/api/odoo/companies', methods=['GET'])
def api_get_companies():
    if odoo: return jsonify(odoo.get_companies())
    return jsonify([])

@app.route('/api/odoo/locations', methods=['GET'])
def api_get_locations():
    if odoo: return jsonify(odoo.get_locations(request.args.get('company_id')))
    return jsonify([])

@app.route('/api/settings/save', methods=['POST'])
def api_save_settings():
    data = request.json
    try:
        # Inventory
        set_config('inventory_locations', data.get('locations', []))
        set_config('inventory_field', data.get('field', 'qty_available'))
        set_config('sync_zero_stock', data.get('sync_zero', False))
        set_config('combine_committed', data.get('combine_committed', False))
        
        # General
        set_config('odoo_company_id', data.get('company_id'))
        
        # Customers
        set_config('cust_direction', data.get('cust_direction'))
        set_config('cust_auto_sync', data.get('cust_auto_sync'))
        set_config('cust_sync_tags', data.get('cust_sync_tags'))
        set_config('cust_whitelist_tags', data.get('cust_whitelist_tags', ''))
        set_config('cust_blacklist_tags', data.get('cust_blacklist_tags', ''))
        
        # Products (NEW)
        set_config('prod_auto_create', data.get('prod_auto_create', False))
        set_config('prod_auto_publish', data.get('prod_auto_publish', False))
        set_config('prod_sync_images', data.get('prod_sync_images', False))
        set_config('prod_sync_tags', data.get('prod_sync_tags', False))
        set_config('prod_sync_meta_vendor_code', data.get('prod_sync_meta_vendor_code', False))
        
        set_config('prod_sync_price', data.get('prod_sync_price', True))
        set_config('prod_sync_title', data.get('prod_sync_title', True))
        set_config('prod_sync_desc', data.get('prod_sync_desc', True))
        set_config('prod_sync_type', data.get('prod_sync_type', True))
        set_config('prod_sync_vendor', data.get('prod_sync_vendor', True))

        # Orders (NEW)
        set_config('order_sync_tax', data.get('order_sync_tax', False))
        
        return jsonify({"message": "Saved"})
    except Exception as e:
        return jsonify({"message": str(e)}), 500


def run_schedule():
    # --- UPDATED: Run tasks in Threads so they don't block each other ---
    schedule.every(1).days.do(lambda: threading.Thread(target=sync_products_master).start())
    schedule.every(1).days.do(lambda: threading.Thread(target=sync_customers_master).start())
    schedule.every(30).days.do(lambda: threading.Thread(target=archive_shopify_duplicates).start())
    
    # 30 Minute Inventory Sync
    schedule.every(30).minutes.do(lambda: threading.Thread(target=scheduled_inventory_sync).start())
    
    while True:
        schedule.run_pending()
        time.sleep(1)

# --- START SCHEDULER (Threaded, outside main so Gunicorn sees it) ---
t = threading.Thread(target=run_schedule, daemon=True)
t.start()

if __name__ == '__main__':
    # Flask Dev Server
    app.run(debug=True)
