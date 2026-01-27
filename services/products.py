import shopify
import hashlib
import time
from difflib import SequenceMatcher
from datetime import datetime
from models import Shop, ProductMap, db
from utils import get_odoo_connection, log_event, setup_shopify_session, q_default

# =====================================================
# 1. THE DISPATCHER (Triggered by Dashboard Button)
# =====================================================
def sync_products_master(shop_url):
    """
    Fetches all active products from Odoo and queues them for processing.
    """
    from app import app
    with app.app_context():
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop: return
        
        odoo = get_odoo_connection(shop_url)
        if not odoo: return

        # Fetch ALL Active Products (Saleable)
        domain = [
            ['sale_ok', '=', True], 
            ['type', 'in', ['product', 'consu']], 
            ['company_id', '=', int(shop.odoo_company_id)]
        ]
        
        try:
            # We only get IDs first to keep it fast
            product_ids = odoo.models.execute_kw(
                odoo.db, odoo.uid, odoo.password, 
                'product.product', 'search', [domain]
            )
        except Exception as e:
            log_event('Product Sync', 'Error', f"Search Failed: {e}", shop_url=shop_url)
            return

        # Queue in Batches of 50
        BATCH_SIZE = 50
        chunks = [product_ids[i:i + BATCH_SIZE] for i in range(0, len(product_ids), BATCH_SIZE)]

        for index, batch_ids in enumerate(chunks):
            # Job timeout is set to 1 hour (3600s) to handle large image uploads safely
            q_default.enqueue(
                sync_product_batch_task, 
                shop_url, 
                batch_ids, 
                f"Batch {index+1}/{len(chunks)}", 
                job_timeout=3600
            )

        log_event('Product Sync', 'Success', f"Queued {len(chunks)} batches (Self-Healing Mode)", shop_url=shop_url)


# =====================================================
# 2. THE WORKER (Runs in Background)
# =====================================================
def sync_product_batch_task(shop_url, batch_ids, batch_name):
    """
    Processes a batch of 50 products:
    - Checks for 'Monsters' (mismatched SKUs) and fixes them.
    - Creates missing products.
    - Updates existing products.
    """
    from app import app
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not setup_shopify_session(shop_url): return
        
        # Fetch detailed data for this batch
        fields = ['default_code', 'name', 'list_price', 'description_sale', 'image_1920', 'qty_per_pack']
        products = odoo.models.execute_kw(
            odoo.db, odoo.uid, odoo.password, 
            'product.product', 'read', [batch_ids], {'fields': fields}
        )
        
        stats = {'fixed': 0, 'created': 0, 'updated': 0, 'skipped': 0}
        
        for p in products:
            try:
                res = process_single_product(p, shop_url)
                stats[res] = stats.get(res, 0) + 1
            except Exception as e:
                print(f"Error processing {p.get('default_code')}: {e}")
        
        # Log summary to Dashboard
        log_event(
            'Product Sync', 
            'Info', 
            f"✅ {batch_name}: Fixed Monsters: {stats.get('fixed', 0)}, Created: {stats.get('created', 0)}, Updated: {stats.get('updated', 0)}", 
            shop_url=shop_url
        )


# =====================================================
# 3. THE LOGIC (Monster Slayer + Sync)
# =====================================================
def process_single_product(p, shop_url):
    """
    The core logic. It ensures the SKU in Shopify matches the Product Name in Odoo.
    If it doesn't, it destroys the bad link and creates a fresh product.
    """
    sku = (p.get('default_code') or '').strip()
    if not sku: return "skipped"
    
    title = p.get('name')
    action = "skipped"

    # --- A. FIND IN SHOPIFY ---
    sp = None
    try:
        # We verify identity by searching FRESH from Shopify (ignoring potentially corrupt DB map)
        variants = shopify.Variant.find(limit=1, params={'sku': sku})
        if variants:
            sp = shopify.Product.find(variants[0].product_id)
    except: pass

    # --- B. MONSTER CHECK (The Fix) ---
    if sp:
        shopify_title = str(sp.title).lower()
        odoo_title = str(title).lower()
        
        # Compare Titles: If they are less than 40% similar, it's a "Monster" (Wrong Product)
        similarity = SequenceMatcher(None, shopify_title, odoo_title).ratio()
        
        if similarity < 0.4:
            print(f"👹 MONSTER FOUND: SKU {sku} is trapped inside '{sp.title}'. Killing variant...")
            try:
                # 1. Destroy the specific variant to free the SKU
                for v in sp.variants:
                    if v.sku == sku:
                        v.destroy()
                        break
            except: pass
            
            # 2. Forget this product object so we force creation of a new one
            sp = None
            action = "fixed"

    # --- C. CREATE OR UPDATE ---
    if not sp:
        # Create New Product
        sp = shopify.Product()
        sp.title = title
        sp.vendor = "VJT"
        sp.product_type = "Synced Product"
        sp.status = 'active'
        sp.save()
        
        # Create Variant
        v = shopify.Variant({'product_id': sp.id})
        v.sku = sku
        v.price = p.get('list_price', 0.0)
        v.inventory_management = 'shopify'
        sp.variants = [v]
        sp.save()
        
        if action != "fixed": action = "created"
    else:
        # Update Existing Product (Only if titles match/are safe)
        if sp.title != title:
            sp.title = title
            sp.save()
            action = "updated"
        
        # Update Price
        v = sp.variants[0]
        if float(v.price or 0) != float(p.get('list_price', 0.0)):
            v.price = p.get('list_price', 0.0)
            sp.variants = [v] # Must re-attach variants list to save updates
            sp.save()
            action = "updated"

    # --- D. IMAGE UPLOAD ---
    # We only upload if the product has NO images. 
    # This prevents the "infinite image append" bug and speeds up sync.
    if p.get('image_1920') and not sp.images:
        try:
            img = shopify.Image(prefix_options={'product_id': sp.id})
            img.attachment = p['image_1920']
            img.save()
        except: pass

    # --- E. UPDATE DATABASE MAP ---
    # Keep our local database in sync with reality
    try:
        pm = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
        
        # If map is missing, create it
        if not pm and sp.variants:
             pm = ProductMap(
                 sku=sku, 
                 odoo_product_id=p['id'], 
                 shopify_variant_id=str(sp.variants[0].id), 
                 shop_url=shop_url
             )
             db.session.add(pm)
        
        # If map exists but points to wrong ID, fix it
        if pm and sp.variants and pm.shopify_variant_id != str(sp.variants[0].id):
            pm.shopify_variant_id = str(sp.variants[0].id)
            
        db.session.commit()
    except: db.session.rollback()

    return action
