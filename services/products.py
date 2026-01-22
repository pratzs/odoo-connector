import shopify
import json
import gc
from datetime import datetime
from models import Shop, ProductMap, AppSetting
from utils import get_odoo_connection, log_event, setup_shopify_session, get_config, q_default

# =====================================================
# 1. THE DISPATCHER
# =====================================================
def sync_products_master(shop_url):
    from app import app
    with app.app_context():
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop: return
        
        odoo = get_odoo_connection(shop_url)
        if not odoo: return

        company_id = shop.odoo_company_id
        if not company_id: return

        # Search for products
        domain = [['sale_ok', '=', True], ['type', 'in', ['product', 'consu']], ['company_id', '=', int(company_id)]]
        try:
            product_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'search', [domain])
        except Exception as e:
            log_event('Product Sync', 'Error', f"Search Failed: {e}", shop_url=shop_url)
            return

        # Batching
        BATCH_SIZE = 50
        chunks = [product_ids[i:i + BATCH_SIZE] for i in range(0, len(product_ids), BATCH_SIZE)]

        for index, batch_ids in enumerate(chunks):
            q_default.enqueue(sync_product_batch_task, shop_url, batch_ids, f"Batch {index+1}/{len(chunks)}", job_timeout=900)

        log_event('Product Sync', 'Success', f"Queued {len(chunks)} batches for {len(product_ids)} items.", shop_url=shop_url)

# =====================================================
# 2. THE WORKER
# =====================================================
def sync_product_batch_task(shop_url, batch_ids, batch_name):
    from app import app
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): return
        
        # Load Config
        cfg = {
            'title': get_config('prod_sync_title', True, shop_url=shop_url),
            'price': get_config('prod_sync_price', True, shop_url=shop_url),
            'cost': get_config('prod_sync_cost', True, shop_url=shop_url),
            'desc': get_config('prod_sync_desc', True, shop_url=shop_url),
            'tags': get_config('prod_sync_tags', False, shop_url=shop_url),
            'images': get_config('prod_sync_images', False, shop_url=shop_url),
            'vendor': get_config('prod_sync_vendor', True, shop_url=shop_url),
            'barcode': get_config('prod_sync_barcode', True, shop_url=shop_url),
            'currency': shopify.Shop.current().currency
        }

        # Fetch Data
        fields = ['default_code', 'name', 'list_price', 'standard_price', 'weight', 'active', 'uom_id', 'qty_per_pack', 'barcode', 'description_sale', 'product_tag_ids', 'public_categ_ids', 'image_1920']
        products = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'read', [batch_ids], {'fields': fields})

        processed = 0
        skipped = 0
        
        for p in products:
            if process_product_data(p, odoo, shop_url, cfg):
                processed += 1
            else:
                skipped += 1

        log_event('Product Sync', 'Info', f"✅ {batch_name}: Updated {processed} | Skipped {skipped} (Safety Mode)", shop_url=shop_url)
        gc.collect()

# =====================================================
# 3. SINGLE PRODUCT LOGIC (SAFETY MODE)
# =====================================================
def process_product_data(p, odoo, shop_url, cfg=None):
    from app import db
    
    # 🛑 SAFETY: STRIP WHITESPACE
    sku = str(p.get('default_code') or '').strip()
    if not sku: return False

    # 🛑 SAFETY: DISABLE CREATION HARDCODED
    # We only update if we find a match.
    allow_creation = False 

    # Find Product
    sid = find_shopify_product_by_sku(sku, shop_url)
    sp = None
    
    if sid:
        try: sp = shopify.Product.find(sid)
        except: sp = None
    
    # If not found -> SKIP IT
    if not sp:
        if allow_creation:
            # Creation logic removed for safety
            pass
        else:
            # print(f"⚠️ Skipping {sku} - Not found in Shopify and creation disabled.")
            return False

    # --- UPDATE LOGIC ---
    # Only runs if 'sp' exists (No duplicates possible)
    
    sp.published_scope = 'global' # Ensure visibility

    if cfg['title']: sp.title = p['name']
    if cfg['vendor']: 
        vendor = p['name'].split(' ')[0] if p['name'] else "Worthy"
        sp.vendor = vendor
        
    if cfg['desc']: sp.body_html = p.get('description_sale') or ''
    if cfg['tags']:
        t_names = odoo.get_tag_names(p.get('product_tag_ids', []))
        if t_names: sp.tags = ",".join(t_names)

    sp.save()

    # Variants
    desired_price = str(float(p.get('list_price', 0.0)))
    desired_cost = str(float(p.get('standard_price', 0.0)))
    desired_barcode = p.get('barcode', '')

    # Simple Variant Update (First Variant Only for now)
    if sp.variants:
        v = sp.variants[0]
        v.sku = sku # Ensure SKU is clean
        if cfg['price']: 
            v.price = desired_price
            v.compare_at_price = desired_price
        if cfg['barcode'] and desired_barcode: 
            v.barcode = desired_barcode
        
        # Save inventory ID for cost update
        inv_id = v.inventory_item_id
        v.save()

        # Update Cost
        if cfg['cost'] and inv_id:
            try:
                ii = shopify.InventoryItem.find(inv_id)
                ii.cost = desired_cost
                ii.tracked = True
                ii.save()
            except: pass
    
    # Update Map
    try:
        pm = ProductMap.query.filter_by(sku=sku).first()
        if not pm:
            vid = sp.variants[0].id if sp.variants else '0'
            pm = ProductMap(sku=sku, odoo_product_id=p['id'], shopify_variant_id=str(vid), shop_url=shop_url)
            db.session.add(pm)
        pm.last_synced_at = datetime.utcnow()
        db.session.commit()
    except: db.session.rollback()

    return True

# =====================================================
# 4. HELPERS & CLEANUP
# =====================================================
def find_shopify_product_by_sku(sku, shop_url):
    # 1. DB Lookup
    pm = ProductMap.query.filter_by(shop_url=shop_url, sku=sku).first()
    if pm and pm.shopify_variant_id and pm.shopify_variant_id != '0':
        try:
            variant = shopify.Variant.find(pm.shopify_variant_id)
            return variant.product_id
        except: pass

    # 2. API Search (Sanitized)
    try:
        # Strict search
        found = shopify.Product.search(query=f"sku:{sku}")
        if found: return found[0].id
    except: pass
    
    return None

# --- CLEANUP TOOLS (Kept for your manual button) ---
def cleanup_batch_task(shop_url, id_list, batch_name):
    from app import app
    with app.app_context():
        if not setup_shopify_session(shop_url): return
        count = 0
        for pid in id_list:
            try:
                p = shopify.Product.find(pid)
                p.status = 'archived'
                p.save()
                count += 1
            except: pass
        log_event('Cleanup', 'Info', f"{batch_name}: Archived {count} items.", shop_url=shop_url)

def cleanup_duplicates_master(shop_url):
    from app import app
    with app.app_context():
        if not setup_shopify_session(shop_url): return
        log_event('Cleanup', 'Info', "Scanning...", shop_url=shop_url)
        try:
            all_products = []
            page = shopify.Product.find(limit=250, status='active', fields="id,updated_at,variants")
            while page:
                all_products.extend(page)
                if page.has_next_page(): page = page.next_page()
                else: break
            
            sku_map = {}
            for p in all_products:
                if p.variants:
                    sku = p.variants[0].sku
                    if sku:
                        if sku not in sku_map: sku_map[sku] = []
                        sku_map[sku].append(p)
            
            ids_to_archive = []
            for sku, items in sku_map.items():
                if len(items) > 1:
                    items.sort(key=lambda x: x.updated_at, reverse=True)
                    ids_to_archive.extend([x.id for x in items[1:]])

            BATCH_SIZE = 50
            chunks = [ids_to_archive[i:i + BATCH_SIZE] for i in range(0, len(ids_to_archive), BATCH_SIZE)]
            for index, batch in enumerate(chunks):
                q_default.enqueue(cleanup_batch_task, shop_url, batch, f"Cleanup Batch {index+1}/{len(chunks)}")
            
            log_event('Cleanup', 'Success', f"Queued {len(chunks)} batches. (Found {len(ids_to_archive)} duplicates)", shop_url=shop_url)
        except Exception as e:
            log_event('Cleanup', 'Error', f"Scan Failed: {e}", shop_url=shop_url)

cleanup_shopify_products = cleanup_duplicates_master
archive_shopify_duplicates = cleanup_duplicates_master
