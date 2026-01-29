import shopify
import json
import gc
import time
import hashlib
import math
from difflib import SequenceMatcher
from datetime import datetime
from models import Shop, ProductMap, AppSetting, db
from utils import get_odoo_connection, log_event, setup_shopify_session, get_config, q_default


# =====================================================
# 0. HELPER FUNCTIONS (MUST BE AT TOP)
# =====================================================
def find_variant_in_cache(sku, shopify_product_cache):
    """
    STRICT LOCAL LOOKUP: Finds SKU in our pre-fetched cache.
    """
    clean_sku = str(sku).strip()
    return shopify_product_cache.get(clean_sku)



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

        # ✅ CRITICAL: Only sync active products to prevent "ghost" duplicates
        domain = [
            ['sale_ok', '=', True], 
            ['type', 'in', ['product', 'consu']], 
            ['company_id', '=', int(company_id)],
            ['active', '=', True] 
        ]
        
        try:
            product_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'search', [domain])
        except Exception as e:
            log_event('Product Sync', 'Error', f"Search Failed: {e}", shop_url=shop_url)
            return

        BATCH_SIZE = 50
        chunks = [product_ids[i:i + BATCH_SIZE] for i in range(0, len(product_ids), BATCH_SIZE)]

        for index, batch_ids in enumerate(chunks):
            q_default.enqueue(sync_product_batch_task, shop_url, batch_ids, f"Batch {index+1}/{len(chunks)}", job_timeout=3600)

        log_event('Product Sync', 'Success', f"Queued {len(chunks)} batches.", shop_url=shop_url)


# =====================================================
# 2. THE WORKER (OPTIMIZED WITH BULK FETCHING)
# =====================================================
def sync_product_batch_task(shop_url, batch_ids, batch_name):
    from app import app
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): return
        
        cfg = {
            'title': get_config('prod_sync_title', True, shop_url=shop_url),
            'price': get_config('prod_sync_price', True, shop_url=shop_url),
            'cost': get_config('prod_sync_cost', True, shop_url=shop_url),
            'desc': get_config('prod_sync_desc', True, shop_url=shop_url),
            'tags': get_config('prod_sync_tags', False, shop_url=shop_url),
            'images': get_config('prod_sync_images', False, shop_url=shop_url),
            'vendor': get_config('prod_sync_vendor', True, shop_url=shop_url),
            'barcode': get_config('prod_sync_barcode', True, shop_url=shop_url),
            'auto_create': get_config('prod_auto_create', False, shop_url=shop_url),
            'auto_publish': get_config('prod_auto_publish', False, shop_url=shop_url),
            'currency': shopify.Shop.current().currency
        }

        # --- A. PREFETCH ODOO DATA ---
        fields = ['default_code', 'name', 'list_price', 'standard_price', 'active', 
                  'uom_id', 'sh_is_secondary_unit', 'sh_secondary_uom', 
                  'public_categ_ids', 'product_tag_ids', 'description_sale', 
                  'image_1920', 'barcode', 'qty_per_pack']
        
        try:
            products = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'read', [batch_ids], {'fields': fields})
        except Exception as e:
            print(f"Odoo Batch Read Error: {e}")
            return

        # Bulk fetch Categories & Tags (Reduces Odoo calls from 2000 to ~40)
        all_categ_ids = set()
        all_tag_ids = set()
        for p in products:
            all_categ_ids.update(p.get('public_categ_ids', []))
            all_tag_ids.update(p.get('product_tag_ids', []))
            
        categ_map = {}
        if all_categ_ids:
            try:
                cats = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.public.category', 'read', [list(all_categ_ids)], {'fields': ['name']})
                categ_map = {c['id']: c['name'] for c in cats}
            except: pass

        tag_map = {}
        if all_tag_ids:
            try:
                tags = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.tag', 'read', [list(all_tag_ids)], {'fields': ['name']})
                tag_map = {t['id']: t['name'] for t in tags}
            except: pass
            
        uom_map = {}
        try:
            uoms = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'uom.uom', 'search_read', [], {'fields': ['id', 'name', 'factor_inv']})
            for u in uoms:
                uom_map[u['id']] = {'name': u['name'], 'ratio': float(u.get('factor_inv', 1.0))}
        except: pass

       # --- B. PREFETCH SHOPIFY DATA ---
        batch_skus = [str(p.get('default_code')).strip() for p in products if p.get('default_code')]
        
        # 1. Bulk DB Lookup (Reduces DB queries)
        db_map_dict = {} 
        if batch_skus:
            maps = ProductMap.query.filter(ProductMap.shop_url == shop_url, ProductMap.sku.in_(batch_skus)).all()
            db_map_dict = {m.sku: m for m in maps}

# --- B. PREFETCH SHOPIFY DATA (TOTAL SCAN STRATEGY) ---
        batch_skus = [str(p.get('default_code')).strip() for p in products if p.get('default_code')]
        
        # 1. Bulk DB Lookup
        db_map_dict = {} 
        if batch_skus:
            maps = ProductMap.query.filter(ProductMap.shop_url == shop_url, ProductMap.sku.in_(batch_skus)).all()
            db_map_dict = {m.sku: m for m in maps}

        # 2. Total Store SKU Map (Bypasses Broken Shopify Search)
        # We build a dictionary of { "SKU": ShopifyProductObject }
        shopify_product_cache = {} 
        try:
            # We fetch all active products to ensure we see A0001, etc.
            page = shopify.Product.find(limit=250, status='active')
            while page:
                for sp in page:
                    for v in sp.variants:
                        if v.sku:
                            # Store by stripped SKU for exact matching
                            shopify_product_cache[str(v.sku).strip()] = sp
                
                if page.has_next_page():
                    page = page.next_page()
                    time.sleep(0.5) # Safety brake
                else:
                    break
            
            print(f"✅ Local Cache Built: Loaded {len(shopify_product_cache)} SKUs from Shopify.")
        except Exception as e:
            print(f"❌ Failed to build Shopify cache: {e}")
            

        # --- C. PROCESS LOOP ---
        stats = {'created': 0, 'updated': 0, 'archived': 0, 'skipped': 0}

        for p in products:
            try:
                # We pass the new maps into the processor
                res = process_product_data(p, odoo, shop_url, cfg, uom_map, categ_map, tag_map, db_map_dict, shopify_product_cache)
                
                if 'archived' in res: stats['archived'] += 1
                elif 'created' in res: stats['created'] += 1
                elif 'updated' in res: stats['updated'] += 1
                else: stats['skipped'] += 1
            except Exception as e:
                print(f"Error syncing {p.get('default_code')}: {e}")

        gc.collect()
        log_event('Product Sync', 'Info', f"✅ {batch_name}: New: {stats['created']}, Updated: {stats['updated']}", shop_url=shop_url)

# =====================================================
# 3. SINGLE PRODUCT LOGIC (Updated for Specific Secondary UOMs)
# =====================================================
def process_product_data(p, odoo, shop_url, cfg, uom_map, categ_map, tag_map, db_map_dict, shopify_product_cache):
    from app import db
    
    if not p.get('active'): return "skipped"
    sku = str(p.get('default_code') or '').strip()
    if not sku: return "skipped"

    # 1. CHECK DB MAP
    pm = db_map_dict.get(sku)
    if pm and pm.odoo_product_id != p['id']:
        print(f"⚠️ BLOCKING: SKU {sku} already claimed by Odoo {pm.odoo_product_id}. Skipping.")
        return "skipped"

    product_name = p.get('name', 'Unknown')
    vendor_name = product_name.split(' ')[0] if product_name else "Worthy"

    # --- UPDATED: Variant / Pack Logic ---
    is_pack = False
    
    # 1. Determine Main Ratio (The "Qty per Pack" on the main product)
    # If list_price is for 24 units, main_ratio is 24.
    main_ratio = float(p.get('qty_per_pack', 1.0))
    if main_ratio < 1.0: main_ratio = 1.0
    
    # 2. Determine Secondary Ratio (The "Picking UOM" or "Secondary UOM")
    sec_ratio = 1.0
    sec_name = "Unit"
    has_secondary = False

    # Check if a specific Secondary UOM is selected in Odoo (e.g., CTNX6)
    if p.get('sh_is_secondary_unit') and p.get('sh_secondary_uom'):
        sec_uom_data = p['sh_secondary_uom'] # Returns [id, "Name"]
        sec_uom_id = sec_uom_data[0]
        
        if sec_uom_id in uom_map:
            sec_ratio = uom_map[sec_uom_id]['ratio']
            sec_name = uom_map[sec_uom_id]['name']
            has_secondary = True
    
    # Logic: It is a pack if Main Ratio > 1 OR we explicitly have a secondary unit
    if main_ratio > 1.0 or has_secondary:
        is_pack = True

    # 3. Determine Names
    main_uom_name = "Outer"
    if p.get('uom_id'):
        u_id = p['uom_id'][0]
        if u_id in uom_map:
            main_uom_name = uom_map[u_id]['name']
            # If the Odoo UOM name is just "Units" but ratio is 24, force a descriptive name
            if main_ratio > 1 and "per pack" not in main_uom_name.lower():
                main_uom_name = f"{int(main_ratio)} per pack"

    desired_variants = []
    raw_price = float(p.get('list_price', 0.0))
    raw_cost = float(p.get('standard_price', 0.0))
    barcode = p.get('barcode', '')

    if not is_pack:
        # Simple Product (1 Variant)
        desired_variants.append({'option1': 'Default Title', 'price': str(raw_price), 'sku': sku, 'barcode': barcode, 'cost': str(raw_cost)})
    else:
        # Pack Product (2 Variants)
        
        # A. Main Variant (The Parent)
        desired_variants.append({
            'option1': main_uom_name, 
            'price': str(raw_price), 
            'sku': sku, 
            'barcode': barcode, 
            'cost': str(raw_cost)
        })
        
        # B. Secondary Variant (The Breakdown)
        # Price Calc: (Total Price / Main Qty) * Secondary Qty
        # Ex: ($73.73 / 24) * 6 = $18.43
        unit_price_base = raw_price / main_ratio
        unit_cost_base = raw_cost / main_ratio
        
        sec_price = round(unit_price_base * sec_ratio, 2)
        sec_cost = round(unit_cost_base * sec_ratio, 2)
        
        # SKU Suffix: OBBA24C-CTNX6 (or -UNIT if ratio is 1)
        suffix = f"-{sec_name.replace(' ', '')}" if sec_ratio > 1 else "-UNIT"
        
        desired_variants.append({
            'option1': sec_name, 
            'price': str(sec_price), 
            'sku': f"{sku}{suffix}", 
            'barcode': '', # Usually secondary units don't share the outer barcode
            'cost': str(sec_cost)
        })

    # ========================================================
    # 2. FIND SHOPIFY PRODUCT
    # ========================================================
    sp = None
    action_log = "updated"

    if sku in shopify_product_cache:
        sp = shopify_product_cache[sku]

    if not sp:
        if pm and pm.shopify_variant_id and pm.shopify_variant_id != '0':
            try:
                v = shopify.Variant.find(pm.shopify_variant_id)
                sp = shopify.Product.find(v.product_id)
            except: pm = None

        if not sp:
            existing_variant = safe_find_variant_by_sku(sku)
            if existing_variant:
                try: sp = shopify.Product.find(existing_variant.product_id)
                except: sp = None

    # RESCUE GUARD
    if not sp:
        try:
            candidates = shopify.Product.find(title=sku, status='any')
            for c in candidates:
                if any(str(v.sku).strip() == sku for v in c.variants):
                    sp = c; break
            if not sp:
                clean_word = "".join(ch for ch in p['name'].split()[0] if ch.isalnum())
                candidates = shopify.Product.find(title=clean_word, status='any', limit=250)
                for c in candidates:
                    if any(str(v.sku).strip() == sku for v in c.variants):
                        sp = c; break
        except: pass

    # ========================================================
    # 3. EXECUTE (Create or Update)
    # ========================================================
    if not sp:
        if not cfg['auto_create']: return "skipped"
        sp = shopify.Product(); sp.title = p['name']; sp.vendor = vendor_name
        sp.status = 'active' if cfg['auto_publish'] else 'draft'
        
        if p.get('public_categ_ids'):
            cat_id = p['public_categ_ids'][0]
            if cat_id in categ_map: sp.product_type = categ_map[cat_id]
            
        if action_log != "archived": action_log = "created"

    if sp.status == 'archived': sp.status = 'active'

    if cfg['title']: sp.title = p['name']
    if cfg['vendor']: sp.vendor = vendor_name
    if cfg['desc']: sp.body_html = p.get('description_sale') or ''
    if cfg['tags'] and p.get('product_tag_ids'):
        t_names = [tag_map[tid] for tid in p['product_tag_ids'] if tid in tag_map]
        if t_names: sp.tags = ",".join(t_names)

    # Clean Options
    if is_pack:
        if not sp.options or sp.options[0].name != 'Pack Size':
            sp.options = [{'name': 'Pack Size'}]
    elif hasattr(sp, 'options') and sp.options and sp.options[0].name != 'Title':
        sp.options = [{'name': 'Title', 'values': ['Default Title']}]

    try: sp.save()
    except Exception as e:
        print(f"Save Error {sku}: {e}")
        return "error"

    # --- Variant Sync ---
    existing = getattr(sp, 'variants', [])
    final_vars = []
    
    for des in desired_variants:
        match = next((v for v in existing if v.sku == des['sku']), None)
        
        # Logic to reuse the Default Variant if we are switching structures
        if not match and des['option1'] == 'Default Title' and len(existing) == 1:
             match = existing[0]
        
        if not match: 
            match = shopify.Variant({'product_id': sp.id})
        
        match.option1 = des['option1']
        match.sku = des['sku']
        
        if cfg['price']: 
            match.price = des['price']
            match.compare_at_price = des['price']
        
        # Only sync barcode for the MAIN unit, usually secondary units in Odoo don't share the same barcode
        if cfg['barcode'] and des['barcode']: 
            match.barcode = des['barcode']
            
        match.inventory_management = 'shopify'
        final_vars.append(match)
    
    sp.variants = final_vars
    try: sp.save()
    except Exception as e:
        print(f"Variant Save Error {sku}: {e}")
        return "error"

    # ========================================================
    # 4. MAP UPDATE
    # ========================================================
    if not sp.variants: return "error"
        
    valid_vid = str(sp.variants[0].id)
    if valid_vid == '0' or not valid_vid: return "error"

    # Image Sync
    if cfg['images'] and p.get('image_1920'):
        try:
            img_raw = p['image_1920']
            if isinstance(img_raw, bytes): img_raw = img_raw.decode('utf-8')
            new_hash = hashlib.md5(img_raw.encode('utf-8')).hexdigest()
            current_hash = pm.image_hash if pm else ""
            
            if new_hash != current_hash or not sp.images:
                if sp.images:
                    for old_img in sp.images:
                        try: shopify.Image.find(old_img.id, product_id=sp.id).destroy()
                        except: pass
                img = shopify.Image(prefix_options={'product_id': sp.id})
                img.attachment = img_raw; img.save()
                
                pm_upd = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
                if pm_upd:
                    pm_upd.image_hash = new_hash; db.session.commit()
        except: pass

    try:
        pm_final = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
        if not pm_final:
            pm_final = ProductMap(sku=sku, odoo_product_id=p['id'], shopify_variant_id=valid_vid, shop_url=shop_url)
            db.session.add(pm_final)
        else:
            pm_final.shopify_variant_id = valid_vid
            
        pm_final.last_synced_at = datetime.utcnow()
        db.session.commit()
    except: db.session.rollback()

    return action_log


# =====================================================
# 4. HELPERS
# =====================================================
def find_shopify_product_by_sku(sku, shop_url):
    pm = ProductMap.query.filter_by(shop_url=shop_url, sku=sku).first()
    if pm and pm.shopify_variant_id and pm.shopify_variant_id != '0':
        try:
            variant = shopify.Variant.find(pm.shopify_variant_id)
            return variant.product_id
        except: pass
    return None

def archive_shopify_duplicates(shop_url):
    """
    Scans all Shopify products. If multiple products share the same SKU,
    it keeps the one linked in ProductMap and archives the others.
    """
    from app import app
    
    with app.app_context():
        if not setup_shopify_session(shop_url):
            return

        log_event('Duplicate Cleanup', 'Info', "Starting scan for duplicate SKUs...", shop_url=shop_url)

        # 1. Fetch all Active Products
        # We only care about cleaning up active mess.
        page = shopify.Product.find(limit=250, status='active')
        sku_tracker = {} # Format: { 'SKU123': [prod_obj_1, prod_obj_2] }
        
        while page:
            for p in page:
                # We assume the first variant holds the master SKU
                if not p.variants: continue
                sku = p.variants[0].sku
                
                if not sku: continue
                
                if sku not in sku_tracker:
                    sku_tracker[sku] = []
                sku_tracker[sku].append(p)
                
            if page.has_next_page():
                page = page.next_page()
            else:
                break

        # 2. Analyze & Archive
        archived_count = 0
        
        for sku, products in sku_tracker.items():
            if len(products) > 1:
                # FOUND DUPLICATES!
                
                # A. Identify the "Master" (The one in our DB)
                pm = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
                master_id = int(pm.shopify_variant_id) if pm else 0
                
                # If we have a map, find the product that owns that variant
                master_product = None
                
                # Try to find the mapped product in the list
                if master_id:
                    for p in products:
                        # Check if any variant of this product matches the DB ID
                        if any(str(v.id) == str(master_id) for v in p.variants):
                            master_product = p
                            break
                
                # Fallback: If no map (or map is wrong), keep the most recently updated one
                if not master_product:
                    # Sort by updated_at descending (newest first)
                    products.sort(key=lambda x: x.updated_at, reverse=True)
                    master_product = products[0]

                # B. Archive the losers
                for p in products:
                    if p.id != master_product.id:
                        try:
                            p.status = 'archived'
                            p.save()
                            archived_count += 1
                            print(f"📦 Archived Duplicate: {p.title} (ID: {p.id}) - Kept: {master_product.id}")
                        except Exception as e:
                            print(f"Failed to archive {p.id}: {e}")

        log_event('Duplicate Cleanup', 'Success', f"Scan complete. Archived {archived_count} duplicates.", shop_url=shop_url)

cleanup_duplicates_master = archive_shopify_duplicates
cleanup_shopify_products = archive_shopify_duplicates


# =====================================================
# 5. FORCE IMAGE SYNC (STRICT REPLACEMENT)
# =====================================================
def sync_images_only_manual(shop_url):
    """
    Dispatcher for the 'Force Image Sync' button.
    Loops through the ProductMap and enqueues individual image repairs.
    """
    from app import app
    with app.app_context():
        # 1. Get all mapped products for this shop
        mapped_products = ProductMap.query.filter_by(shop_url=shop_url).all()
        
        if not mapped_products:
            log_event('Force Image Sync', 'Info', "No products found in map to sync.", shop_url=shop_url)
            return

        # 2. Chunk them to avoid overloading the queue
        for pm in mapped_products:
            q_default.enqueue(repair_single_product_image, shop_url, pm.sku, job_timeout=300)

        log_event('Force Image Sync', 'Success', f"Queued image repair for {len(mapped_products)} products.", shop_url=shop_url)


def repair_single_product_image(shop_url, sku):
    """
    The Worker: Strictly replaces Shopify images with Odoo images.
    """
    from app import app
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): return

        # 1. Find the Shopify Product
        shopify_product_id = find_shopify_product_by_sku(sku, shop_url)
        if not shopify_product_id: return

        try:
            sp = shopify.Product.find(shopify_product_id)
            
            # 2. Fetch fresh image from Odoo
            # We search for the product to get the latest image_1920
            odoo_p = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'search_read', 
                [[['default_code', '=', sku], ['active', '=', True]]], 
                {'fields': ['image_1920'], 'limit': 1}
            )

            if not odoo_p or not odoo_p[0].get('image_1920'):
                print(f"Skipping {sku}: No image found in Odoo.")
                return

            # 3. THE WIPE: Delete ALL existing images on Shopify first
            if sp.images:
                print(f"🧹 Wiping {len(sp.images)} images for {sku}...")
                for img in sp.images:
                    try:
                        shopify.Image.find(img.id, product_id=sp.id).destroy()
                    except: pass

            # 4. THE INJECTION: Upload the fresh Odoo image
            new_image = shopify.Image(prefix_options={'product_id': sp.id})
            img_data = odoo_p[0]['image_1920']
            if isinstance(img_data, bytes): img_data = img_data.decode('utf-8')
            
            new_image.attachment = img_data
            new_image.save()

            # 5. Update the Hash in DB so the regular sync doesn't try to "fix" it again
            new_hash = hashlib.md5(img_data.encode('utf-8')).hexdigest()
            pm = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
            if pm:
                pm.image_hash = new_hash
                db.session.commit()
                
            print(f"✅ Image for {sku} replaced successfully.")

        except Exception as e:
            print(f"❌ Error repairing image for {sku}: {e}")


# =====================================================
# 6. STRICT VARIANT REPAIR (The "Fix Mess" Tool)
# =====================================================
def fix_variant_mess_task(shop_url, company_id):
    """
    Scans Shopify Products.
    If SKU matches Odoo:
    1. Enforces strict Variant list (Pack vs Unit).
    2. Updates Prices.
    3. Updates Stock Levels (Floor calculation for Packs).
    4. Deletes any variant not defined in the logic.
    """
    from app import app
    with app.app_context():
        # 1. Setup
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): 
            log_event('Cleanup', 'Error', "Startup Failed: No Odoo/Shopify Connection", shop_url=shop_url)
            return
        
        log_event('Cleanup', 'Info', f"Starting Strict Variant Repair for Company {company_id}...", shop_url=shop_url)
        
        # 2. Fetch Odoo Data (Active Products Only)
        try:
            domain = [['sale_ok', '=', True], ['type', 'in', ['product', 'consu']], 
                      ['company_id', '=', int(company_id)], ['active', '=', True]]
            
            # Added 'qty_available' to fetch stock
            fields = ['default_code', 'name', 'list_price', 'standard_price', 
                      'sh_is_secondary_unit', 'qty_per_pack', 'qty_available']
            
            odoo_products = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'search_read', [domain], {'fields': fields})
            
            # Map by SKU for fast lookup
            odoo_map = {str(p.get('default_code')).strip(): p for p in odoo_products if p.get('default_code')}
            
        except Exception as e:
            log_event('Cleanup', 'Error', f"Odoo Data Fetch Failed: {e}", shop_url=shop_url)
            return

        # 3. Scan Shopify Products
        page = shopify.Product.find(limit=50, status='active')
        processed = 0
        repaired = 0
        
        while page:
            for sp in page:
                # Identify Parent SKU from first variant
                if not sp.variants: continue
                ref_sku = str(sp.variants[0].sku).replace("-UNIT", "").strip()
                
                if not ref_sku or ref_sku not in odoo_map:
                    continue # Skip if not found in Odoo

                p = odoo_map[ref_sku]
                processed += 1
                
                # --- LOGIC: PACK vs UNIT ---
                is_pack = False
                qty_pack = float(p.get('qty_per_pack', 0.0))
                if p.get('sh_is_secondary_unit') is True and qty_pack > 1.0:
                    is_pack = True

                # Prices
                pack_price = float(p.get('list_price', 0.0))
                pack_cost = float(p.get('standard_price', 0.0))
                unit_price = round(pack_price / qty_pack, 2) if is_pack else 0.0
                unit_cost = round(pack_cost / qty_pack, 2) if is_pack else 0.0

                # Stock (Qty on Hand)
                raw_stock = int(p.get('qty_available', 0))
                pack_stock = math.floor(raw_stock / qty_pack) if is_pack else 0
                
                # Define Desired Structure
                desired_variants = []
                
                if is_pack:
                    sp.options = [{'name': 'Pack Size'}] # Enforce Option Name
                    
                    # 1. The Pack (Outer)
                    desired_variants.append({
                        'sku': ref_sku,
                        'option1': f"{int(qty_pack)} per pack",
                        'price': str(pack_price),
                        'cost': str(pack_cost),
                        'stock': pack_stock
                    })
                    
                    # 2. The Unit (Single)
                    desired_variants.append({
                        'sku': f"{ref_sku}-UNIT",
                        'option1': "Unit",
                        'price': str(unit_price),
                        'cost': str(unit_cost),
                        'stock': raw_stock # Unit stock is raw stock
                    })
                else:
                    # Simple Product
                    if sp.options and sp.options[0].name != 'Title':
                        sp.options = [{'name': 'Title', 'values': ['Default Title']}]
                    
                    desired_variants.append({
                        'sku': ref_sku,
                        'option1': "Default Title",
                        'price': str(pack_price),
                        'cost': str(pack_cost),
                        'stock': raw_stock
                    })

                # --- EXECUTE UPDATE ---
                current_variants = sp.variants
                final_list = []
                dirty = False

                for target in desired_variants:
                    # Find existing or create new
                    match = next((v for v in current_variants if v.sku == target['sku']), None)
                    
                    # Fallback for switching from Pack -> Single (Default Title match)
                    if not match and target['option1'] == 'Default Title' and current_variants:
                        match = current_variants[0]

                    if not match:
                        match = shopify.Variant({'product_id': sp.id})
                        match.inventory_management = 'shopify'
                        dirty = True

                   # Update core fields (Using getattr to prevent AttributeError on new objects)
                    current_opt1 = getattr(match, 'option1', None)
                    current_price = getattr(match, 'price', None)
                    current_sku = getattr(match, 'sku', None)

                    if current_opt1 != target['option1']: 
                        match.option1 = target['option1']
                        dirty = True
                    
                    if str(current_price) != str(target['price']): 
                        match.price = target['price']
                        dirty = True
                    
                    if current_sku != target['sku']: 
                        match.sku = target['sku']
                        dirty = True
                    
                    final_list.append(match)

                # Detect Deletions (If Shopify has more than Desired)
                if len(current_variants) > len(final_list):
                    dirty = True

                if dirty:
                    sp.variants = final_list
                    try:
                        if sp.save():
                            repaired += 1
                            # POST-SAVE: Update Stock & Cost (Requires InventoryItem ID)
                            location_id = get_config('shopify_location_id', None, shop_url=shop_url)
                            if location_id:
                                for v in sp.variants:
                                    target = next((t for t in desired_variants if t['sku'] == v.sku), None)
                                    if target:
                                        # Update Cost
                                        if v.inventory_item_id:
                                            try:
                                                ii = shopify.InventoryItem.find(v.inventory_item_id)
                                                ii.cost = target['cost']
                                                ii.save()
                                            except: pass
                                        
                                        # Update Stock
                                        try:
                                            shopify.InventoryLevel.set(
                                                location_id=location_id,
                                                inventory_item_id=v.inventory_item_id,
                                                available=target['stock']
                                            )
                                        except: pass
                    except Exception as e:
                        print(f"Failed to fix {ref_sku}: {e}")

            if page.has_next_page():
                page = page.next_page()
            else:
                break

        log_event('Cleanup', 'Success', f"Done. Scanned {processed}, Repaired {repaired}.", shop_url=shop_url)
