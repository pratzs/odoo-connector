import shopify
import json
import gc
import time
import hashlib
from difflib import SequenceMatcher
from datetime import datetime
from models import Shop, ProductMap, AppSetting, db
from utils import get_odoo_connection, log_event, setup_shopify_session, get_config, q_default


def safe_find_variant_by_sku(sku, retries=3):
    """
    TRIPLE-CHECK: robustly finds a variant by SKU.
    If Shopify blinks/throttles, it waits and tries again.
    """
    clean_sku = str(sku).strip()
    for attempt in range(retries):
        try:
            # Search for the variant by SKU
            variants = shopify.Variant.find(sku=clean_sku)
            
            # Filter results to ensure EXACT match (Shopify fuzzy search can be weird)
            exact_matches = [v for v in variants if v.sku == clean_sku]
            
            if exact_matches:
                return exact_matches[0] # Found it!
            
            # If not found, wait slightly longer before assuming it doesn't exist
            time.sleep(1)
            
        except Exception as e:
            print(f"Search Warning (Attempt {attempt+1}): {e}")
            time.sleep(2) # Wait longer on error
            
    return None # Truly not found after 3 tries


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

        # 2. Bulk Shopify Fetch
        shopify_product_cache = {} 
        variant_ids_to_fetch = []
        for pm in db_map_dict.values():
            if pm.shopify_variant_id and pm.shopify_variant_id != '0':
                variant_ids_to_fetch.append(pm.shopify_variant_id)

        if variant_ids_to_fetch:
            try:
                # Chunk IDs into 50s for Shopify API limit safety
                v_chunks = [variant_ids_to_fetch[i:i + 50] for i in range(0, len(variant_ids_to_fetch), 50)]
                product_ids_to_fetch = set()
                
                for v_chunk in v_chunks:
                    variants = shopify.Variant.find(ids=",".join(v_chunk))
                    for v in variants:
                        product_ids_to_fetch.add(str(v.product_id))
                    
                    # ✅ SAFETY BRAKE: Sleep 0.5s between Shopify calls
                    time.sleep(0.5) 

                if product_ids_to_fetch:
                    p_chunks = [list(product_ids_to_fetch)[i:i + 50] for i in range(0, len(product_ids_to_fetch), 50)]
                    for p_chunk in p_chunks:
                        s_products = shopify.Product.find(ids=",".join(p_chunk))
                        for sp in s_products:
                            for v in sp.variants:
                                if v.sku: shopify_product_cache[v.sku] = sp
                        
                        # ✅ SAFETY BRAKE: Sleep 0.5s between Shopify calls
                        time.sleep(0.5)

            except Exception as e:
                print(f"Batch Shopify Fetch Error: {e}")

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
# 3. SINGLE PRODUCT LOGIC (MEMORY OPTIMIZED)
# =====================================================
def process_product_data(p, odoo, shop_url, cfg, uom_map, categ_map, tag_map, db_map_dict, shopify_product_cache):
    from app import db
    
    if not p.get('active'): return "skipped"
    sku = str(p.get('default_code') or '').strip()
    if not sku: return "skipped"

    # 1. CHECK DB MAP (From Memory)
    pm = db_map_dict.get(sku)
    
    # Ownership Guard
    if pm and pm.odoo_product_id != p['id']:
        print(f"⚠️ BLOCKING: SKU {sku} already claimed by Odoo {pm.odoo_product_id}. Skipping.")
        return "skipped"

    product_name = p.get('name', 'Unknown')
    vendor_name = product_name.split(' ')[0] if product_name else "Worthy"

    # --- Variant / Pack Logic ---
    is_pack = False; ratio = 1.0; main_uom_name = 'Outer'
    if p.get('sh_is_secondary_unit') is True:
        qty_pack = p.get('qty_per_pack', 0.0)
        if qty_pack and float(qty_pack) > 1.0:
            is_pack = True; ratio = float(qty_pack); main_uom_name = f"{int(ratio)} per pack"
        elif p.get('sh_secondary_uom'):
            sec_id = p['sh_secondary_uom'][0]
            if sec_id in uom_map:
                ratio = uom_map[sec_id]['ratio']
                if ratio > 1.0: is_pack = True
    
    if p.get('uom_id'):
        u_id = p['uom_id'][0]
        if u_id in uom_map and (not main_uom_name or "per pack" not in main_uom_name):
             main_uom_name = uom_map[u_id]['name']
    
    if not main_uom_name: main_uom_name = "Outer"

    desired_variants = []
    raw_price = float(p.get('list_price', 0.0))
    raw_cost = float(p.get('standard_price', 0.0))
    barcode = p.get('barcode', '')

    if not is_pack:
        desired_variants.append({'option1': 'Default Title', 'price': str(raw_price), 'sku': sku, 'barcode': barcode, 'cost': str(raw_cost)})
    else:
        desired_variants.append({'option1': main_uom_name, 'price': str(raw_price), 'sku': sku, 'barcode': barcode, 'cost': str(raw_cost)})
        desired_variants.append({'option1': 'Unit', 'price': str(round(raw_price / ratio, 2) if ratio > 0 else 0.0), 'sku': f"{sku}-UNIT", 'barcode': '', 'cost': str(round(raw_cost / ratio, 2) if ratio > 0 else 0.0)})

    # ========================================================
    # 2. FIND SHOPIFY PRODUCT (From Memory Cache)
    # ========================================================
    sp = shopify_product_cache.get(sku)
    action_log = "updated"
    
    if not sp:
        # This will try 3 times, waiting between tries if Shopify errors out
        existing_variant = safe_find_variant_by_sku(sku)
        
        if existing_variant:
            # Check for collisions (ensure we aren't stealing another product's map)
            collision = ProductMap.query.filter_by(shopify_variant_id=str(existing_variant.id), shop_url=shop_url).first()
            
            # Only proceed if it's not mapped to a different Odoo ID
            if not collision or collision.odoo_product_id == p['id']:
                try:
                    # We found the variant, now get the parent product to update it
                    sp = shopify.Product.find(existing_variant.product_id)
                except:
                    sp = None

    # --- Create Logic ---
    if not sp:
        if not cfg['auto_create']: return "skipped"
        sp = shopify.Product(); sp.title = p['name']; sp.vendor = vendor_name
        sp.status = 'active' if cfg['auto_publish'] else 'draft'
        sp.published_scope = 'global'
        # Category from Memory
        if p.get('public_categ_ids'):
            cat_id = p['public_categ_ids'][0]
            if cat_id in categ_map: sp.product_type = categ_map[cat_id]
        
        if action_log != "archived": action_log = "created"

    if sp.status == 'archived': sp.status = 'active'

    # Updates
    if cfg['title']: sp.title = p['name']
    if cfg['vendor']: sp.vendor = vendor_name
    if cfg['desc']: sp.body_html = p.get('description_sale') or ''
    
    # Tags from Memory
    if cfg['tags'] and p.get('product_tag_ids'):
        t_names = [tag_map[tid] for tid in p['product_tag_ids'] if tid in tag_map]
        if t_names: sp.tags = ",".join(t_names)

    # Clean Options
    if is_pack:
        if not sp.options or sp.options[0].name != 'Pack Size':
            sp.options = [{'name': 'Pack Size'}]
    elif hasattr(sp, 'options') and sp.options and sp.options[0].name != 'Title':
        sp.options = [{'name': 'Title', 'values': ['Default Title']}]

    try:
        sp.save()
    except Exception as e:
        print(f"Save Error {sku}: {e}")
        return "error"

    # --- Variant Sync ---
    existing = getattr(sp, 'variants', [])
    final_vars = []
    for des in desired_variants:
        match = next((v for v in existing if v.sku == des['sku']), None)
        if not match and des['option1'] == 'Default Title' and len(existing) == 1:
             match = existing[0]
        if not match: match = shopify.Variant({'product_id': sp.id})
        
        match.option1 = des['option1']; match.sku = des['sku']
        if cfg['price']: 
            match.price = des['price']
            match.compare_at_price = des['price']
        if cfg['barcode'] and des['barcode']: match.barcode = des['barcode']
        match.inventory_management = 'shopify'
        final_vars.append(match)
    sp.variants = final_vars
    sp.save()

    # Cost Sync
    if sp.variants and cfg['cost']:
        for v in sp.variants:
            d = next((x for x in desired_variants if x['sku'] == v.sku), None)
            if d and v.inventory_item_id:
                try:
                    ii = shopify.InventoryItem.find(v.inventory_item_id)
                    ii.cost = d['cost']; ii.save()
                except: pass

    # Image Sync (Memory Hash Check)
    if cfg['images'] and p.get('image_1920'):
        try:
            img_raw = p['image_1920']
            if isinstance(img_raw, bytes): img_raw = img_raw.decode('utf-8')
            new_hash = hashlib.md5(img_raw.encode('utf-8')).hexdigest()
            
            # Check Hash from Memory Map (if available) or DB
            current_hash = pm.image_hash if pm else ""
            
            if new_hash != current_hash or not sp.images:
                if sp.images:
                    for old_img in sp.images:
                        try: shopify.Image.find(old_img.id, product_id=sp.id).destroy()
                        except: pass
                img = shopify.Image(prefix_options={'product_id': sp.id})
                img.attachment = img_raw; img.save()
                
                # Update DB (We need to query fresh to ensure session is attached)
                pm_update = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
                if pm_update:
                    pm_update.image_hash = new_hash
                    db.session.commit()
        except: pass

    # --- MAP UPDATE ---
    try:
        # Check if we already have the map in memory to avoid query if unchanged? 
        # Safest to query fresh for the write operation.
        pm_final = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
        if not pm_final:
            vid = sp.variants[0].id if sp.variants else '0'
            pm_final = ProductMap(sku=sku, odoo_product_id=p['id'], shopify_variant_id=str(vid), shop_url=shop_url)
            db.session.add(pm_final)
        
        if sp.variants:
            pm_final.shopify_variant_id = str(sp.variants[0].id)
            
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
