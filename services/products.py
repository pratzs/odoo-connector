import shopify
import json
import gc
import time
import hashlib
import math
from difflib import SequenceMatcher
from datetime import datetime, timedelta
from models import Shop, ProductMap, AppSetting, db
from utils import get_odoo_connection, log_event, setup_shopify_session, get_config, q_default


# =====================================================
# 0. HELPER FUNCTIONS
# =====================================================
def find_variant_in_cache(sku, shopify_product_cache):
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

        # Look back 2 days for routine syncs to prevent memory crashes
        cutoff = datetime.utcnow() - timedelta(days=2)
        cutoff_str = cutoff.strftime('%Y-%m-%d %H:%M:%S')

        domain = [
            ['sale_ok', '=', True], 
            ['type', 'in', ['product', 'consu']], 
            ['active', '=', True], 
            ['write_date', '>=', cutoff_str], # <--- Delta Sync Gatekeeper
            '|', 
            ['company_id', '=', int(company_id)],
            ['company_id', '=', False]
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
# 1.5 THE ABSOLUTE MASTER (FULL CATALOG RESYNC)
# =====================================================
def sync_all_products_absolute_master(shop_url):
    from app import app
    with app.app_context():
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop: return
        
        odoo = get_odoo_connection(shop_url)
        if not odoo: return

        company_id = shop.odoo_company_id
        if not company_id: return

        log_event('Product Sync', 'Warning', "INITIATING FULL CATALOG RESYNC (HEAVY). Bypassing 48-hour rule.", shop_url=shop_url)

        # NO TIME LIMIT - GRAB EVERYTHING ACTIVE
        domain = [
            ['sale_ok', '=', True], 
            ['type', 'in', ['product', 'consu']], 
            ['active', '=', True], 
            '|', 
            ['company_id', '=', int(company_id)],
            ['company_id', '=', False]
        ]
        
        try:
            product_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'search', [domain])
        except Exception as e:
            log_event('Product Sync', 'Error', f"Full Catalog Search Failed: {e}", shop_url=shop_url)
            return

        BATCH_SIZE = 50
        chunks = [product_ids[i:i + BATCH_SIZE] for i in range(0, len(product_ids), BATCH_SIZE)]

        for index, batch_ids in enumerate(chunks):
            # We reuse the exact same reliable batch worker we already use!
            q_default.enqueue(sync_product_batch_task, shop_url, batch_ids, f"Full Resync Batch {index+1}/{len(chunks)}", job_timeout=3600)

        log_event('Product Sync', 'Success', f"Queued FULL CATALOG sync. {len(chunks)} batches processing.", shop_url=shop_url)


# =====================================================
# 2. THE WORKER (OPTIMIZED)
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
            'meta_original_price': get_config('prod_sync_meta_original_price', False, shop_url=shop_url),
            'meta_vendor_code': get_config('prod_sync_meta_vendor_code', False, shop_url=shop_url),
            'currency': shopify.Shop.current().currency
        }

        # --- A. PREFETCH ODOO DATA ---
        fields = ['default_code', 'name', 'list_price', 'standard_price', 'active', 
                  'uom_id', 'sh_is_secondary_unit', 'sh_secondary_uom', 
                  'public_categ_ids', 'product_tag_ids', 'description_sale', 
                  'image_1920', 'barcode', 'qty_per_pack', 'product_tmpl_id']
        
        try:
            products = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'read', [batch_ids], {'fields': fields})
        except Exception as e:
            print(f"Odoo Batch Read Error: {e}")
            return

        # Bulk fetch Categories, Tags & UOMs
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

        # --- NEW: PREFETCH VENDOR CODES ---
        vendor_code_map = {}
        if cfg.get('meta_vendor_code'):
            tmpl_ids = [p['product_tmpl_id'][0] for p in products if p.get('product_tmpl_id')]
            if tmpl_ids:
                try:
                    supplier_info = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.supplierinfo', 'search_read',
                        [[['product_tmpl_id', 'in', tmpl_ids]]], {'fields': ['product_tmpl_id', 'product_code']})
                    for info in supplier_info:
                        if info.get('product_code') and info.get('product_tmpl_id'):
                            # Map the Template ID to the Vendor Code
                            vendor_code_map[info['product_tmpl_id'][0]] = info['product_code']
                except Exception as e:
                    print(f"Vendor Code Fetch Error: {e}")
                    
        # Inject the vendor code into the product dictionary so process_product_data can see it
        for p in products:
            if p.get('product_tmpl_id'):
                p['vendor_code'] = vendor_code_map.get(p['product_tmpl_id'][0], '')

        # --- B. PREFETCH SHOPIFY DATA ---
        batch_skus = [str(p.get('default_code')).strip() for p in products if p.get('default_code')]
        
        # 1. Bulk DB Lookup (Keep this, it's efficient)
        db_map_dict = {} 
        if batch_skus:
            maps = ProductMap.query.filter(ProductMap.shop_url == shop_url, ProductMap.sku.in_(batch_skus)).all()
            db_map_dict = {m.sku: m for m in maps}
            
        # --- C. PROCESS LOOP ---
        stats = {'created': 0, 'updated': 0, 'archived': 0, 'skipped': 0}

        for p in products:
            try:
                # --- OPTIMIZATION: TARGETED LOOKUP ---
                # Instead of a massive cache, we find JUST this product.
                sku = str(p.get('default_code')).strip()
                single_item_cache = {}

                if sku:
                    # 1. Try to find ID via our helper (uses GraphQL or Search)
                    shopify_id = find_shopify_product_by_sku(sku, shop_url=shop_url)
                    
                    if shopify_id:
                        try:
                            # 2. Fetch the specific product object
                            sp_product = shopify.Product.find(shopify_id)
                            single_item_cache[sku] = sp_product
                        except:
                            pass

                # Pass the single-item cache to your existing processor
                res = process_product_data(p, odoo, shop_url, cfg, uom_map, categ_map, tag_map, db_map_dict, single_item_cache)
                
                if 'archived' in res: stats['archived'] += 1
                elif 'created' in res: stats['created'] += 1
                elif 'updated' in res: stats['updated'] += 1
                else: stats['skipped'] += 1

            except Exception as e:
                print(f"Error syncing {p.get('default_code')}: {e}")

        gc.collect()
        log_event('Product Sync', 'Info', f"✅ {batch_name}: New: {stats['created']}, Updated: {stats['updated']}", shop_url=shop_url)

# =====================================================
# 3. SINGLE PRODUCT LOGIC
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

    # --- UPDATED: Strict Variant Logic ---
    # --- UPDATED: Strict Variant Logic ---
    is_pack = False
    
    main_ratio = float(p.get('qty_per_pack', 1.0))
    if main_ratio < 1.0: main_ratio = 1.0
    
    sec_ratio = 1.0
    sec_name = "Unit"
    
    # 1. Check if Secondary Unit is active
    if p.get('sh_is_secondary_unit'):
        if p.get('sh_secondary_uom'):
            sec_data = p['sh_secondary_uom'] # [id, "Name"]
            
            # A. Get Name (Try Odoo data first, then map)
            if len(sec_data) > 1 and sec_data[1]:
                sec_name = str(sec_data[1])
            elif sec_data[0] in uom_map:
                sec_name = uom_map[sec_data[0]]['name']

            # B. Get Ratio (from Map)
            if sec_data[0] in uom_map:
                sec_ratio = uom_map[sec_data[0]]['ratio']
        
       # 2. Determine if it is a pack
        if main_ratio > 1.0 or sec_ratio != 1.0:
            is_pack = True
            
            # 3. FORCE FORMATTING: "6 per pack"
            if sec_ratio > 1.0:
                sec_name = f"{int(sec_ratio)} per pack"

    
    # 3. Main UOM Name
    main_uom_name = "Outer"
    if p.get('uom_id') and p['uom_id'][0] in uom_map:
        raw_name = uom_map[p['uom_id'][0]]['name']
        # FIX: Ensure it's a string to prevent .lower() crash
        main_uom_name = str(raw_name) if raw_name else "Outer"
        
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
        unit_price = round((raw_price / main_ratio) * sec_ratio, 2)
        unit_cost = round((raw_cost / main_ratio) * sec_ratio, 2)
        
        suffix = f"-{sec_name.replace(' ', '')}" if sec_ratio > 1 else "-UNIT"
        
        desired_variants.append({
            'option1': sec_name, 
            'price': str(unit_price), 
            'sku': f"{sku}{suffix}", 
            'barcode': '', 
            'cost': str(unit_cost)
        })

   # --- 2. FIND SHOPIFY PRODUCT (STRICT) ---
    sp = None
    
    # Priority 1: Direct ID from DB (The most reliable link)
    if pm and pm.shopify_variant_id:
        try:
            variant = shopify.Variant.find(pm.shopify_variant_id)
            sp = shopify.Product.find(variant.product_id)
        except:
            sp = None

    # Priority 2: GraphQL strict SKU search (if DB link is missing)
    if not sp:
        sp_id = find_shopify_product_by_sku(sku, shop_url)
        if sp_id:
            try:
                sp = shopify.Product.find(sp_id)
            except:
                sp = None

    # ========================================================
    # 3. EXECUTE (Create or Update)
    # ========================================================
    action_log = "updated"  # Default state

    if not sp:
        if not cfg['auto_create']:
            log_event('Product Sync', 'Warning', f"Skipped '{sku}' - Auto-Create is OFF.", shop_url=shop_url)
            return "skipped"

        # Final race-condition safety check before creating
        try:
            client = shopify.GraphQL()
            safety_check = json.loads(client.execute(
                '{ productVariants(first: 1, query: "sku:\'%s\'") { edges { node { sku product { legacyResourceId } } } } }'
                % sku.replace("'", "\\'")))
            edges = safety_check.get('data', {}).get('productVariants', {}).get('edges', [])
            for edge in edges:
                if edge.get('node', {}).get('sku', '').strip() == sku:
                    existing_id = int(edge['node']['product']['legacyResourceId'])
                    sp = shopify.Product.find(existing_id)
                    log_event('Product Sync', 'Info',
                        f"Race condition caught for '{sku}' — updating existing product.",
                        shop_url=shop_url)
                    break
        except Exception as e:
            print(f"Safety check error for {sku}: {e}")

        if not sp:
            log_event('Product Sync', 'Info', f"Creating NEW product in Shopify: {sku}", shop_url=shop_url)
            sp = shopify.Product()
            sp.title = p['name']
            sp.vendor = vendor_name
            sp.status = 'active' if cfg['auto_publish'] else 'draft'

            if p.get('public_categ_ids'):
                cat_id = p['public_categ_ids'][0]
                if cat_id in categ_map:
                    sp.product_type = categ_map[cat_id]

            action_log = "created"
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

    # 1. Save the core Shopify Product first
    try: 
        sp.save()
    except Exception as e:
        print(f"Save Error {sku}: {e}")
        return "error"

    # 2. Explicitly handle Metafields (Using Shopify's Recommended GraphQL Upsert)
    try:
        meta_targets = []
        
        # Original Retail Price (Strictly formatted to 2 decimal places for Shopify validation)
        if cfg.get('meta_original_price'):
            safe_price = "{:.2f}".format(float(p.get('list_price') or 0.0))
            meta_targets.append({
                'ownerId': f"gid://shopify/Product/{sp.id}",
                'namespace': 'custom',
                'key': 'original_retail_price', 
                'value': str(safe_price), 
                'type': 'number_decimal'
            })
        
        # Vendor Product Code
        if cfg.get('meta_vendor_code') and p.get('vendor_code'):
            meta_targets.append({
                'ownerId': f"gid://shopify/Product/{sp.id}",
                'namespace': 'custom',
                'key': 'vendor_product_code', 
                'value': str(p.get('vendor_code')), 
                'type': 'single_line_text_field'
            })

        if meta_targets:
            client = shopify.GraphQL()
            query = """
            mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
              metafieldsSet(metafields: $metafields) {
                metafields { key value }
                userErrors { field message }
              }
            }
            """
            # Execute the GraphQL Upsert
            result = json.loads(client.execute(query, {'metafields': meta_targets}))
            
            # Catch and log any specific Shopify validation errors
            errors = result.get('data', {}).get('metafieldsSet', {}).get('userErrors', [])
            if errors:
                print(f"GraphQL Metafield Validation Error for {sku}: {errors}")
                    
    except Exception as e:
        print(f"Metafield Save Error {sku}: {e}")

    # --- Variant Sync ---
    existing = getattr(sp, 'variants', [])
    final_vars = []
    
    for des in desired_variants:
        match = next((v for v in existing if v.sku == des['sku']), None)
        
        if not match and des['option1'] == 'Default Title' and len(existing) == 1:
             match = existing[0]
        
        if not match: 
            match = shopify.Variant({'product_id': sp.id})
        
        match.option1 = des['option1']
        match.sku = des['sku']
        
        if cfg['price']: 
            match.price = des['price']
            match.compare_at_price = des['price']
        
        if cfg['barcode'] and des['barcode']: 
            match.barcode = des['barcode']
            
        match.inventory_management = 'shopify'
        final_vars.append(match)
    
    sp.variants = final_vars
    try: 
        sp.save()
        if sp.errors:
            log_event('Product Sync', 'Error', f"Shopify rejected variants for '{sku}': {sp.errors.full_messages()}", shop_url=shop_url)
            return "error"
    except Exception as e:
        log_event('Product Sync', 'Error', f"Variant Save Code Error {sku}: {e}", shop_url=shop_url)
        return "error"

    # Final Map Update — save ALL variant IDs to prevent duplicate creates
    try:
        for variant in sp.variants:
            v_sku = getattr(variant, "sku", None)
            if not v_sku:
                continue
            pm_v = ProductMap.query.filter_by(sku=v_sku, shop_url=shop_url).first()
            if not pm_v:
                pm_v = ProductMap(sku=v_sku, odoo_product_id=p['id'], shop_url=shop_url)
                db.session.add(pm_v)
            pm_v.shopify_variant_id = str(variant.id)
            pm_v.last_synced_at = datetime.utcnow()
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print(f"Final mapping error for {sku}: {e}")
    # Image Sync
    if cfg['images'] and p.get('image_1920'):
        try:
            img_raw = p['image_1920']
            if isinstance(img_raw, bytes): img_raw = img_raw.decode('utf-8')
            new_hash = hashlib.md5(img_raw.encode('utf-8')).hexdigest()
            
            # Fetch the map we just updated/created above
            pm_upd = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
            current_hash = pm_upd.image_hash if pm_upd else ""
            
            if new_hash != current_hash or not sp.images:
                if sp.images:
                    for old_img in sp.images:
                        try: shopify.Image.find(old_img.id, product_id=sp.id).destroy()
                        except: pass
                img = shopify.Image(prefix_options={'product_id': sp.id})
                img.attachment = img_raw; img.save()
                
                if pm_upd:
                    pm_upd.image_hash = new_hash; db.session.commit()
        except: pass

    # ONLY ONE RETURN AT THE VERY END
    return action_log


# =====================================================
# 4. HELPERS & UTILITIES
# =====================================================
def safe_find_variant_by_sku(sku):
    try:
        variants = shopify.Variant.find(params={'sku': sku})
        if variants: return variants[0]
    except: pass
    return None

def find_shopify_product_by_sku(sku, shop_url):
    """
    Strictly finds a Shopify product ID by SKU using GraphQL.
    Returns the integer product ID, or None if not found.

    Uses exact SKU match — no fuzzy title search.
    The same GraphQL pattern is used in the race-condition safety
    check inside process_product_data, so we know it works.
    """
    clean_sku = str(sku).strip()
    if not clean_sku:
        return None

    # Escape any single quotes in the SKU to avoid breaking the query
    escaped_sku = clean_sku.replace("'", "\\'")

    try:
        client = shopify.GraphQL()
        result = json.loads(client.execute(
            '{ productVariants(first: 5, query: "sku:\'%s\'") { edges { node { sku product { legacyResourceId } } } } }'
            % escaped_sku
        ))

        edges = result.get('data', {}).get('productVariants', {}).get('edges', [])

        for edge in edges:
            node = edge.get('node', {})
            # Strict exact match — GraphQL query can return partial matches
            if node.get('sku', '').strip() == clean_sku:
                return int(node['product']['legacyResourceId'])

        return None

    except Exception as e:
        print(f"SKU search error for '{clean_sku}': {e}")
        return None

def archive_shopify_duplicates(shop_url):
    from app import app, db
    with app.app_context():
        if not setup_shopify_session(shop_url): return
        log_event('Duplicate Cleanup', 'Info', "Starting scan for duplicates. Prioritizing OLD products to preserve history...", shop_url=shop_url)

        page = shopify.Product.find(limit=250, status='active')
        sku_tracker = {}
        
        while page:
            for p in page:
                if not p.variants: continue
                sku = p.variants[0].sku
                if not sku: continue
                
                clean_sku = str(sku).strip()
                if clean_sku not in sku_tracker: sku_tracker[clean_sku] = []
                sku_tracker[clean_sku].append(p)
            
            if page.has_next_page(): page = page.next_page()
            else: break

        archived_count = 0
        relinked_count = 0
        
        for sku, products in sku_tracker.items():
            if len(products) > 1:
                # 1. Sort products by CREATION DATE ascending (Oldest first)
                # This ensures the product with SEO/Sales/App history is kept.
                products.sort(key=lambda x: x.created_at)
                master_product = products[0] # The historical, original product
                
                # 2. Archive all the newer duplicates
                for p in products[1:]:
                    try:
                        p.status = 'archived'
                        p.save()
                        archived_count += 1
                    except Exception as e:
                        print(f"Failed to archive duplicate {p.id}: {e}")

                # 3. REPAIR THE DATABASE MAP
                # We must tell our app to sync to this older, historical product from now on
                pm = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
                if pm and master_product.variants:
                    correct_variant_id = str(master_product.variants[0].id)
                    if pm.shopify_variant_id != correct_variant_id:
                        pm.shopify_variant_id = correct_variant_id
                        db.session.commit()
                        relinked_count += 1

        log_event('Duplicate Cleanup', 'Success', f"Complete. Archived {archived_count} new duplicates. Re-linked {relinked_count} historical products.", shop_url=shop_url)

# Keep these aliases if they are used elsewhere in your code
cleanup_duplicates_master = archive_shopify_duplicates
cleanup_shopify_products = archive_shopify_duplicates

def sync_images_only_manual(shop_url):
    from app import app
    with app.app_context():
        mapped_products = ProductMap.query.filter_by(shop_url=shop_url).all()
        for pm in mapped_products:
            q_default.enqueue(repair_single_product_image, shop_url, pm.sku, job_timeout=300)
        log_event('Force Image Sync', 'Success', f"Queued image repair for {len(mapped_products)} products.", shop_url=shop_url)

def repair_single_product_image(shop_url, sku):
    from app import app
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): return

        shopify_product_id = find_shopify_product_by_sku(sku, shop_url)
        if not shopify_product_id: return

        try:
            sp = shopify.Product.find(shopify_product_id)
            odoo_p = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'search_read', 
                [[['default_code', '=', sku], ['active', '=', True]]], {'fields': ['image_1920'], 'limit': 1}
            )

            if not odoo_p or not odoo_p[0].get('image_1920'): return

            if sp.images:
                for img in sp.images:
                    try: shopify.Image.find(img.id, product_id=sp.id).destroy()
                    except: pass

            new_image = shopify.Image(prefix_options={'product_id': sp.id})
            img_data = odoo_p[0]['image_1920']
            if isinstance(img_data, bytes): img_data = img_data.decode('utf-8')
            new_image.attachment = img_data
            new_image.save()

            new_hash = hashlib.md5(img_data.encode('utf-8')).hexdigest()
            pm = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
            if pm:
                pm.image_hash = new_hash
                db.session.commit()
                
        except: pass

# =====================================================
# 6. STRICT VARIANT REPAIR (The "Fix Mess" Tool)
# =====================================================
def fix_variant_mess_task(shop_url, company_id):
    from app import app
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): 
            log_event('Cleanup', 'Error', "Startup Failed: No Odoo/Shopify Connection", shop_url=shop_url)
            return
        
        log_event('Cleanup', 'Info', f"Starting Strict Variant Repair for Company {company_id}...", shop_url=shop_url)
        
        # 2. Fetch UOM Map (For Ratios)
        uom_map = {}
        try:
            uoms = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'uom.uom', 'search_read', [], {'fields': ['id', 'name', 'factor_inv']})
            for u in uoms:
                uom_map[u['id']] = {'name': u['name'], 'ratio': float(u.get('factor_inv', 1.0))}
        except Exception as e:
            print(f"UOM Fetch Warning: {e}")

        # 3. Fetch Odoo Data
        try:
            domain = [
                ['sale_ok', '=', True], 
                ['type', 'in', ['product', 'consu']], 
                ['active', '=', True],
                '|', 
                ['company_id', '=', int(company_id)], 
                ['company_id', '=', False]
            ]
            
            # --- THIS LINE IS NOW FIXED ---
            fields = ['default_code', 'name', 'list_price', 'standard_price', 
                      'sh_is_secondary_unit', 'sh_secondary_uom', 'qty_per_pack', 'qty_available', 'uom_id']
            
            odoo_products = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'search_read', [domain], {'fields': fields})
            
            odoo_map = {str(p.get('default_code')).strip(): p for p in odoo_products if p.get('default_code')}
            
        except Exception as e:
            log_event('Cleanup', 'Error', f"Odoo Data Fetch Failed: {e}", shop_url=shop_url)
            return
        # 4. Scan Shopify Products
        page = shopify.Product.find(limit=50, status='active')
        processed = 0
        repaired = 0
        
        while page:
            for sp in page:
                if not sp.variants: continue
                ref_sku = str(sp.variants[0].sku).split('-')[0].strip()
                
                if not ref_sku or ref_sku not in odoo_map:
                    continue 

                p = odoo_map[ref_sku]
                processed += 1
                

               # --- UPDATED: Strict Variant Logic ---
                is_pack = False
                main_ratio = float(p.get('qty_per_pack', 1.0))
                if main_ratio < 1.0: main_ratio = 1.0
                
                sec_ratio = 1.0
                sec_name = "Unit"
                
                # 1. Check if Secondary Unit is active
                if p.get('sh_is_secondary_unit'):
                    if p.get('sh_secondary_uom'):
                        sec_data = p['sh_secondary_uom']
                        
                        # A. Get Name
                        if len(sec_data) > 1 and sec_data[1]:
                            sec_name = str(sec_data[1])
                        elif sec_data[0] in uom_map:
                            sec_name = uom_map[sec_data[0]]['name']

                        # B. Get Ratio
                        if sec_data[0] in uom_map:
                            sec_ratio = uom_map[sec_data[0]]['ratio']
                    
                    # 2. Determine Pack Status
                    if main_ratio > 1.0 or sec_ratio != 1.0:
                        is_pack = True
                        
                        # 3. FORCE FORMATTING: "6 per pack"
                        if sec_ratio > 1.0:
                            sec_name = f"{int(sec_ratio)} per pack"
    

                # Prices
                pack_price = float(p.get('list_price', 0.0))
                pack_cost = float(p.get('standard_price', 0.0))
                raw_stock = int(p.get('qty_available', 0))
                
                # Main UOM Name
                main_uom_name = "Outer"
                if p.get('uom_id') and p['uom_id'][0] in uom_map:
                    raw_name = uom_map[p['uom_id'][0]]['name']
                    main_uom_name = str(raw_name) if raw_name else "Outer"
                    
                    if main_ratio > 1 and "per pack" not in main_uom_name.lower():
                        main_uom_name = f"{int(main_ratio)} per pack"

                # Define Desired Variants
                desired_variants = []
                
                if is_pack:
                    sp.options = [{'name': 'Pack Size'}]
                    
                    # 1. The Pack
                    pack_stock = math.floor(raw_stock / main_ratio) if main_ratio > 0 else 0
                    desired_variants.append({
                        'sku': ref_sku,
                        'option1': main_uom_name,
                        'price': str(pack_price),
                        'cost': str(pack_cost),
                        'stock': pack_stock
                    })
                    
                    # 2. The Secondary (CTNX6)
                    # Price = (PackPrice / 24) * 6
                    unit_price = round((pack_price / main_ratio) * sec_ratio, 2)
                    unit_cost = round((pack_cost / main_ratio) * sec_ratio, 2)
                    unit_stock = math.floor(raw_stock / sec_ratio) if sec_ratio > 0 else 0
                    
                    # SKU: OBBA24C-CTNX6
                    safe_suffix = sec_name.replace(' ', '')
                    suffix = f"-{safe_suffix}" if safe_suffix.upper() != "UNIT" else "-UNIT"
                    
                    desired_variants.append({
                        'sku': f"{ref_sku}{suffix}",
                        'option1': sec_name,
                        'price': str(unit_price),
                        'cost': str(unit_cost),
                        'stock': unit_stock
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
                    match = next((v for v in current_variants if v.sku == target['sku']), None)
                    
                    # Intelligent Matching
                    if not match and len(current_variants) == 1 and len(desired_variants) == 1:
                        match = current_variants[0]
                    
                    # If updating "Unit" -> "CTNX6", match by the SKU prefix if secondary
                    if not match and target['option1'] == sec_name and len(current_variants) > 1:
                         # Find the variant that is NOT the main pack (different price/sku)
                         match = next((v for v in current_variants if v.sku != ref_sku), None)

                    if not match:
                        match = shopify.Variant({'product_id': sp.id})
                        match.inventory_management = 'shopify'
                        dirty = True

                    # Safe Update
                    if getattr(match, 'option1', '') != target['option1']: 
                        match.option1 = target['option1']
                        dirty = True
                    if str(getattr(match, 'price', '')) != target['price']: 
                        match.price = target['price']
                        dirty = True
                    if getattr(match, 'sku', '') != target['sku']: 
                        match.sku = target['sku']
                        dirty = True
                    
                    final_list.append(match)

                if len(current_variants) > len(final_list):
                    dirty = True

                if dirty:
                    sp.variants = final_list
                    try:
                        if sp.save():
                            repaired += 1
                            # Stock Update
                            location_id = get_config('shopify_target_location_id', None, shop_url=shop_url)
                            if location_id:
                                for v in sp.variants:
                                    target = next((t for t in desired_variants if t['option1'] == v.option1), None)
                                    if target and v.inventory_item_id:
                                        try:
                                            ii = shopify.InventoryItem.find(v.inventory_item_id)
                                            ii.cost = target['cost']
                                            ii.save()
                                            
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


# =====================================================
# 7. FORCE HEAL MISSING/UNMAPPED PRODUCTS (ALL TIME)
# =====================================================
def sync_missing_new_products(shop_url):
    from app import app
    from models import ProductMap # Ensure we have access to the mapping table
    
    with app.app_context():
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop: return
        
        odoo = get_odoo_connection(shop_url)
        if not odoo: return

        company_id = shop.odoo_company_id
        if not company_id: return

        # 1. NO DATE LIMIT: Scan ALL active products
        domain = [
            ['sale_ok', '=', True], 
            ['type', 'in', ['product', 'consu']], 
            ['active', '=', True], 
            '|', 
            ['company_id', '=', int(company_id)],
            ['company_id', '=', False]
        ]
        
        try:
            odoo_products = odoo.models.execute_kw(
                odoo.db, odoo.uid, odoo.password, 
                'product.product', 'search_read', 
                [domain], {'fields': ['id']}
            )
        except Exception as e:
            log_event('Missing Sync', 'Error', f"Search Failed: {e}", shop_url=shop_url)
            return

        if not odoo_products:
            log_event('Missing Sync', 'Info', "No Odoo products found.", shop_url=shop_url)
            return

        # 2. CHECK DATABASE FOR BROKEN/MISSING MAPPINGS
        existing_maps = ProductMap.query.filter_by(shop_url=shop_url).all()
        mapped_odoo_ids = set()
        
        for pm in existing_maps:
            # If the shopify_variant_id exists and is not '0' or empty, it is fully mapped
            if pm.shopify_variant_id and pm.shopify_variant_id.strip() not in ['0', '']:
                mapped_odoo_ids.add(pm.odoo_product_id)

        # 3. FILTER: Only keep Odoo IDs that are NOT perfectly mapped
        missing_product_ids = [p['id'] for p in odoo_products if p['id'] not in mapped_odoo_ids]

        if not missing_product_ids:
            log_event('Missing Sync', 'Success', "All active Odoo products are already correctly mapped in the database. Nothing to fix.", shop_url=shop_url)
            return

        # 4. QUEUE THE FIXES
        BATCH_SIZE = 50
        chunks = [missing_product_ids[i:i + BATCH_SIZE] for i in range(0, len(missing_product_ids), BATCH_SIZE)]

        for index, batch_ids in enumerate(chunks):
            q_default.enqueue(sync_product_batch_task, shop_url, batch_ids, f"Force Map Catch-Up {index+1}/{len(chunks)}", job_timeout=3600)

        log_event('Missing Sync', 'Success', f"Force Queued {len(missing_product_ids)} unmapped Odoo products to fetch Shopify IDs and repair database.", shop_url=shop_url)


def refresh_metafields_for_shop(shop_url):
    """
    Force re-writes the two metafields (original_retail_price + vendor_product_code)
    on every product in ProductMap for this shop.

    Unlike the regular sync which skips unchanged values, this ALWAYS writes —
    which is exactly what you need when metafields are blank in Shopify but the
    setting is ticked. Called by the manual "Refresh Metafields" button.
    """
    from app import app
    from models import ProductMap

    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url):
            log_event('Metafield Refresh', 'Error', 'Connection failed (Odoo or Shopify)', shop_url=shop_url)
            return 0, "Connection failed"

        # Only run if at least one of the two metafields is enabled in settings
        sync_price  = get_config('prod_sync_meta_original_price', False, shop_url=shop_url)
        sync_vendor = get_config('prod_sync_meta_vendor_code',    False, shop_url=shop_url)

        if not sync_price and not sync_vendor:
            return 0, "Both metafields are disabled in settings — tick at least one to refresh"

        # 1. Load every mapped product for this shop (skip placeholder -1 rows)
        maps = ProductMap.query.filter(
            ProductMap.shop_url == shop_url,
            ProductMap.odoo_product_id != -1
        ).all()

        if not maps:
            return 0, "No mapped products found"

        odoo_ids    = [m.odoo_product_id for m in maps]
        shopify_map = {m.odoo_product_id: m.shopify_variant_id for m in maps}

        # 2. Bulk-fetch Odoo data (CHUNKED to prevent Render OOM crashes)
        odoo_products = []
        CHUNK_SIZE = 250
        try:
            for i in range(0, len(odoo_ids), CHUNK_SIZE):
                chunk = odoo_ids[i:i + CHUNK_SIZE]
                chunk_data = odoo.models.execute_kw(
                    odoo.db, odoo.uid, odoo.password,
                    'product.product', 'read', [chunk],
                    {'fields': ['id', 'list_price', 'product_tmpl_id']}
                )
                if chunk_data:
                    odoo_products.extend(chunk_data)
        except Exception as e:
            log_event('Metafield Refresh', 'Error', f"Odoo read failed: {e}", shop_url=shop_url)
            return 0, f"Odoo read failed: {e}"

        # 3. Bulk-fetch vendor codes from product.supplierinfo (CHUNKED)
        vendor_code_map = {}
        if sync_vendor:
            tmpl_ids = [p['product_tmpl_id'][0] for p in odoo_products if p.get('product_tmpl_id')]
            if tmpl_ids:
                try:
                    for i in range(0, len(tmpl_ids), CHUNK_SIZE):
                        chunk = tmpl_ids[i:i + CHUNK_SIZE]
                        supplier_info = odoo.models.execute_kw(
                            odoo.db, odoo.uid, odoo.password,
                            'product.supplierinfo', 'search_read',
                            [[['product_tmpl_id', 'in', chunk]]],
                            {'fields': ['product_tmpl_id', 'product_code']}
                        )
                        for info in supplier_info:
                            if info.get('product_code') and info.get('product_tmpl_id'):
                                vendor_code_map[info['product_tmpl_id'][0]] = info['product_code']
                except Exception as e:
                    log_event('Metafield Refresh', 'Warning', f"Vendor code fetch error: {e}", shop_url=shop_url)

        # 4. Build lookup: odoo_id -> {price, vendor_code}
        odoo_data = {}
        for p in odoo_products:
            tmpl_id = p['product_tmpl_id'][0] if p.get('product_tmpl_id') else None
            odoo_data[p['id']] = {
                'price': "{:.2f}".format(float(p.get('list_price') or 0.0)),
                'vendor_code': vendor_code_map.get(tmpl_id, '') if tmpl_id else ''
            }

        # 5. Walk through each product and force-write metafields
        updated = 0
        errors  = 0
        BATCH_SIZE = 50

        # Process in batches to avoid memory issues on large catalogs
        map_items = list(shopify_map.items())  # [(odoo_id, shopify_variant_id), ...]

        for i in range(0, len(map_items), BATCH_SIZE):
            batch = map_items[i:i + BATCH_SIZE]
            variant_ids = [str(v_id) for _, v_id in batch]

            # Bulk-fetch Shopify products by variant ID via GraphQL
            gql = """
            query($ids: [ID!]!) {
              nodes(ids: $ids) {
                ... on ProductVariant {
                  id
                  product {
                    id
                    metafields(first: 20, namespace: "custom") {
                      edges { node { id key value } }
                    }
                  }
                }
              }
            }
            """
            gql_ids = [f"gid://shopify/ProductVariant/{v}" for v in variant_ids]
            try:
                import shopify as _shopify
                client = _shopify.GraphQL()
                import json as _json
                result = _json.loads(client.execute(gql, {'ids': gql_ids}))
                nodes = result.get('data', {}).get('nodes', [])
            except Exception as e:
                log_event('Metafield Refresh', 'Warning', f"GraphQL batch error: {e}", shop_url=shop_url)
                errors += len(batch)
                continue

            # Build reverse map: variant GID -> odoo_id
            gid_to_odoo = {}
            for odoo_id, v_id in batch:
                gid_to_odoo[f"gid://shopify/ProductVariant/{v_id}"] = odoo_id

            for node in nodes:
                if not node:
                    continue
                variant_gid = node.get('id')
                odoo_id = gid_to_odoo.get(variant_gid)
                if not odoo_id or odoo_id not in odoo_data:
                    continue

                data = odoo_data[odoo_id]
                product_gid = node.get('product', {}).get('id')
                existing_metas = {
                    edge['node']['key']: edge['node']
                    for edge in node.get('product', {}).get('metafields', {}).get('edges', [])
                }

                # Build the metafield mutations
                mutations = []

                if sync_price:
                    mutations.append({
                        'key':   'original_retail_price',
                        'value': data['price'],
                        'type':  'number_decimal',
                        'existing': existing_metas.get('original_retail_price')
                    })

                if sync_vendor and data['vendor_code']:
                    mutations.append({
                        'key':   'vendor_product_code',
                        'value': data['vendor_code'],
                        'type':  'single_line_text_field',
                        'existing': existing_metas.get('vendor_product_code')
                    })

                for m in mutations:
                    existing = m.pop('existing')
                    try:
                        if existing:
                            # UPDATE — force write even if value looks the same (fixes blank issue)
                            update_gql = """
                            mutation($id: ID!, $metafield: MetafieldInput!) {
                              metafieldUpdate(metafield: {id: $id, value: $metafield.value}) {
                                metafield { id value }
                                userErrors { field message }
                              }
                            }
                            """
                            # Use the simpler metafieldsSet mutation which handles both create/update
                            set_gql = """
                            mutation($metafields: [MetafieldsSetInput!]!) {
                              metafieldsSet(metafields: $metafields) {
                                metafields { key value }
                                userErrors { field message }
                              }
                            }
                            """
                            set_vars = {'metafields': [{
                                'ownerId': product_gid,
                                'namespace': 'custom',
                                'key': m['key'],
                                'value': m['value'],
                                'type': m['type']
                            }]}
                            client.execute(set_gql, set_vars)
                        else:
                            # CREATE
                            create_gql = """
                            mutation($metafields: [MetafieldsSetInput!]!) {
                              metafieldsSet(metafields: $metafields) {
                                metafields { key value }
                                userErrors { field message }
                              }
                            }
                            """
                            create_vars = {'metafields': [{
                                'ownerId': product_gid,
                                'namespace': 'custom',
                                'key': m['key'],
                                'value': m['value'],
                                'type': m['type']
                            }]}
                            client.execute(create_gql, create_vars)
                        updated += 1
                    except Exception as e:
                        errors += 1
                        print(f"Metafield write error ({m['key']}): {e}")

        summary = f"Metafield refresh complete. Updated {updated} metafields across {len(maps)} products."
        if errors:
            summary += f" ({errors} errors — check logs)"
        log_event('Metafield Refresh', 'Success', summary, shop_url=shop_url)
        return updated, summary
