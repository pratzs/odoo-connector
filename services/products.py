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

        # Only sync active products
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

        # --- B. PREFETCH SHOPIFY DATA ---
        batch_skus = [str(p.get('default_code')).strip() for p in products if p.get('default_code')]
        
        # 1. Bulk DB Lookup
        db_map_dict = {} 
        if batch_skus:
            maps = ProductMap.query.filter(ProductMap.shop_url == shop_url, ProductMap.sku.in_(batch_skus)).all()
            db_map_dict = {m.sku: m for m in maps}

        # 2. Total Store SKU Map (Bypasses Broken Shopify Search)
        shopify_product_cache = {} 
        try:
            page = shopify.Product.find(limit=250, status='active')
            while page:
                for sp in page:
                    for v in sp.variants:
                        if v.sku:
                            shopify_product_cache[str(v.sku).strip()] = sp
                if page.has_next_page():
                    page = page.next_page()
                    time.sleep(0.5) 
                else:
                    break
            print(f"✅ Local Cache Built: Loaded {len(shopify_product_cache)} SKUs from Shopify.")
        except Exception as e:
            print(f"❌ Failed to build Shopify cache: {e}")
            

        # --- C. PROCESS LOOP ---
        stats = {'created': 0, 'updated': 0, 'archived': 0, 'skipped': 0}

        for p in products:
            try:
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
            
            # 3. FORCE FORMATTING: "CTNX6" -> "6 per pack"
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
                first_word = p['name'].split()[0]
                clean_word = "".join(ch for ch in first_word if ch.isalnum())
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
# 4. HELPERS & UTILITIES
# =====================================================
def safe_find_variant_by_sku(sku):
    try:
        variants = shopify.Variant.find(params={'sku': sku})
        if variants: return variants[0]
    except: pass
    return None

def find_shopify_product_by_sku(sku, shop_url):
    pm = ProductMap.query.filter_by(shop_url=shop_url, sku=sku).first()
    if pm and pm.shopify_variant_id and pm.shopify_variant_id != '0':
        try:
            variant = shopify.Variant.find(pm.shopify_variant_id)
            return variant.product_id
        except: pass
    return None

def archive_shopify_duplicates(shop_url):
    from app import app
    with app.app_context():
        if not setup_shopify_session(shop_url): return
        log_event('Duplicate Cleanup', 'Info', "Starting scan for duplicate SKUs...", shop_url=shop_url)

        page = shopify.Product.find(limit=250, status='active')
        sku_tracker = {}
        
        while page:
            for p in page:
                if not p.variants: continue
                sku = p.variants[0].sku
                if not sku: continue
                if sku not in sku_tracker: sku_tracker[sku] = []
                sku_tracker[sku].append(p)
            
            if page.has_next_page(): page = page.next_page()
            else: break

        archived_count = 0
        for sku, products in sku_tracker.items():
            if len(products) > 1:
                pm = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
                master_id = int(pm.shopify_variant_id) if pm else 0
                master_product = None
                
                if master_id:
                    for p in products:
                        if any(str(v.id) == str(master_id) for v in p.variants):
                            master_product = p
                            break
                
                if not master_product:
                    products.sort(key=lambda x: x.updated_at, reverse=True)
                    master_product = products[0]

                for p in products:
                    if p.id != master_product.id:
                        try:
                            p.status = 'archived'
                            p.save()
                            archived_count += 1
                        except: pass

        log_event('Duplicate Cleanup', 'Success', f"Scan complete. Archived {archived_count} duplicates.", shop_url=shop_url)

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
            domain = [['sale_ok', '=', True], ['type', 'in', ['product', 'consu']], 
                      ['company_id', '=', int(company_id)], ['active', '=', True]]
            
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
                        
                        # 3. FORCE FORMATTING: "CTNX6" -> "6 per pack"
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
                            location_id = get_config('shopify_location_id', None, shop_url=shop_url)
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
