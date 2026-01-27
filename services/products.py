import shopify
import json
import gc
import time
import hashlib
from difflib import SequenceMatcher
from datetime import datetime
from models import Shop, ProductMap, AppSetting, db
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

        # Fetch Odoo Products
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
            # Increased timeout to 3600s for safety
            q_default.enqueue(sync_product_batch_task, shop_url, batch_ids, f"Batch {index+1}/{len(chunks)}", job_timeout=3600)

        log_event('Product Sync', 'Success', f"Queued {len(chunks)} batches.", shop_url=shop_url)


# =====================================================
# 2. THE WORKER
# =====================================================
def sync_product_batch_task(shop_url, batch_ids, batch_name):
    from app import app
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): return
        
        # Load Configuration
        cfg = {
            'title': get_config('prod_sync_title', True, shop_url=shop_url),
            'price': get_config('prod_sync_price', True, shop_url=shop_url),
            'cost': get_config('prod_sync_cost', True, shop_url=shop_url),
            'desc': get_config('prod_sync_desc', True, shop_url=shop_url),
            'tags': get_config('prod_sync_tags', False, shop_url=shop_url),
            'meta_price': get_config('prod_sync_meta_original_price', False, shop_url=shop_url),
            'images': get_config('prod_sync_images', False, shop_url=shop_url),
            'vendor': get_config('prod_sync_vendor', True, shop_url=shop_url),
            'barcode': get_config('prod_sync_barcode', True, shop_url=shop_url),
            'auto_create': get_config('prod_auto_create', False, shop_url=shop_url),
            'auto_publish': get_config('prod_auto_publish', False, shop_url=shop_url),
            'meta_code': get_config('prod_sync_meta_vendor_code', False, shop_url=shop_url),
            'currency': shopify.Shop.current().currency
        }

        uom_map = {}
        try:
            uoms = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'uom.uom', 'search_read', [], {'fields': ['id', 'name', 'factor_inv']})
            for u in uoms:
                uom_map[u['id']] = {'name': u['name'], 'ratio': float(u.get('factor_inv', 1.0))}
        except: pass

        fields = ['default_code', 'name', 'list_price', 'standard_price', 'weight', 'active', 'uom_id', 'sh_is_secondary_unit', 'sh_secondary_uom', 'public_categ_ids', 'product_tag_ids', 'description_sale', 'product_tmpl_id', 'image_1920', 'barcode', 'qty_per_pack']
        
        products = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'read', [batch_ids], {'fields': fields})

        stats = {'created': 0, 'updated': 0, 'fixed': 0, 'skipped': 0}

        for p in products:
            try:
                res = process_product_data(p, odoo, shop_url, cfg, uom_map)
                stats[res] = stats.get(res, 0) + 1
            except Exception as e:
                print(f"Error syncing {p.get('default_code')}: {e}")

        gc.collect()
        log_event('Product Sync', 'Info', f"✅ {batch_name}: New: {stats['created']}, Updated: {stats['updated']}, Fixed Monsters: {stats.get('fixed', 0)}", shop_url=shop_url)


# =====================================================
# 3. SINGLE PRODUCT LOGIC (Merged: Features + Safety)
# =====================================================
def process_product_data(p, odoo, shop_url, cfg, uom_map):
    from app import db
    
    sku = str(p.get('default_code') or '').strip()
    if not sku: return "skipped"

    product_name = p.get('name', 'Unknown')
    vendor_name = product_name.split(' ')[0] if product_name else "Worthy"

    # --- Pack & UOM Logic ---
    is_pack = False
    ratio = 1.0
    main_uom_name = 'Outer'

    if p.get('sh_is_secondary_unit') is True:
        qty_pack = p.get('qty_per_pack', 0.0)
        if qty_pack and float(qty_pack) > 1.0:
            is_pack = True
            ratio = float(qty_pack)
            main_uom_name = f"{int(ratio)} per pack"
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

    # --- Prepare Variants ---
    desired_variants = []
    raw_price = float(p.get('list_price', 0.0))
    raw_cost = float(p.get('standard_price', 0.0))
    barcode = p.get('barcode', '')

    if not is_pack:
        desired_variants.append({
            'option1': 'Default Title', 'price': str(raw_price),
            'sku': sku, 'barcode': barcode, 'cost': str(raw_cost)
        })
    else:
        desired_variants.append({
            'option1': main_uom_name, 'price': str(raw_price),
            'sku': sku, 'barcode': barcode, 'cost': str(raw_cost)
        })
        unit_price = round(raw_price / ratio, 2) if ratio > 0 else 0.0
        unit_cost = round(raw_cost / ratio, 2) if ratio > 0 else 0.0
        desired_variants.append({
            'option1': 'Unit', 'price': str(unit_price),
            'sku': f"{sku}-UNIT", 'barcode': '', 'cost': str(unit_cost)
        })

    # --- FIND PRODUCT (With Retry) ---
    sid = find_shopify_product_by_sku(sku, shop_url)
    sp = None
    action = "updated"
    
    if sid:
        try: sp = shopify.Product.find(sid)
        except: sp = None
    
    # === 🛡️ MONSTER SLAYER (Safety Lock) ===
    # This prevents the "Black Hole" bug by checking name similarity.
    if sp:
        shopify_title = str(sp.title or "").lower()
        odoo_title = str(product_name).lower()
        
        # Calculate similarity (0.0 to 1.0). If < 40% match, it's a Monster.
        similarity = SequenceMatcher(None, shopify_title, odoo_title).ratio()
        
        if similarity < 0.4:
            print(f"👹 MONSTER FOUND: SKU {sku} inside '{sp.title}'. Killing variant...")
            try:
                # Destroy only the bad variant to free the SKU
                for v in sp.variants:
                    if v.sku == sku:
                        v.destroy()
                        break
            except: pass
            
            # Force create new product
            sp = None
            action = "fixed"
    # ========================================

    # --- CREATE ---
    if not sp:
        if not cfg['auto_create']: return "skipped"

        sp = shopify.Product()
        sp.title = p['name']
        sp.vendor = vendor_name
        sp.status = 'active' if cfg['auto_publish'] else 'draft'
        sp.published_scope = 'global'
        
        cat_name = odoo.get_public_category_name(p.get('public_categ_ids', []))
        if cat_name: sp.product_type = cat_name
        
        if action != "fixed": action = "created"

    # --- UPDATE ---
    sp.published_scope = 'global'
    # Ensure active if previously archived
    if sp.status == 'archived': sp.status = 'active'

    if not sp.product_type:
        cat_name = odoo.get_public_category_name(p.get('public_categ_ids', []))
        if cat_name: sp.product_type = cat_name

    if cfg['title']: sp.title = p['name']
    if cfg['vendor']: sp.vendor = vendor_name
    if cfg['desc']: sp.body_html = p.get('description_sale') or ''
    if cfg['tags']:
        t_names = odoo.get_tag_names(p.get('product_tag_ids', []))
        if t_names: sp.tags = ",".join(t_names)

    if is_pack:
        if not sp.options or sp.options[0].name != 'Pack Size':
            sp.options = [{'name': 'Pack Size'}]
    elif hasattr(sp, 'options') and sp.options and sp.options[0].name != 'Title':
        sp.options = [{'name': 'Title', 'values': ['Default Title']}]

    sp.save()

    # Variants Update
    existing = getattr(sp, 'variants', [])
    final_vars = []
    
    for des in desired_variants:
        match = next((v for v in existing if v.sku == des['sku']), None)
        # Handle case where "Default Title" variant exists but we are renaming it
        if not match and des['option1'] == 'Default Title':
             match = next((v for v in existing), None)
        
        if not match: match = shopify.Variant({'product_id': sp.id})
        
        match.option1 = des['option1']
        match.sku = des['sku']
        if cfg['price']: 
            match.price = des['price']
            match.compare_at_price = des['price']
        if cfg['barcode'] and des['barcode']: match.barcode = des['barcode']
        match.inventory_management = 'shopify'
        final_vars.append(match)

    sp.variants = final_vars
    sp.save()

    # Post-Save: Cost & Metafields
    if sp.variants and cfg['cost']:
        for v in sp.variants:
            d = next((x for x in desired_variants if x['sku'] == v.sku), None)
            if d and v.inventory_item_id:
                try:
                    ii = shopify.InventoryItem.find(v.inventory_item_id)
                    ii.cost = d['cost']
                    ii.tracked = True
                    ii.save()
                except: pass

    if cfg['meta_price']:
        try:
            val = json.dumps({"amount": str(raw_price), "currency_code": cfg['currency']})
            m = shopify.Metafield({'key': 'original_retail_price', 'value': val, 'type': 'money', 'namespace': 'custom', 'owner_resource': 'product', 'owner_id': sp.id})
            m.save()
        except: pass

    if cfg['meta_code']:
        try:
            code = odoo.get_vendor_product_code(p['id'])
            if code:
                m = shopify.Metafield({'key': 'vendor_product_code', 'value': code, 'type': 'single_line_text_field', 'namespace': 'custom', 'owner_resource': 'product', 'owner_id': sp.id})
                sp.add_metafield(m)
        except: pass

    if cfg['images'] and p.get('image_1920'):
        # Safety: Only upload if no images exist to prevent dupes/slowdown
        if not sp.images:
            try:
                img_raw = p['image_1920']
                if isinstance(img_raw, bytes): img_raw = img_raw.decode('utf-8')
                img = shopify.Image(prefix_options={'product_id': sp.id})
                img.attachment = img_raw
                img.save()
            except: pass

    # Map Update
    try:
        pm = ProductMap.query.filter_by(sku=sku).first()
        if not pm:
            vid = sp.variants[0].id if sp.variants else '0'
            pm = ProductMap(sku=sku, odoo_product_id=p['id'], shopify_variant_id=str(vid), shop_url=shop_url)
            db.session.add(pm)
        
        # Ensure ID is correct
        if sp.variants:
            pm.shopify_variant_id = str(sp.variants[0].id)
            
        pm.last_synced_at = datetime.utcnow()
        db.session.commit()
    except: db.session.rollback()

    return action


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

    retries = 3
    for attempt in range(retries):
        try:
            variants = shopify.Variant.find(limit=1, params={'sku': sku})
            if variants: return variants[0].product_id
            return None
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            raise e
    return None

# =====================================================
# 5. MAINTENANCE TOOLS (RESTORED DEEP SCAN)
# =====================================================
def archive_shopify_duplicates(shop_url):
    """
    DEEP SCAN: Checks ALL variants of ALL products.
    Archives older products if their SKU conflicts with a newer one.
    """
    from app import app
    with app.app_context():
        if not setup_shopify_session(shop_url): return

        log_event('Cleanup', 'Info', "Starting Deep Duplicate Scan (All Variants)...", shop_url=shop_url)
        
        sku_map = {}
        # Iterate all products
        page = shopify.Product.find(limit=250)
        count = 0

        while page:
            for p in page:
                for v in p.variants:
                    raw_sku = getattr(v, 'sku', '')
                    if not raw_sku: continue
                    sku = str(raw_sku).strip()
                    if not sku: continue

                    if sku not in sku_map: sku_map[sku] = []
                    
                    if not any(existing.id == p.id for existing in sku_map[sku]):
                        sku_map[sku].append(p)
                count += 1
            
            if page.has_next_page():
                page = page.next_page()
            else:
                break

        duplicates_found = 0
        archived_count = 0

        for sku, product_list in sku_map.items():
            if len(product_list) > 1:
                duplicates_found += 1
                # Sort: Active first, then by Created At (Newest first)
                product_list.sort(key=lambda x: (x.status == 'active', x.created_at), reverse=True)
                
                winner = product_list[0]
                losers = product_list[1:]

                for loser in losers:
                    try:
                        if loser.status != 'archived':
                            loser.status = 'archived'
                            loser.save()
                            archived_count += 1
                            print(f"Archived duplicate {sku} (Product ID: {loser.id})")
                    except: pass

        msg = f"Deep Clean Complete. Found {duplicates_found} SKU conflicts. Archived {archived_count} products."
        log_event('Cleanup', 'Success', msg, shop_url=shop_url)

# Aliases
cleanup_duplicates_master = archive_shopify_duplicates
cleanup_shopify_products = archive_shopify_duplicates
