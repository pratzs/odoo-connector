import shopify
import json
import gc
import time
import hashlib
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

        # Fetch Odoo Products (Only IDs)
        domain = [['sale_ok', '=', True], ['type', 'in', ['product', 'consu']], ['company_id', '=', int(company_id)]]
        try:
            product_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'search', [domain])
        except Exception as e:
            log_event('Product Sync', 'Error', f"Search Failed: {e}", shop_url=shop_url)
            return

        # Batching (50 items per batch)
        BATCH_SIZE = 50
        chunks = [product_ids[i:i + BATCH_SIZE] for i in range(0, len(product_ids), BATCH_SIZE)]

        for index, batch_ids in enumerate(chunks):
            q_default.enqueue(sync_product_batch_task, shop_url, batch_ids, f"Batch {index+1}/{len(chunks)}", job_timeout=1200)

        log_event('Product Sync', 'Success', f"Queued {len(chunks)} batches for {len(product_ids)} products.", shop_url=shop_url)


# =====================================================
# 2. THE WORKER
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
            'meta_price': get_config('prod_sync_meta_original_price', False, shop_url=shop_url),
            'images': get_config('prod_sync_images', False, shop_url=shop_url),
            'vendor': get_config('prod_sync_vendor', True, shop_url=shop_url),
            'barcode': get_config('prod_sync_barcode', True, shop_url=shop_url),
            'auto_create': get_config('prod_auto_create', False, shop_url=shop_url),
            'auto_publish': get_config('prod_auto_publish', False, shop_url=shop_url),
            'meta_code': get_config('prod_sync_meta_vendor_code', False, shop_url=shop_url),
            'currency': shopify.Shop.current().currency
        }

        # Pre-fetch UOMs
        uom_map = {}
        try:
            uoms = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'uom.uom', 'search_read', [], {'fields': ['id', 'name', 'factor_inv']})
            for u in uoms:
                uom_map[u['id']] = {'name': u['name'], 'ratio': float(u.get('factor_inv', 1.0))}
        except: pass

        fields = ['default_code', 'name', 'list_price', 'standard_price', 'weight', 'active', 'uom_id', 'sh_is_secondary_unit', 'sh_secondary_uom', 'public_categ_ids', 'product_tag_ids', 'description_sale', 'product_tmpl_id', 'image_1920', 'barcode', 'qty_per_pack']
        
        products = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'read', [batch_ids], {'fields': fields})

        stats = {'created': 0, 'updated': 0, 'skipped': 0, 'errors': 0}
        
        for p in products:
            try:
                res = process_product_data(p, odoo, shop_url, cfg, uom_map)
                stats[res] = stats.get(res, 0) + 1
            except Exception as e:
                print(f"Error syncing {p.get('default_code')}: {e}")
                stats['errors'] += 1

        gc.collect()
        
        # Log Summary
        log_event('Product Sync', 'Info', 
                  f"✅ {batch_name}: New: {stats['created']}, Updated: {stats['updated']}, No Change: {stats['skipped']}", 
                  shop_url=shop_url)


# =====================================================
# 3. SINGLE PRODUCT LOGIC (WITH DEBUGGING)
# =====================================================
def process_product_data(p, odoo, shop_url, cfg, uom_map):
    sku = str(p.get('default_code') or '').strip()
    if not sku: return "skipped" 

    product_name = p.get('name', 'Unknown')
    # Vendor Heuristic: "Blue Shirt" -> "Blue"
    vendor_name = product_name.split(' ')[0] if product_name else "Worthy"

    # --- 1. PREPARE ODOO DATA ---
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

    raw_price = float(p.get('list_price', 0.0))
    raw_cost = float(p.get('standard_price', 0.0))
    barcode = p.get('barcode', '') or ''

    desired_variants = []
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

    # --- 2. FIND SHOPIFY PRODUCT ---
    sid = find_shopify_product_by_sku(sku, shop_url)
    sp = None
    if sid:
        try: sp = shopify.Product.find(sid)
        except: sp = None 
    
    action = "skipped"
    dirty_reasons = [] # DEBUGGING TOOL

    # --- 3. CREATE ---
    if not sp:
        if not cfg['auto_create']: return "skipped"
        sp = shopify.Product()
        sp.title = p['name']
        sp.vendor = vendor_name
        sp.status = 'active' if cfg['auto_publish'] else 'draft'
        sp.published_scope = 'global'
        sp.body_html = p.get('description_sale') or ''
        cat_name = odoo.get_public_category_name(p.get('public_categ_ids', []))
        if cat_name: sp.product_type = cat_name
        sp.save()
        action = "created"
        # We must reload to get variant IDs
    
    # --- 4. DIRTY CHECKING ---
    dirty = False

    # A. Title
    if cfg['title'] and sp.title != p['name']:
        dirty_reasons.append(f"Title: {sp.title} -> {p['name']}")
        sp.title = p['name']
        dirty = True
    
    # B. Vendor
    if cfg['vendor'] and sp.vendor != vendor_name:
        dirty_reasons.append(f"Vendor: {sp.vendor} -> {vendor_name}")
        sp.vendor = vendor_name
        dirty = True

    # C. Description
    if cfg['desc']:
        clean_desc = (p.get('description_sale') or '').strip()
        current_body = (sp.body_html or '').strip()
        # Simple string compare
        if current_body != clean_desc:
            dirty_reasons.append("Description changed")
            sp.body_html = clean_desc
            dirty = True

    # D. Tags (NORMALIZED)
    if cfg['tags']:
        t_names = odoo.get_tag_names(p.get('product_tag_ids', []))
        # Shopify adds space after comma (e.g. "A, B"). Odoo might just have "A,B".
        # We sort and strip to compare accurately.
        current_tags_set = set([t.strip() for t in (sp.tags or '').split(',') if t.strip()])
        new_tags_set = set([t.strip() for t in t_names if t.strip()])
        
        if current_tags_set != new_tags_set:
            new_tags_str = ", ".join(sorted(new_tags_set))
            dirty_reasons.append(f"Tags: {sp.tags} -> {new_tags_str}")
            sp.tags = new_tags_str
            dirty = True

    # E. Status
    if sp.status == 'archived':
        sp.status = 'active'
        dirty_reasons.append("Un-archived product")
        dirty = True

    # F. Options
    if is_pack:
        if not sp.options or sp.options[0].name != 'Pack Size':
            sp.options = [{'name': 'Pack Size'}]
            dirty = True
    elif len(desired_variants) == 1 and desired_variants[0]['option1'] == 'Default Title':
        if sp.options and sp.options[0].name != 'Title':
             sp.options = [{'name': 'Title', 'values': ['Default Title']}]
             dirty = True

    if dirty or action == "created":
        sp.save()
        if action != "created": action = "updated"

    # --- 5. VARIANT SYNC ---
    existing_vars = getattr(sp, 'variants', [])
    final_vars = []
    variants_dirty = False
    
    for des in desired_variants:
        match = next((v for v in existing_vars if v.sku == des['sku']), None)
        if not match and des['option1'] == 'Default Title' and len(existing_vars) > 0:
             match = existing_vars[0]
        
        if not match:
            match = shopify.Variant({'product_id': sp.id})
            match.sku = des['sku']
            variants_dirty = True
            dirty_reasons.append(f"New Variant {des['sku']}")
        
        # Check Fields
        if match.option1 != des['option1']:
            match.option1 = des['option1']
            variants_dirty = True
            
        if cfg['price']:
            # Float comparison to avoid "10.0" != "10.00"
            if abs(float(match.price or 0) - float(des['price'])) > 0.01:
                dirty_reasons.append(f"Price {des['sku']}: {match.price} -> {des['price']}")
                match.price = des['price']
                match.compare_at_price = des['price']
                variants_dirty = True
        
        if cfg['barcode'] and des['barcode']:
            if (match.barcode or '') != des['barcode']:
                match.barcode = des['barcode']
                variants_dirty = True
                
        match.inventory_management = 'shopify'
        final_vars.append(match)

    if variants_dirty or len(existing_vars) != len(final_vars):
        sp.variants = final_vars
        sp.save()
        action = "updated"

    # --- 6. COST SYNC ---
    if sp.variants and cfg['cost']:
        for v in sp.variants:
            d = next((x for x in desired_variants if x['sku'] == v.sku), None)
            if d and v.inventory_item_id:
                try:
                    ii = shopify.InventoryItem.find(v.inventory_item_id)
                    if abs(float(ii.cost or 0) - float(d['cost'])) > 0.01:
                        ii.cost = d['cost']
                        ii.tracked = True
                        ii.save()
                except: pass

    # --- 7. IMAGE SYNC (HASH CHECK) ---
    if cfg['images'] and p.get('image_1920'):
        try:
            img_raw = p['image_1920']
            if isinstance(img_raw, bytes): img_raw = img_raw.decode('utf-8')
            new_hash = hashlib.md5(img_raw.encode('utf-8')).hexdigest()
            
            pm = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
            if not pm: 
                vid = sp.variants[0].id if sp.variants else '0'
                pm = ProductMap(sku=sku, odoo_product_id=p['id'], shopify_variant_id=str(vid), shop_url=shop_url)
                db.session.add(pm)

            if pm.image_hash != new_hash:
                # If hash changed, we upload. 
                # NOTE: First run will always trigger this if DB hash is None.
                img = shopify.Image(prefix_options={'product_id': sp.id})
                img.attachment = img_raw
                img.save()
                
                pm.image_hash = new_hash
                db.session.commit()
                if action == "skipped": action = "updated"
                dirty_reasons.append("Image Updated")
        except Exception as e:
            print(f"Image Sync Error: {e}")

    # --- 8. LOGGING ---
    # Update Map Timestamp
    try:
        pm = ProductMap.query.filter_by(sku=sku, shop_url=shop_url).first()
        if pm:
            pm.last_synced_at = datetime.utcnow()
            db.session.commit()
    except: db.session.rollback()

    # PRINT DEBUG INFO IF UPDATED
    if action == "updated" and dirty_reasons:
        print(f"🔄 Updated {sku}: {', '.join(dirty_reasons)}")

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
                time.sleep(1)
                continue
            raise e
    return None

def archive_shopify_duplicates(shop_url):
    pass # Kept for structure

# COMPATIBILITY ALIASES
cleanup_duplicates_master = archive_shopify_duplicates
cleanup_shopify_products = archive_shopify_duplicates
