import shopify
import json
import gc
from datetime import datetime
from models import Shop, ProductMap, AppSetting
from utils import get_odoo_connection, log_event, setup_shopify_session, get_config, q_default

# =====================================================
# 1. THE DISPATCHER (Fast - Queues Batches)
# =====================================================
def sync_products_master(shop_url):
    """
    Fetches all Odoo IDs and splits them into small batch jobs.
    """
    from app import app
    
    with app.app_context():
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop: return
        
        odoo = get_odoo_connection(shop_url)
        if not odoo: 
            log_event('Product Sync', 'Error', "Connection failed.", shop_url=shop_url)
            return

        company_id = shop.odoo_company_id
        if not company_id: return

        log_event('Product Sync', 'Info', "Fetching Odoo IDs...", shop_url=shop_url)
        
        domain = [
            ['sale_ok', '=', True],
            ['type', 'in', ['product', 'consu']],
            ['company_id', '=', int(company_id)]
        ]
        
        try:
            # Fetch IDs only
            product_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'search', [domain])
        except Exception as e:
            log_event('Product Sync', 'Error', f"Search Failed: {e}", shop_url=shop_url)
            return

        total_count = len(product_ids)
        log_event('Product Sync', 'Info', f"Found {total_count} items. Dispatching to queue...", shop_url=shop_url)

        # Split into batches of 50
        BATCH_SIZE = 50
        chunks = [product_ids[i:i + BATCH_SIZE] for i in range(0, len(product_ids), BATCH_SIZE)]

        for index, batch_ids in enumerate(chunks):
            q_default.enqueue(
                sync_product_batch_task, 
                shop_url, 
                batch_ids, 
                f"Batch {index+1}/{len(chunks)}",
                job_timeout=900
            )

        log_event('Product Sync', 'Success', f"Queued {len(chunks)} batches.", shop_url=shop_url)


# =====================================================
# 2. THE WORKER (Runs Batch - 50 items)
# =====================================================
def sync_product_batch_task(shop_url, batch_ids, batch_name):
    from app import app, db
    
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): return
        
        # Load configs once for this batch
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

        # Pre-load UOM cache
        uom_map = {}
        try:
            uoms = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'uom.uom', 'search_read', [], {'fields': ['id', 'name', 'factor_inv']})
            for u in uoms:
                uom_map[u['id']] = {'name': u['name'], 'ratio': float(u.get('factor_inv', 1.0))}
        except: pass

        fields = [
            'default_code', 'name', 'list_price', 'standard_price', 'weight', 
            'active', 'uom_id', 'sh_is_secondary_unit', 'sh_secondary_uom', 
            'public_categ_ids', 'product_tag_ids', 'description_sale', 
            'product_tmpl_id', 'image_1920', 'barcode', 'qty_per_pack'
        ]
        
        try:
            products = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'read', [batch_ids], {'fields': fields})
        except Exception as e:
            log_event('Product Sync', 'Error', f"{batch_name} Read Failed: {e}", shop_url=shop_url)
            return

        processed = 0
        for p in products:
            try:
                # Call the single-item logic
                process_product_data(p, odoo, shop_url, cfg, uom_map)
                processed += 1
            except Exception as e:
                # Log detailed error for debugging
                print(f"Error syncing {p.get('default_code')}: {e}")

        gc.collect()
        log_event('Product Sync', 'Info', f"✅ {batch_name} Complete. Processed {processed} items.", shop_url=shop_url)


# =====================================================
# 3. SINGLE PRODUCT LOGIC (Restored & Updated)
# =====================================================
def process_product_data(p, odoo, shop_url, cfg=None, uom_map=None):
    """
    Handles the logic for syncing ONE product from Odoo to Shopify.
    """
    from app import db
    
    # 1. Config Fallback (if called individually)
    if not cfg:
        cfg = {
            'title': True, 'price': True, 'cost': True, 'desc': True, 
            'tags': False, 'meta_price': False, 'images': False,
            'vendor': True, 'barcode': True, 'auto_create': False,
            'auto_publish': False, 'meta_code': False,
            'currency': 'USD' 
        }
    if not uom_map: uom_map = {}

    sku = p.get('default_code')
    if not sku: return

    product_name = p.get('name', 'Unknown')
    vendor_name = product_name.split(' ')[0] if product_name else "Worthy"

    # --- PACK LOGIC ---
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

    # --- PREPARE VARIANTS ---
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

    # --- SHOPIFY API ---
    # ✅ FIX: Use the updated helper function
    sid = find_shopify_product_by_sku(sku, shop_url)
    sp = None
    
    if sid:
        try: sp = shopify.Product.find(sid)
        except: sp = None
    
    if not sp:
        if not cfg['auto_create']: return
        sp = shopify.Product()
        sp.title = p['name']
        sp.vendor = vendor_name
        sp.status = 'active' if cfg['auto_publish'] else 'draft'
        
        # Categorization
        cat_name = odoo.get_public_category_name(p.get('public_categ_ids', []))
        if cat_name: sp.product_type = cat_name

    # Update Fields
    if cfg['title']: sp.title = p['name']
    if cfg['vendor']: sp.vendor = vendor_name
    if cfg['desc']: sp.body_html = p.get('description_sale') or ''
    
    if cfg['tags']:
        t_names = odoo.get_tag_names(p.get('product_tag_ids', []))
        if t_names: sp.tags = ",".join(t_names)

    # Options
    if is_pack:
        sp.options = [{'name': 'Pack Size'}]
    elif hasattr(sp, 'options') and sp.options and sp.options[0].name != 'Title':
        sp.options = [{'name': 'Title', 'values': ['Default Title']}]

    sp.save()

    # Variants
    existing = getattr(sp, 'variants', [])
    final_vars = []
    
    for des in desired_variants:
        match = next((v for v in existing if v.sku == des['sku']), None)
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

    # --- POST SAVE UPDATES ---
    
    # 1. Cost
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

    # 2. Metafields (Original Price)
    if cfg['meta_price']:
        try:
            val = json.dumps({"amount": str(raw_price), "currency_code": cfg['currency']})
            m = shopify.Metafield({
                'key': 'original_retail_price', 'value': val, 'type': 'money',
                'namespace': 'custom', 'owner_resource': 'product', 'owner_id': sp.id
            })
            m.save()
        except: pass

    # 3. Metafields (Vendor Code)
    if cfg['meta_code']:
        try:
            code = odoo.get_vendor_product_code(p['id'])
            if code:
                m = shopify.Metafield({
                    'key': 'vendor_product_code', 'value': code, 'type': 'single_line_text_field',
                    'namespace': 'custom', 'owner_resource': 'product', 'owner_id': sp.id
                })
                sp.add_metafield(m)
        except: pass

    # 4. Images
    if cfg['images'] and p.get('image_1920'):
        if not sp.images:
            try:
                img_raw = p['image_1920']
                if isinstance(img_raw, bytes): img_raw = img_raw.decode('utf-8')
                img = shopify.Image(prefix_options={'product_id': sp.id})
                img.attachment = img_raw
                img.save()
            except: pass

    # 5. Save Map
    try:
        pm = ProductMap.query.filter_by(sku=sku).first()
        if not pm:
            vid = sp.variants[0].id if sp.variants else '0'
            # ✅ FIX: Do NOT try to save shopify_product_id if the column doesn't exist
            pm = ProductMap(sku=sku, odoo_product_id=p['id'], shopify_variant_id=str(vid), shop_url=shop_url)
            db.session.add(pm)
        pm.last_synced_at = datetime.utcnow()
        db.session.commit()
    except: db.session.rollback()


# =====================================================
# 4. HELPERS
# =====================================================
def find_shopify_product_by_sku(sku, shop_url):
    """
    Finds a Product ID for a given SKU.
    Correctly handles DB schema limitation (no shopify_product_id column).
    """
    pm = ProductMap.query.filter_by(shop_url=shop_url, sku=sku).first()
    
    # ✅ FIX: Use Variant ID to fetch Product ID (since we don't store Product ID)
    if pm and pm.shopify_variant_id and pm.shopify_variant_id != '0':
        try:
            variant = shopify.Variant.find(pm.shopify_variant_id)
            return variant.product_id
        except:
            pass # Variant might be deleted or ID invalid

    # Fallback: API Search
    try:
        products = shopify.Product.find(limit=1, title=sku)
        if products: return products[0].id
    except: pass
    
    return None

def cleanup_shopify_products(shop_url):
    """
    Scans for duplicate SKUs on Shopify and archives the older versions.
    """
    from app import app
    
    with app.app_context():
        if not setup_shopify_session(shop_url): return
        
        log_event('Cleanup', 'Info', "Starting duplicate scan...", shop_url=shop_url)
        try:
            products = []
            page = shopify.Product.find(limit=250, status='active', fields="id,title,updated_at,variants")
            while page:
                products.extend(page)
                if page.has_next_page(): 
                    page = page.next_page()
                else: 
                    break
            
            sku_map = {}
            for p in products:
                if p.variants:
                    sku = p.variants[0].sku
                    if sku:
                        if sku not in sku_map: sku_map[sku] = []
                        sku_map[sku].append(p)
            
            cleaned = 0
            for sku, items in sku_map.items():
                if len(items) > 1:
                    items.sort(key=lambda x: x.updated_at, reverse=True)
                    trash = items[1:]
                    for t in trash:
                        try:
                            t.status = 'archived'
                            t.save()
                            cleaned += 1
                        except: pass
                        
            log_event('Cleanup', 'Success', f"Duplicate scan finished. Archived {cleaned} items.", shop_url=shop_url)
        except Exception as e:
            log_event('Cleanup', 'Error', f"Cleanup failed: {e}", shop_url=shop_url)

# Alias for compatibility
archive_shopify_duplicates = cleanup_shopify_products
