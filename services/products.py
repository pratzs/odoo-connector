import shopify
import json
import gc
from datetime import datetime
from models import Shop, ProductMap, AppSetting
from utils import get_odoo_connection, log_event, setup_shopify_session, get_config, q_default

# REMOVED: from app import app, db  <-- This was causing the crash

# =====================================================
# 1. THE DISPATCHER (Runs fast, queues the work)
# =====================================================
def sync_products_master(shop_url):
    """
    Step 1: Get all IDs from Odoo.
    Step 2: Split them into chunks of 50.
    Step 3: Queue a separate job for each chunk.
    """
    # ✅ FIX: Import inside the function to avoid circular dependency
    from app import app, db 
    
    with app.app_context():
        # 1. Basic Setup
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop: return
        
        odoo = get_odoo_connection(shop_url)
        if not odoo: 
            log_event('Product Sync', 'Error', "Could not connect to Odoo.", shop_url=shop_url)
            return

        company_id = shop.odoo_company_id
        if not company_id: return

        # 2. Fetch IDs ONLY (Lightweight)
        log_event('Product Sync', 'Info', "Fetching Odoo product IDs...", shop_url=shop_url)
        
        domain = [
            ['sale_ok', '=', True],
            ['type', 'in', ['product', 'consu']],
            ['company_id', '=', int(company_id)]
        ]
        
        try:
            # Search returns a list of IDs e.g. [1, 2, 3...]
            product_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'search', [domain])
        except Exception as e:
            log_event('Product Sync', 'Error', f"Odoo Search Failed: {e}", shop_url=shop_url)
            return

        total_count = len(product_ids)
        log_event('Product Sync', 'Info', f"Found {total_count} items. Dispatching to queue...", shop_url=shop_url)

        # 3. Chunk and Queue
        BATCH_SIZE = 50
        chunks = [product_ids[i:i + BATCH_SIZE] for i in range(0, len(product_ids), BATCH_SIZE)]

        for index, batch_ids in enumerate(chunks):
            q_default.enqueue(
                sync_product_batch_task, 
                shop_url, 
                batch_ids, 
                f"Batch {index+1}/{len(chunks)}",
                job_timeout=900 # 15 mins per batch
            )

        log_event('Product Sync', 'Success', f"Queued {len(chunks)} batches. Check logs for progress.", shop_url=shop_url)


# =====================================================
# 2. THE WORKER (Processes 50 items at a time)
# =====================================================
def sync_product_batch_task(shop_url, batch_ids, batch_name):
    """
    Processes a specific list of 50 IDs.
    """
    # ✅ FIX: Import inside the function here too
    from app import app, db
    
    with app.app_context():
        # --- A. SETUP ---
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): return
        
        shop_info = shopify.Shop.current()
        currency_code = shop_info.currency
        
        # --- B. PRE-LOAD CACHES (UOMs) ---
        uom_map = {}
        try:
            uoms = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'uom.uom', 'search_read', 
                [['|', ['active', '=', True], ['active', '=', False]]], 
                {'fields': ['id', 'name', 'factor_inv']}
            )
            for u in uoms:
                safe_name = u['name'] if u['name'] else "Outer"
                uom_map[u['id']] = {'name': safe_name, 'ratio': float(u.get('factor_inv', 1.0))}
        except: pass

        # --- C. LOAD CONFIGS ---
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
            'meta_code': get_config('prod_sync_meta_vendor_code', False, shop_url=shop_url)
        }

        # --- D. FETCH DATA ---
        fields = [
            'default_code', 'name', 'list_price', 'standard_price', 'weight', 
            'active', 'uom_id', 'sh_is_secondary_unit', 'sh_secondary_uom', 
            'public_categ_ids', 'product_tag_ids', 'description_sale', 
            'product_tmpl_id', 'image_1920', 'barcode', 'qty_per_pack'
        ]
        
        try:
            odoo_products = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'read', [batch_ids], {'fields': fields})
        except Exception as e:
            log_event('Product Sync', 'Error', f"{batch_name} Read Failed: {e}", shop_url=shop_url)
            return

        synced = 0

        # --- E. PROCESSING LOOP ---
        for p in odoo_products:
            sku = p.get('default_code')
            if not sku or not p.get('active', True): continue

            product_name = p.get('name', 'Unknown')
            vendor_name = product_name.split(' ')[0] if product_name else "Worthy"
            
            # === STRICT PACK LOGIC ===
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
                    sec_uom_id = p['sh_secondary_uom'][0]
                    if sec_uom_id in uom_map:
                        ratio = uom_map[sec_uom_id]['ratio']
                        if ratio > 1.0: is_pack = True
                
                if p.get('uom_id'):
                    if len(p['uom_id']) > 1 and p['uom_id'][1]:
                        if not main_uom_name or "per pack" not in main_uom_name:
                            main_uom_name = p['uom_id'][1]
                    elif p['uom_id'][0] in uom_map: 
                         if not main_uom_name or "per pack" not in main_uom_name:
                            main_uom_name = uom_map[p['uom_id'][0]]['name']

            if main_uom_name == "Unknown (UOM)" or not main_uom_name:
                main_uom_name = "Outer"

            desired_variants = []
            raw_price = float(p.get('list_price', 0.0))
            raw_cost = float(p.get('standard_price', 0.0)) 
            barcode = p.get('barcode', '') 
            
            if not is_pack:
                desired_variants.append({
                    'option1': 'Default Title',
                    'price': str(raw_price),
                    'sku': sku,
                    'barcode': barcode,
                    'cost': str(raw_cost)
                })
            else:
                desired_variants.append({
                    'option1': main_uom_name, 
                    'price': str(raw_price),
                    'sku': sku,
                    'barcode': barcode,
                    'cost': str(raw_cost)
                })
                unit_price = round(raw_price / ratio, 2) if ratio > 0 else 0.00
                unit_cost = round(raw_cost / ratio, 2) if ratio > 0 else 0.00
                desired_variants.append({
                    'option1': 'Unit', 
                    'price': str(unit_price),
                    'sku': f"{sku}-UNIT",
                    'barcode': '', 
                    'cost': str(unit_cost)
                })

            # --- SHOPIFY ACTIONS ---
            # Helper to find ID safely
            def find_sid(target_sku):
                pm = ProductMap.query.filter_by(sku=target_sku, shop_url=shop_url).first()
                return pm.shopify_product_id if pm else None

            shopify_id = find_sid(sku)
            sp = None
            
            if shopify_id:
                try: sp = shopify.Product.find(shopify_id)
                except: sp = None
            
            # Search Fallback
            if not sp:
                try:
                    products = shopify.Product.find(limit=1, title=sku)
                    if products: sp = products[0]
                except: pass

            if not sp:
                if not cfg['auto_create']: continue
                sp = shopify.Product()
                sp.title = p['name']
                sp.vendor = vendor_name 
                sp.product_type = odoo.get_public_category_name(p.get('public_categ_ids', [])) or '' 
                sp.status = 'active' if cfg['auto_publish'] else 'draft'
            
            if cfg['title']: sp.title = p['name']
            if cfg['vendor']: sp.vendor = vendor_name 
            if cfg['desc']: sp.body_html = p.get('description_sale') or ''
            
            if cfg['tags']:
                odoo_tags = odoo.get_tag_names(p.get('product_tag_ids', []))
                if odoo_tags: sp.tags = ",".join(odoo_tags)

            if is_pack:
                sp.options = [{'name': 'Pack Size'}] 
            elif hasattr(sp, 'options') and sp.options and sp.options[0].name != 'Title':
                sp.options = [{'name': 'Title', 'values': ['Default Title']}]

            try:
                sp.save()
            except Exception as e:
                print(f"Error saving product {sku}: {e}")
                continue

            existing_vars = getattr(sp, 'variants', [])
            final_vars = []
            
            for des in desired_variants:
                match = next((v for v in existing_vars if getattr(v, 'sku', None) == des['sku']), None)
                if not match and des['option1'] == 'Default Title':
                      match = next((v for v in existing_vars), None)
                if not match: match = shopify.Variant({'product_id': sp.id})
                
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
            sp.save()
            
            # --- METAFIELDS ---
            if cfg['meta_price']:
                try:
                    price_val = str(p.get('list_price', 0.0))
                    val_json = json.dumps({"amount": price_val, "currency_code": currency_code})
                    meta = shopify.Metafield({
                        'key': 'original_retail_price',
                        'value': val_json,
                        'type': 'money', 
                        'namespace': 'custom',
                        'owner_resource': 'product',
                        'owner_id': sp.id
                    })
                    meta.save()
                except: pass
            
            # --- COST UPDATE ---
            if sp.variants:
                for v in sp.variants:
                    d_data = next((d for d in desired_variants if d['sku'] == v.sku), None)
                    if d_data and v.inventory_item_id and cfg['cost']:
                        try:
                            inv_item = shopify.InventoryItem.find(v.inventory_item_id)
                            if inv_item:
                                inv_item.cost = d_data['cost'] 
                                inv_item.tracked = True
                                inv_item.save()
                        except: pass

            if cfg['meta_code']:
                try:
                    v_code = odoo.get_vendor_product_code(p['id'])
                    if v_code:
                        meta = shopify.Metafield({
                            'key': 'vendor_product_code',
                            'value': v_code,
                            'type': 'single_line_text_field',
                            'namespace': 'custom',
                            'owner_resource': 'product',
                            'owner_id': sp.id
                        })
                        sp.add_metafield(meta)
                except: pass

            # --- IMAGES ---
            if cfg['images'] and p.get('image_1920'):
                  if not hasattr(sp, 'images') or not sp.images:
                      try:
                        img_data = p['image_1920']
                        if isinstance(img_data, bytes): img_data = img_data.decode('utf-8')
                        image = shopify.Image(prefix_options={'product_id': sp.id})
                        image.attachment = img_data
                        image.save()
                      except: pass

            # --- MAPPING ---
            try:
                pm = ProductMap.query.filter_by(sku=sku).first()
                if not pm:
                    v_id = sp.variants[0].id if sp.variants else '0'
                    pm = ProductMap(sku=sku, odoo_product_id=p['id'], shopify_variant_id=str(v_id), shop_url=shop_url)
                    db.session.add(pm)
                pm.last_synced_at = datetime.utcnow()
                db.session.commit()
            except: db.session.rollback()

            synced += 1

        # End of Batch
        del odoo_products
        gc.collect()
        log_event('Product Sync', 'Info', f"✅ Completed {batch_name}. Synced {synced} items.", shop_url=shop_url)
