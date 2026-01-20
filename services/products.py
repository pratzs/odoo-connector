import shopify
import json
import gc
from datetime import datetime
from models import db, Shop, ProductMap
from utils import get_config, log_event, setup_shopify_session, get_odoo_connection

# --- HELPER: GraphQL Product Lookups ---
def find_shopify_product_by_sku(sku, shop_url=None):
    try:
        if not shopify.ShopifyResource.site:
            return None 
    except: return None

    query = """{ productVariants(first: 5, query: "sku:%s") { edges { node { sku product { legacyResourceId } } } } }""" % sku
    try:
        client = shopify.GraphQL()
        result = client.execute(query)
        data = json.loads(result)
        edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
        for edge in edges:
            node = edge['node']
            if node.get('sku') == sku:
                return node['product']['legacyResourceId']
    except Exception as e: print(f"GraphQL Error: {e}")
    return None

def process_product_data(data, odoo_client, shop_url=None):
    """
    Handles Shopify Product Webhooks.
    CURRENT STATUS: Disabled (Odoo is Master).
    """
    return 0

def archive_shopify_duplicates(shop_url):
    """Scans Shopify for duplicate SKUs and archives the older ones."""
    # Add Import locally for context
    from app import app
    
    with app.app_context():
        if not setup_shopify_session(shop_url): return

        log_event('Duplicate Scan', 'Info', "Starting scan for duplicate SKUs...", shop_url=shop_url)
        
        sku_map = {} 
        page = shopify.Product.find(limit=250)
        
        # 1. Build Map
        while page:
            for product in page:
                if product.status == 'archived': continue
                sku = product.variants[0].sku if product.variants else None
                if sku:
                    if sku not in sku_map: sku_map[sku] = []
                    sku_map[sku].append(product)
            
            if page.has_next_page(): page = page.next_page()
            else: break
        
        # 2. Process Duplicates
        archived_count = 0
        for sku, products in sku_map.items():
            if len(products) > 1:
                products.sort(key=lambda x: x.created_at, reverse=True)
                to_archive = products[1:]
                for p in to_archive:
                    try:
                        p.status = 'archived'
                        p.save()
                        archived_count += 1
                        log_event('Duplicate Scan', 'Warning', f"Archived Duplicate: {p.title} (SKU: {sku})", shop_url=shop_url)
                    except Exception as e:
                        print(f"Failed to archive {p.id}: {e}")

        log_event('Duplicate Scan', 'Success', f"Scan Complete. Archived {archived_count} duplicates.", shop_url=shop_url)

def cleanup_shopify_products(shop_url):
    """
    Safely cleans up Shopify Duplicates (Archives duplicates, keeps original).
    """
    from app import app
    
    with app.app_context():
        if not setup_shopify_session(shop_url): return
        seen_skus = set()
        
        page = shopify.Product.find(limit=250)
        archived_count = 0
        
        try:
            while page:
                for sp in page:
                    variant = sp.variants[0] if sp.variants else None
                    if not variant or not variant.sku: continue
                    
                    sku = variant.sku
                    needs_archive = False
                    
                    # ONLY archive if we have already seen this SKU in this loop (Duplicate)
                    if sku in seen_skus: 
                        needs_archive = True
                    
                    if needs_archive:
                        if sp.status != 'archived':
                            sp.status = 'archived'
                            sp.save()
                            archived_count += 1
                            log_event('System', 'Warning', f"Archived Duplicate in Shopify: {sku}", shop_url=shop_url)
                    else: 
                        seen_skus.add(sku)
                
                if page.has_next_page(): 
                    page = page.next_page()
                else: 
                    break
        except Exception as e:
            print(f"Cleanup Error: {e}")
            
        if archived_count > 0: 
            log_event('System', 'Success', f"Cleanup Complete. Archived {archived_count} duplicates.", shop_url=shop_url)

def sync_products_master(shop_url):
    """
    DYNAMIC Odoo -> Shopify Product Sync.
    """
    # 1. IMPORT APP FOR CONTEXT
    from app import app

    # 2. WRAP LOGIC IN CONTEXT
    with app.app_context():
        # 1. Load Shop Data from DB
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop:
            log_event('System', 'Error', f"Sync Failed: Shop {shop_url} not found in DB.")
            return

        # 2. Connect to Odoo
        odoo = get_odoo_connection(shop_url)
        if not odoo:
            log_event('System', 'Error', f"Sync Failed: Could not connect to Odoo for {shop_url}")
            return

        # 3. Connect to Shopify
        try:
            if not setup_shopify_session(shop_url):
                 log_event('System', 'Error', f"Shopify Auth Failed", shop_url=shop_url)
                 return
            shop_info = shopify.Shop.current()
            currency_code = shop_info.currency
        except Exception as e:
            log_event('System', 'Error', f"Shopify Auth Failed: {e}")
            return

        # 4. Get Company ID
        company_id = shop.odoo_company_id
        if not company_id:
            log_event('Product Sync', 'Error', "CRITICAL: No Odoo Company ID set for this shop.")
            return

        # --- STEP 1: GET IDs ONLY ---
        log_event('Product Sync', 'Info', f"Fetching Odoo product IDs...", shop_url=shop_url)
        
        domain = [
            ['sale_ok', '=', True],
            ['type', 'in', ['product', 'consu']],
            ['company_id', '=', int(company_id)]
        ]
        
        try:
            product_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'product.product', 'search', [domain])
        except Exception as e:
            log_event('Product Sync', 'Error', f"Odoo Search Failed: {e}", shop_url=shop_url)
            return

        total_count = len(product_ids)
        log_event('Product Sync', 'Info', f"Found {total_count} sellable products. Starting Batch Sync...", shop_url=shop_url)

        # --- PRE-LOAD CACHES ---
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

        # --- CONFIGS ---
        sync_title = get_config('prod_sync_title', True, shop_url=shop_url)
        sync_price = get_config('prod_sync_price', True, shop_url=shop_url)
        sync_cost = get_config('prod_sync_cost', True, shop_url=shop_url)
        sync_desc = get_config('prod_sync_desc', True, shop_url=shop_url)
        sync_tags = get_config('prod_sync_tags', False, shop_url=shop_url)
        sync_original_price_meta = get_config('prod_sync_meta_original_price', False, shop_url=shop_url)
        sync_images = get_config('prod_sync_images', False, shop_url=shop_url)
        sync_vendor = get_config('prod_sync_vendor', True, shop_url=shop_url) 
        sync_barcode = get_config('prod_sync_barcode', True, shop_url=shop_url)
        
        auto_create = get_config('prod_auto_create', False, shop_url=shop_url)
        auto_publish = get_config('prod_auto_publish', False, shop_url=shop_url)

        synced = 0
        BATCH_SIZE = 50

        # --- STEP 2: BATCH LOOP ---
        for i in range(0, total_count, BATCH_SIZE):
            batch_ids = product_ids[i:i + BATCH_SIZE]
            
            fields = [
                'default_code', 'name', 'list_price', 'standard_price', 'weight', 
                'active', 'uom_id', 'sh_is_secondary_unit', 'sh_secondary_uom', 
                'public_categ_ids', 'product_tag_ids', 'description_sale', 
                'product_tmpl_id', 'image_1920', 'barcode', 'qty_per_pack'
            ]
            
            try:
                ctx = {'company_id': int(company_id)}
                odoo_products = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                    'product.product', 'read', [batch_ids], {'fields': fields, 'context': ctx})
            except Exception as e:
                log_event('Product Sync', 'Error', f"Batch Read Error: {e}", shop_url=shop_url)
                continue

            for p in odoo_products:
                sku = p.get('default_code')
                if not sku: continue
                if not p.get('active', True): continue

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
                shopify_id = find_shopify_product_by_sku(sku, shop_url=shop_url)
                sp = None
                
                if shopify_id:
                    try: sp = shopify.Product.find(shopify_id)
                    except: sp = None
                
                if not sp:
                    if not auto_create: continue
                    sp = shopify.Product()
                    sp.title = p['name']
                    sp.vendor = vendor_name 
                    sp.product_type = odoo.get_public_category_name(p.get('public_categ_ids', [])) or '' 
                    sp.status = 'active' if auto_publish else 'draft'
                
                if sync_title: sp.title = p['name']
                if sync_vendor: sp.vendor = vendor_name 
                if sync_desc: sp.body_html = p.get('description_sale') or ''
                
                if sync_tags:
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
                    
                    if sync_price: 
                        match.price = des['price']
                        match.compare_at_price = des['price'] 
                        
                    if sync_barcode and des['barcode']: 
                        match.barcode = des['barcode']
                    match.inventory_management = 'shopify'
                    final_vars.append(match)

                sp.variants = final_vars
                sp.save()
                
                if sync_original_price_meta:
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
                    except Exception as e:
                        print(f"Metafield Price Error for {sku}: {e}")
                
                if sp.variants:
                    for v in sp.variants:
                        d_data = next((d for d in desired_variants if d['sku'] == v.sku), None)
                        if d_data and v.inventory_item_id and sync_cost:
                            try:
                                inv_item = shopify.InventoryItem.find(v.inventory_item_id)
                                if inv_item:
                                    inv_item.cost = d_data['cost'] 
                                    inv_item.tracked = True
                                    inv_item.save()
                            except: pass

                if get_config('prod_sync_meta_vendor_code', False, shop_url=shop_url):
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

                if sync_images and p.get('image_1920'):
                      if not hasattr(sp, 'images') or not sp.images:
                          try:
                            img_data = p['image_1920']
                            if isinstance(img_data, bytes): img_data = img_data.decode('utf-8')
                            image = shopify.Image(prefix_options={'product_id': sp.id})
                            image.attachment = img_data
                            image.save()
                          except: pass

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

            del odoo_products
            gc.collect()
            log_event('Product Sync', 'Info', f"Processed batch {i}-{i+BATCH_SIZE}...", shop_url=shop_url)

        log_event('Product Sync', 'Success', f"Sync Complete. Synced {synced} products.", shop_url=shop_url)
