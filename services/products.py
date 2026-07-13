import shopify
import json
import gc
import time
import hashlib
import math
import re
from difflib import SequenceMatcher
from datetime import datetime, timedelta
from models import Shop, ProductMap, AppSetting, db
from utils import get_odoo_connection, log_event, setup_shopify_session, get_config, get_shop_company_id, q_default


# =====================================================
# 0. HELPER FUNCTIONS
# =====================================================
def find_variant_in_cache(sku, shopify_product_cache):
    clean_sku = str(sku).strip()
    return shopify_product_cache.get(clean_sku)

# Known word-based UoM names with an implicit fixed quantity — not a digit
# suffix, so the regex below never catches these. Extend this map if other
# named quantities (e.g. "Pair", "Gross") show up in the Odoo UoM list.
_KNOWN_WORD_QUANTITIES = {
    'dozen': 12,
}

def extract_pack_size(uom_name):
    """
    Pulls the pack size out of a Purchase UoM name — any 'CTNX<n>' name works
    generically (e.g. 'CTNX24' -> 24, 'CTNX12' -> 12, 'CTNX150' -> 150,
    including any future CTNX<n> value, not just ones seen today), plus known
    word-based quantities like 'Dozen' -> 12. Returns None for UoMs with no
    defined pack multiplier (e.g. 'Unit', 'Each', 'Bag', 'CTN', 'm3', 'MTR1') —
    callers should skip writing the metafield in that case rather than guessing.

    Deliberately requires the 'CTNX' prefix rather than matching any trailing
    digit: dimensional/volume UoMs like 'm3' or 'MTR1' also end in a digit but
    aren't pack sizes, and a bare trailing-digit match would misfire on those.
    """
    if not uom_name:
        return None
    name = str(uom_name).strip()
    word_qty = _KNOWN_WORD_QUANTITIES.get(name.lower())
    if word_qty is not None:
        return word_qty
    match = re.match(r'ctnx(\d+)$', name, re.IGNORECASE)
    return int(match.group(1)) if match else None

def bisecting_read(odoo, model, ids, fields, shop_url, label, chunk_size=250):
    """
    Reads records in chunks; if a chunk's read call errors (e.g. Odoo's XML-RPC
    'cannot marshal NewId objects' bug on certain products), bisects that chunk down
    until the exact poison record(s) are isolated and skipped — instead of losing
    every product in the chunk over one bad record.

    Returns (records, failed_ids).
    """
    results = []
    failed = []

    def read_range(id_list):
        if not id_list:
            return
        try:
            data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, model, 'read', [id_list], {'fields': fields})
            results.extend(data)
        except Exception:
            if len(id_list) == 1:
                failed.append(id_list[0])
            else:
                mid = len(id_list) // 2
                read_range(id_list[:mid])
                read_range(id_list[mid:])

    for i in range(0, len(ids), chunk_size):
        read_range(ids[i:i + chunk_size])

    if failed:
        shown = failed[:20]
        suffix = f" (+{len(failed) - 20} more)" if len(failed) > 20 else ""
        log_event('Metafield Refresh', 'Warning',
            f"{label}: skipped {len(failed)} product(s) that error on read — Odoo IDs: {shown}{suffix}",
            shop_url=shop_url)

    return results, failed

# =====================================================
# 1. THE DISPATCHER
# =====================================================
def sync_products_master(shop_url):
    from app import app
    with app.app_context():
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop: return

        odoo = get_odoo_connection(shop_url)
        if not odoo:
            raise RuntimeError("Connection failed: could not reach Odoo")

        company_id = shop.odoo_company_id
        if not company_id: return

        # Look back 2 days for routine syncs to prevent memory crashes
        cutoff = datetime.utcnow() - timedelta(days=2)
        cutoff_str = cutoff.strftime('%Y-%m-%d %H:%M:%S')

        domain = [
            ['sale_ok', '=', True],
            ['type', 'in', ['product', 'consu']],
            ['active', '=', True],
            '|', ['write_date', '>=', cutoff_str], ['product_tmpl_id.write_date', '>=', cutoff_str],
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

        # Archive in Shopify any products archived in Odoo since the last sync
        try:
            archived = _archive_odoo_products_in_shopify(shop_url, odoo, cutoff_str=cutoff_str)
            if archived:
                log_event('Product Sync', 'Info',
                    f"Archived {archived} Shopify product(s) that were archived in Odoo.", shop_url=shop_url)
        except Exception as e:
            log_event('Product Sync', 'Warning', f"Archive propagation error: {e}", shop_url=shop_url)


# =====================================================
# 1.5 THE ABSOLUTE MASTER (FULL CATALOG RESYNC)
# =====================================================
def sync_all_products_absolute_master(shop_url):
    from app import app
    with app.app_context():
        shop = Shop.query.filter_by(shop_url=shop_url).first()
        if not shop: return
        
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): return

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

        # Full mode: compare ALL ProductMap entries against currently active Odoo products
        # and archive any Shopify products whose Odoo product has been deactivated.
        # This handles products archived before this feature existed (e.g. the current 700).
        try:
            archived = _archive_odoo_products_in_shopify(shop_url, odoo, full=True)
            if archived:
                log_event('Product Sync', 'Warning',
                    f"FULL RESYNC: Archived {archived} Shopify product(s) — no longer active in Odoo.", shop_url=shop_url)
        except Exception as e:
            log_event('Product Sync', 'Warning', f"Full archive propagation error: {e}", shop_url=shop_url)


# =====================================================
# 2. THE WORKER (OPTIMIZED)
# =====================================================
def sync_product_batch_task(shop_url, batch_ids, batch_name):
    from app import app
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): return
        
        # Cache currency in Redis for 1 hour — avoids 1 API call per batch
        from utils import conn as redis_conn
        currency_key = f"shopify_currency_{shop_url}"
        cached_currency = redis_conn.get(currency_key)
        if cached_currency:
            currency = cached_currency.decode('utf-8')
        else:
            try:
                currency = shopify.Shop.current().currency
                redis_conn.setex(currency_key, 3600, currency)
            except Exception:
                currency = 'NZD'  # Safe fallback

        # Bulk-load all product sync settings in one query — avoids N+1
        _cfg_keys = {'prod_sync_title', 'prod_sync_price', 'prod_sync_cost', 'prod_sync_desc',
                     'prod_sync_tags', 'prod_sync_images', 'prod_sync_vendor', 'prod_sync_barcode',
                     'prod_auto_create', 'prod_auto_publish',
                     'prod_sync_meta_original_price', 'prod_sync_meta_vendor_code',
                     'prod_sync_meta_qty_per_pack', 'prod_sync_meta_pack_size'}
        _rows = AppSetting.query.filter(
            AppSetting.shop_url == shop_url,
            AppSetting.key.in_(_cfg_keys)
        ).all()
        _raw = {}
        for r in _rows:
            try:
                _raw[r.key] = json.loads(r.value)
            except Exception:
                _raw[r.key] = r.value

        cfg = {
            'title':               _raw.get('prod_sync_title', True),
            'price':               _raw.get('prod_sync_price', True),
            'cost':                _raw.get('prod_sync_cost', True),
            'desc':                _raw.get('prod_sync_desc', True),
            'tags':                _raw.get('prod_sync_tags', False),
            'images':              _raw.get('prod_sync_images', False),
            'vendor':              _raw.get('prod_sync_vendor', True),
            'barcode':             _raw.get('prod_sync_barcode', True),
            'auto_create':         _raw.get('prod_auto_create', False),
            'auto_publish':        _raw.get('prod_auto_publish', False),
            'meta_original_price': _raw.get('prod_sync_meta_original_price', False),
            'meta_vendor_code':    _raw.get('prod_sync_meta_vendor_code', False),
            'meta_qty_per_pack':   _raw.get('prod_sync_meta_qty_per_pack', False),
            'meta_pack_size':      _raw.get('prod_sync_meta_pack_size', False),
            'currency': currency
        }

        # --- A. PREFETCH ODOO DATA ---
        fields = ['default_code', 'name', 'list_price', 'standard_price', 'active',
                  'uom_id', 'sh_is_secondary_unit', 'sh_secondary_uom',
                  'public_categ_ids', 'product_tag_ids', 'description_sale',
                  'image_1920', 'barcode', 'qty_per_pack', 'product_tmpl_id']
        if cfg['meta_pack_size']:
            fields.append('uom_po_id')

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

        # --- BULK SKU LOOKUP (1 GraphQL call for entire batch instead of 1 per product) ---
        bulk_shopify_cache = {}
        batch_skus_for_lookup = [str(p.get('default_code')).strip() for p in products if p.get('default_code')]

        if batch_skus_for_lookup:
            try:
                # Build a single GraphQL query with OR conditions for all SKUs in batch
                sku_query = " OR ".join([f"sku:'{s}'" for s in batch_skus_for_lookup])
                gql = """
                {
                  productVariants(first: 100, query: "%s") {
                    edges {
                      node {
                        sku
                        product { legacyResourceId }
                      }
                    }
                  }
                }
                """ % sku_query.replace('"', '\\"')

                client = shopify.GraphQL()
                result = json.loads(client.execute(gql))
                edges = result.get('data', {}).get('productVariants', {}).get('edges', [])

                for edge in edges:
                    node = edge.get('node', {})
                    v_sku = node.get('sku', '').strip()
                    product_id = node.get('product', {}).get('legacyResourceId')
                    if v_sku and product_id and v_sku in batch_skus_for_lookup:
                        try:
                            sp = shopify.Product.find(int(product_id))
                            bulk_shopify_cache[v_sku] = sp
                        except Exception:
                            pass
            except Exception as e:
                print(f"Bulk SKU lookup error: {e}")
                # Falls back to per-product lookup below

        for p in products:
            try:
                sku = str(p.get('default_code')).strip()

                # Use bulk cache result — fall back to individual lookup only if missing
                single_item_cache = {}
                if sku and sku in bulk_shopify_cache:
                    single_item_cache[sku] = bulk_shopify_cache[sku]
                elif sku:
                    # Fallback for any SKU missed by bulk query
                    shopify_id = find_shopify_product_by_sku(sku, shop_url=shop_url)
                    if shopify_id:
                        try:
                            single_item_cache[sku] = shopify.Product.find(shopify_id)
                        except Exception:
                            pass

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
        
        # Original Retail Price
        # Shopify metafield definition is type 'money' — value must be JSON {"amount":"X.XX","currency_code":"XXX"}
        if cfg.get('meta_original_price'):
            safe_price = "{:.2f}".format(float(p.get('list_price') or 0.0))
            currency = cfg.get('currency') or 'NZD'
            meta_targets.append({
                'ownerId': f"gid://shopify/Product/{sp.id}",
                'namespace': 'custom',
                'key': 'original_retail_price',
                'value': json.dumps({"amount": safe_price, "currency_code": currency}),
                'type': 'money'
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

        # Qty Per Pack (e.g. 12.00 for a CTNX12 carton)
        if cfg.get('meta_qty_per_pack'):
            meta_targets.append({
                'ownerId': f"gid://shopify/Product/{sp.id}",
                'namespace': 'custom',
                'key': 'qty_per_pack',
                'value': "{:.2f}".format(float(p.get('qty_per_pack') or 1.0)),
                'type': 'number_decimal'
            })

        # Pack Size — parsed from Purchase UoM name (e.g. 'CTNX24' -> 24)
        if cfg.get('meta_pack_size'):
            po_uom = p.get('uom_po_id')  # [id, "CTNX24"] or False
            pack_size = extract_pack_size(po_uom[1]) if po_uom else None
            if pack_size is not None:
                meta_targets.append({
                    'ownerId': f"gid://shopify/Product/{sp.id}",
                    'namespace': 'custom',
                    'key': 'pack_size',
                    'value': str(pack_size),
                    'type': 'number_integer'
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
            result = json.loads(client.execute(query, {'metafields': meta_targets}))
            errors = result.get('data', {}).get('metafieldsSet', {}).get('userErrors', [])
            if errors:
                log_event('Product', 'Warning', f"Metafield error for {sku}: {errors}", shop_url=shop_url)

    except Exception as e:
        log_event('Product', 'Warning', f"Metafield save error for {sku}: {e}", shop_url=shop_url)

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
# 4. ARCHIVE PROPAGATION — Odoo → Shopify
# =====================================================
def _archive_odoo_products_in_shopify(shop_url, odoo, cutoff_str=None, full=False):
    """
    Archives Shopify products whose matching Odoo product has been set to active=False.

    Delta mode  (full=False, cutoff_str provided):
        Queries Odoo for products archived within the sync window. Fast — only
        a handful of products are typically archived per cycle.

    Full mode   (full=True):
        Compares all ProductMap entries against currently active Odoo IDs.
        Any Shopify product mapped to a now-inactive Odoo product gets archived.
        Use for the one-time migration of products archived before this feature.

    SKU REUSE SAFETY:
        Before archiving any Shopify product, the function checks the product's
        primary variant SKU against a live set of all active Odoo SKUs. If the
        SKU is still active in Odoo (meaning a NEW product was created with the
        same SKU after the old one was archived), the Shopify product is left
        alone. This prevents false-archiving when SKUs are recycled.
        If the active-SKU safety set cannot be built, the function aborts
        entirely — never archives without verified safety data.
    """
    company_id = get_shop_company_id(shop_url)
    archived_count = 0

    # ── STEP 0: Build active-SKU safety guard (mandatory) ───────────────────
    # We fetch BOTH active Odoo IDs and active Odoo SKUs in one query.
    # Every archive candidate is checked against active_sku_set before touching
    # Shopify. If we cannot build this set, we abort — never risk false archives.
    active_domain = [['active', '=', True], ['type', 'in', ['product', 'consu']]]
    if company_id:
        active_domain += ['|', ['company_id', '=', int(company_id)], ['company_id', '=', False]]

    try:
        active_rows = odoo.models.execute_kw(
            odoo.db, odoo.uid, odoo.password,
            'product.product', 'search_read',
            [active_domain], {'fields': ['id', 'default_code']}
        )
        active_odoo_ids = set(r['id'] for r in active_rows)
        active_sku_set = set(
            str(r['default_code']).strip()
            for r in active_rows if r.get('default_code')
        )
    except Exception as e:
        log_event('Product Sync', 'Warning',
            f"Archive check aborted — could not build active-SKU safety set: {e}", shop_url=shop_url)
        return 0  # Never archive without confirmed active-SKU data

    def _sku_is_still_active(sp, label):
        """
        Returns True (safe to archive) if the Shopify product's primary variant
        SKU is NOT in the active Odoo SKU set.
        Returns False (skip) if the SKU is still live in Odoo — this means a
        new active product was created with the same SKU after the old one was
        archived (SKU reuse). Archiving would silently remove a live product.
        """
        if not sp or not sp.variants:
            return False
        primary_sku = str(getattr(sp.variants[0], 'sku', '') or '').strip()
        if primary_sku and primary_sku in active_sku_set:
            log_event('Product Sync', 'Warning',
                f"SKU reuse guard: '{primary_sku}' still active in Odoo — "
                f"skipping archive for {label}",
                shop_url=shop_url)
            return False
        return True

    if full:
        # ── 1. Find stale ProductMap entries ────────────────────────────────
        all_maps = ProductMap.query.filter(
            ProductMap.shop_url == shop_url,
            ProductMap.odoo_product_id != -1,
            ProductMap.shopify_variant_id != None
        ).all()

        stale_maps = [m for m in all_maps if m.odoo_product_id not in active_odoo_ids]

        if not stale_maps:
            log_event('Product Sync', 'Info', "Archive full-check: no stale products found.", shop_url=shop_url)
            return 0

        log_event('Product Sync', 'Warning',
            f"Archive full-check: {len(stale_maps)} candidate(s) — verifying each against active SKUs.",
            shop_url=shop_url)

        # ── 2. Archive confirmed stale products ─────────────────────────────
        # Pack products produce 2 ProductMap rows for the same Shopify product.
        # seen_shopify_ids ensures we only archive each Shopify product once.
        seen_shopify_ids = set()
        for pm in stale_maps:
            try:
                variant = shopify.Variant.find(int(pm.shopify_variant_id))
                product_id = variant.product_id
                if product_id in seen_shopify_ids:
                    continue

                sp = shopify.Product.find(product_id)

                # SKU reuse guard — do not archive if primary SKU is still live
                if not _sku_is_still_active(sp, pm.sku):
                    seen_shopify_ids.add(product_id)  # mark seen so we don't recheck
                    continue

                seen_shopify_ids.add(product_id)
                if sp and sp.status != 'archived':
                    sp.status = 'archived'
                    sp.save()
                    archived_count += 1
                    log_event('Product Sync', 'Info',
                        f"Archived Shopify product: {pm.sku} (inactive in Odoo)", shop_url=shop_url)
            except Exception as e:
                log_event('Product Sync', 'Warning',
                    f"Could not archive Shopify product for SKU {pm.sku}: {e}", shop_url=shop_url)

    else:
        # ── Delta: products archived in Odoo since cutoff_str ───────────────
        if not cutoff_str:
            return 0

        archive_domain = [
            ['active', '=', False],
            ['type', 'in', ['product', 'consu']],
            ['write_date', '>=', cutoff_str],
        ]
        if company_id:
            archive_domain += ['|', ['company_id', '=', int(company_id)], ['company_id', '=', False]]

        try:
            recently_archived = odoo.models.execute_kw(
                odoo.db, odoo.uid, odoo.password,
                'product.product', 'search_read', [archive_domain],
                {'fields': ['id', 'default_code'], 'context': {'active_test': False}}
            )
        except Exception as e:
            log_event('Product Sync', 'Warning', f"Archive delta query error: {e}", shop_url=shop_url)
            return 0

        if not recently_archived:
            return 0

        log_event('Product Sync', 'Info',
            f"Archive delta: {len(recently_archived)} product(s) archived in Odoo since last sync.",
            shop_url=shop_url)

        seen_shopify_ids = set()
        for p in recently_archived:
            odoo_id = p['id']
            sku = str(p.get('default_code') or '').strip()

            # Resolve Shopify product — ProductMap first (fast), SKU search as fallback
            product_id = None
            try:
                pm = ProductMap.query.filter_by(odoo_product_id=odoo_id, shop_url=shop_url).first()
                if pm and pm.shopify_variant_id:
                    variant = shopify.Variant.find(int(pm.shopify_variant_id))
                    product_id = variant.product_id
                elif sku:
                    product_id = find_shopify_product_by_sku(sku, shop_url)
            except Exception:
                pass

            if not product_id or product_id in seen_shopify_ids:
                continue

            try:
                sp = shopify.Product.find(product_id)

                # SKU reuse guard
                if not _sku_is_still_active(sp, sku or str(odoo_id)):
                    seen_shopify_ids.add(product_id)
                    continue

                seen_shopify_ids.add(product_id)
                if sp and sp.status != 'archived':
                    sp.status = 'archived'
                    sp.save()
                    archived_count += 1
                    log_event('Product Sync', 'Info',
                        f"Archived Shopify product: {sku or odoo_id} (archived in Odoo)", shop_url=shop_url)
            except Exception as e:
                log_event('Product Sync', 'Warning',
                    f"Could not archive Shopify product {sku or odoo_id}: {e}", shop_url=shop_url)

    return archived_count


# =====================================================
# 5. HELPERS & UTILITIES
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
    Force re-writes the metafields (original_retail_price + vendor_product_code + qty_per_pack)
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

        # Only run if at least one metafield is enabled in settings
        sync_price        = get_config('prod_sync_meta_original_price', False, shop_url=shop_url)
        sync_vendor        = get_config('prod_sync_meta_vendor_code',    False, shop_url=shop_url)
        sync_qty_per_pack  = get_config('prod_sync_meta_qty_per_pack',   False, shop_url=shop_url)
        sync_pack_size     = get_config('prod_sync_meta_pack_size',      False, shop_url=shop_url)

        # Fetch store currency (needed for money-type metafield value format)
        try:
            currency = shopify.Shop.current().currency
        except Exception:
            currency = 'NZD'

        if not sync_price and not sync_vendor and not sync_qty_per_pack and not sync_pack_size:
            return 0, "All metafields are disabled in settings — tick at least one to refresh"

        # 1. Load every mapped product for this shop (skip placeholder -1/0 rows —
        # 0 is never a valid Odoo record id and reading it errors on the Odoo side)
        maps = ProductMap.query.filter(
            ProductMap.shop_url == shop_url,
            ProductMap.odoo_product_id.notin_([-1, 0])
        ).all()

        if not maps:
            return 0, "No mapped products found"

        # Only request the fields each enabled metafield actually needs — qty_per_pack
        # in particular is a custom computed field that has been known to crash Odoo's
        # XML-RPC serialization for some products (NewId marshalling error), so it must
        # never be fetched unless prod_sync_meta_qty_per_pack is actually ticked.
        read_fields = ['id', 'list_price', 'product_tmpl_id']
        if sync_qty_per_pack:
            read_fields.append('qty_per_pack')
        if sync_pack_size:
            read_fields.append('uom_po_id')

        CHUNK_SIZE = 250
        WRITE_BATCH_SIZE = 50
        # SUPER_BATCH bounds how many products' Odoo+Shopify data live in memory at
        # once. This runs on a 512MB Render Starter instance that's already had
        # multiple OOM incidents (see recent worker/scheduler commits) — a
        # whole-catalog refresh held everything in memory at once and was getting
        # silently killed mid-run (jobs stuck at "Started" forever, no error logged,
        # because the process itself died rather than raising a catchable
        # exception). Processing in bounded super-batches with an explicit gc.collect()
        # between each keeps peak memory well below one super-batch's footprint
        # regardless of total catalog size.
        SUPER_BATCH = 500

        total_updated = 0
        total_errors = 0
        total_failed_ids = []
        total_products = len(maps)

        # Diagnostics for why pack_size might not populate for a given product,
        # even when sync_pack_size is enabled — tracks whether Odoo simply never
        # returned a uom_po_id at all vs. returned one that didn't parse.
        diag_no_uom = 0
        diag_unparsed_samples = {}  # {raw uom name: count}, capped below

        for sb_start in range(0, total_products, SUPER_BATCH):
            sb_maps = maps[sb_start:sb_start + SUPER_BATCH]
            odoo_ids    = [m.odoo_product_id for m in sb_maps]
            shopify_map = {m.odoo_product_id: m.shopify_variant_id for m in sb_maps}

            # 2. Bulk-fetch Odoo data for this super-batch (CHUNKED + bisected)
            odoo_products, failed_ids = bisecting_read(
                odoo, 'product.product', odoo_ids, read_fields, shop_url,
                label="Product read", chunk_size=CHUNK_SIZE
            )
            total_failed_ids.extend(failed_ids)

            if not odoo_products:
                continue

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

            # 4. Build lookup: odoo_id -> {price, vendor_code, qty_per_pack, pack_size}
            odoo_data = {}
            for p in odoo_products:
                tmpl_id = p['product_tmpl_id'][0] if p.get('product_tmpl_id') else None
                po_uom = p.get('uom_po_id')  # [id, "CTNX24"] or False
                pack_size = extract_pack_size(po_uom[1]) if po_uom else None

                if sync_pack_size:
                    if not po_uom:
                        diag_no_uom += 1
                    elif pack_size is None:
                        diag_unparsed_samples[po_uom[1]] = diag_unparsed_samples.get(po_uom[1], 0) + 1

                odoo_data[p['id']] = {
                    'price': "{:.2f}".format(float(p.get('list_price') or 0.0)),
                    'vendor_code': vendor_code_map.get(tmpl_id, '') if tmpl_id else '',
                    'qty_per_pack': "{:.2f}".format(float(p.get('qty_per_pack') or 1.0)),
                    'pack_size': pack_size
                }

            # 5. Walk through each product in this super-batch and force-write metafields
            map_items = list(shopify_map.items())  # [(odoo_id, shopify_variant_id), ...]

            for i in range(0, len(map_items), WRITE_BATCH_SIZE):
                batch = map_items[i:i + WRITE_BATCH_SIZE]
                variant_ids = [str(v_id) for _, v_id in batch]

                # Bulk-fetch Shopify product IDs by variant ID via GraphQL. metafieldsSet
                # always upserts (create or update) regardless of current value, so we
                # don't need each product's existing metafield state here — fetching it
                # was dead weight adding unnecessary query cost per call.
                gql = """
                query($ids: [ID!]!) {
                  nodes(ids: $ids) {
                    ... on ProductVariant {
                      id
                      product {
                        id
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
                    total_errors += len(batch)
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

                    # Build the metafield mutations
                    mutations = []

                    if sync_price:
                        mutations.append({
                            'key':   'original_retail_price',
                            'value': json.dumps({"amount": data['price'], "currency_code": currency}),
                            'type':  'money'
                        })

                    if sync_vendor and data['vendor_code']:
                        mutations.append({
                            'key':   'vendor_product_code',
                            'value': data['vendor_code'],
                            'type':  'single_line_text_field'
                        })

                    if sync_qty_per_pack:
                        mutations.append({
                            'key':   'qty_per_pack',
                            'value': data['qty_per_pack'],
                            'type':  'number_decimal'
                        })

                    if sync_pack_size and data['pack_size'] is not None:
                        mutations.append({
                            'key':   'pack_size',
                            'value': str(data['pack_size']),
                            'type':  'number_integer'
                        })

                    if mutations:
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
                        } for m in mutations]}

                        try:
                            raw = client.execute(set_gql, set_vars)
                            result = json.loads(raw)
                        except Exception as e:
                            total_errors += len(mutations)
                            log_event('Metafield Refresh', 'Warning',
                                f"Metafield write transport error for {product_gid}: {e}", shop_url=shop_url)
                            continue

                        # Shopify's GraphQL API returns HTTP 200 even when a mutation is
                        # rejected — the failure only shows up in these two response
                        # fields, never as a Python exception. Previously neither was
                        # checked, so rejected writes (throttling, validation errors)
                        # were silently counted as successes.
                        top_level_errors = result.get('errors')
                        user_errors = result.get('data', {}).get('metafieldsSet', {}).get('userErrors', [])

                        if top_level_errors or user_errors:
                            total_errors += len(mutations)
                            keys = ', '.join(m['key'] for m in mutations)
                            log_event('Metafield Refresh', 'Warning',
                                f"Metafield write rejected ({keys}) for {product_gid}: "
                                f"{top_level_errors or user_errors}", shop_url=shop_url)
                        else:
                            total_updated += len(mutations)

            # Free this super-batch's data before starting the next one
            del odoo_products, odoo_data, vendor_code_map, map_items, shopify_map
            gc.collect()

            log_event('Metafield Refresh', 'Info',
                f"Progress: {min(sb_start + SUPER_BATCH, total_products)}/{total_products} products processed, "
                f"{total_updated} metafields updated so far",
                shop_url=shop_url)

        summary = f"Metafield refresh complete. Updated {total_updated} metafields across {total_products} products."
        if total_errors:
            summary += f" ({total_errors} errors — check logs)"
        if total_failed_ids:
            summary += f" ({len(total_failed_ids)} product(s) skipped — Odoo read errors, see logs)"
        log_event('Metafield Refresh', 'Success', summary, shop_url=shop_url)

        if sync_pack_size:
            top_unparsed = sorted(diag_unparsed_samples.items(), key=lambda kv: -kv[1])[:15]
            log_event('Metafield Refresh', 'Info',
                f"pack_size diagnostics: {diag_no_uom} product(s) had no uom_po_id returned by Odoo at all; "
                f"top unparsed Purchase UoM values seen (name: count): {top_unparsed}",
                shop_url=shop_url)

        return total_updated, summary
