"""
Clearance mirror sync.

The main inventory sync (perform_inventory_sync in app.py) shows only normal
stock — it excludes the Clearance + Damaged locations. This module runs as a
second pass and does the mirror of that: it takes the stock sitting in the
Clearance + Damaged locations and pushes it onto a *separate* Shopify product
per SKU ('{base_sku}{suffix}', e.g. 'ABC123-CLR') so that clearance stock can:

  - carry its own discounted price (clearance_discount_pct, default 40%),
  - live only in the Clearance collection (not the main catalogue),
  - surface an expiry date pulled from the Odoo lot(s).

Because the two location sets are disjoint (main sync excludes exactly what this
pass includes), a product with stock in both Pick/Bulk and Clearance shows the
full quantity on its normal listing and the clearance quantity on its mirror —
no double counting.

Orders for a '-CLR' SKU route back to the correct Odoo product for free:
services/orders.py already strips a trailing '-<suffix>' to re-resolve the base
product, and sends the exact (discounted) line price to Odoo with
manual_price=True. No order-side code is needed here.

Mirrors are tracked in the ClearanceMirror table so stale ones (clearance stock
gone to 0) can be zeroed out and set to draft.
"""

import math
import shopify
from datetime import datetime

from models import db, ProductMap, ClearanceMirror
from utils import (
    get_odoo_connection, log_event, setup_shopify_session,
    get_config, get_shop_company_id,
)


def _truthy(val):
    """AppSetting values come back as parsed JSON (True/False) or raw strings."""
    if isinstance(val, bool):
        return val
    if val is None:
        return False
    return str(val).strip().lower() in ('true', '1', 'yes', 'on')


def _int_list(val):
    out = []
    for v in (val or []):
        try:
            out.append(int(v))
        except (TypeError, ValueError):
            continue
    return out


def _read_lot_expiry(odoo, lot_ids):
    """
    Return {lot_id: 'YYYY-MM-DD'} for lots that have an expiration date.
    Odoo renamed the model (stock.production.lot -> stock.lot) and the
    expiration_date field only exists when the product_expiry module is
    installed, so degrade gracefully if neither is present.
    """
    out = {}
    if not lot_ids:
        return out
    for model in ('stock.lot', 'stock.production.lot'):
        try:
            recs = odoo.models.execute_kw(
                odoo.db, odoo.uid, odoo.password,
                model, 'read', [lot_ids], {'fields': ['expiration_date']})
            for r in recs:
                exp = r.get('expiration_date')
                if exp:
                    out[r['id']] = str(exp)[:10]
            return out
        except Exception:
            continue
    return out


def _resolve_shopify_location_id(shop_url):
    """Same target location the main inventory sync pushes to."""
    saved = get_config('shopify_target_location_id', None, shop_url=shop_url)
    if saved:
        try:
            return int(saved)
        except (TypeError, ValueError):
            pass
    try:
        active = [l for l in shopify.Location.find() if l.active]
        if active:
            return active[0].id
    except Exception:
        pass
    return None


def _set_inventory(location_id, inventory_item_id, qty):
    """Set available qty, connecting the item to the location first if needed
    (a freshly-created mirror variant isn't stocked at the location yet)."""
    try:
        shopify.InventoryLevel.set(location_id=location_id,
                                   inventory_item_id=inventory_item_id,
                                   available=int(qty))
        return
    except Exception:
        pass
    try:
        shopify.InventoryLevel.connect(location_id, inventory_item_id)
    except Exception:
        pass
    shopify.InventoryLevel.set(location_id=location_id,
                               inventory_item_id=inventory_item_id,
                               available=int(qty))


def _find_product_id_by_sku(sku):
    """Locate an existing Shopify product by a variant SKU (avoids duplicate
    mirror creation if a row was lost). Returns legacy product id or None."""
    try:
        client = shopify.GraphQL()
        import json as _json
        res = _json.loads(client.execute(
            '{ productVariants(first: 1, query: "sku:\'%s\'") '
            '{ edges { node { sku product { legacyResourceId } } } } }'
            % sku.replace("'", "\\'")))
        for edge in res.get('data', {}).get('productVariants', {}).get('edges', []):
            node = edge.get('node', {})
            if (node.get('sku') or '').strip() == sku:
                return int(node['product']['legacyResourceId'])
    except Exception:
        pass
    return None


def _get_base_shopify_product(base_sku):
    """Fetch the base Shopify product for a SKU. Uses the GraphQL exact-SKU
    lookup (shopify.Variant.find(params={'sku': ...}) is unreliable — it does
    not filter server-side and can return the wrong product, which caused the
    draft to target the wrong product)."""
    pid = _find_product_id_by_sku(base_sku)
    if pid:
        try:
            return shopify.Product.find(pid)
        except Exception:
            return None
    return None


def _draft_base_product(shop_url, base_sku, row):
    """
    Draft the normal product because its only remaining stock is clearance
    stock (sold via the mirror). Records base_drafted=True ONLY when we
    actually transition it active->draft — so a product the merchant drafted
    for some other reason is never claimed (and never auto-reactivated) by us.
    Does not commit; the caller does.
    """
    # Always resolve the base fresh by SKU (do NOT trust a cached
    # base_shopify_product_id — an earlier bug cached the wrong product id).
    prod = _get_base_shopify_product(base_sku)
    if prod is None:
        return
    row.base_shopify_product_id = str(prod.id)
    if getattr(prod, 'status', None) == 'active':
        prod.status = 'draft'
        try:
            prod.save()
            row.base_drafted = True
            log_event('Clearance', 'Info',
                      f"Drafted normal product {base_sku} — only clearance stock remains",
                      shop_url=shop_url)
        except Exception as e:
            log_event('Clearance', 'Warning', f"Could not draft {base_sku}: {e}", shop_url=shop_url)


def _reactivate_base_if_ours(shop_url, row):
    """Re-activate the normal product, but only if WE drafted it. Does not
    commit; the caller does."""
    if not row.base_drafted:
        return
    # Resolve fresh by SKU (ignore any stale/wrong cached id).
    prod = _get_base_shopify_product(row.base_sku)
    if prod is not None and getattr(prod, 'status', None) != 'active':
        prod.status = 'active'
        try:
            prod.save()
            log_event('Clearance', 'Info',
                      f"Re-activated normal product {row.base_sku} — stock available again",
                      shop_url=shop_url)
        except Exception as e:
            log_event('Clearance', 'Warning', f"Could not re-activate {row.base_sku}: {e}", shop_url=shop_url)
            return
    row.base_drafted = False


def _write_clearance_metafields(product_id, expiry):
    """Write the theme-facing signals on the mirror product:
      - clearance.is_clearance (boolean) — always true; the theme keys the
        'Clearance' badge off this (product tag 'Clearance' is set too).
      - clearance.expiry_date (date)     — earliest lot expiry, or cleared.
    """
    owner = f"gid://shopify/Product/{product_id}"
    client = shopify.GraphQL()

    metafields = [{
        'ownerId': owner,
        'namespace': 'clearance',
        'key': 'is_clearance',
        'value': 'true',
        'type': 'boolean',
    }]
    if expiry:
        metafields.append({
            'ownerId': owner,
            'namespace': 'clearance',
            'key': 'expiry_date',
            'value': expiry,
            'type': 'date',
        })

    client.execute("""
        mutation($metafields: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $metafields) {
            userErrors { field message }
          }
        }""", {'metafields': metafields})

    # No expiry → clear any stale value so the theme never shows an old date.
    if not expiry:
        try:
            client.execute("""
                mutation($metafields: [MetafieldIdentifierInput!]!) {
                  metafieldsDelete(metafields: $metafields) {
                    userErrors { field message }
                  }
                }""", {'metafields': [{
                'ownerId': owner,
                'namespace': 'clearance',
                'key': 'expiry_date',
            }]})
        except Exception:
            pass


def _add_to_collection(product_id, collection_id):
    """Add the mirror to the manual Clearance collection. Idempotent-ish:
    Shopify errors if it's already a member, which we swallow."""
    if not collection_id:
        return
    try:
        shopify.Collect.create({'product_id': int(product_id),
                                'collection_id': int(collection_id)})
    except Exception:
        pass


def _upsert_mirror(shop_url, pid, base_sku, clr_sku, info,
                   clr_price, base_price, final_qty, expiry,
                   shopify_location_id, collection_id, normal_qty=None):
    """Create or update the mirror product + variant, push qty, write expiry.

    normal_qty: the product's stock in the normal (Pick/Bulk) locations, used
    to decide the base product's visibility. None = don't manage the base
    product (normal stock couldn't be determined)."""
    row = ClearanceMirror.query.filter_by(shop_url=shop_url, base_sku=base_sku).first()

    sp = None
    if row and row.shopify_product_id:
        try:
            sp = shopify.Product.find(int(row.shopify_product_id))
        except Exception:
            sp = None
    if sp is None:
        existing_id = _find_product_id_by_sku(clr_sku)
        if existing_id:
            try:
                sp = shopify.Product.find(existing_id)
            except Exception:
                sp = None

    created = False
    if sp is None:
        # --- Create a fresh mirror, copying the base product's look ---
        base = _get_base_shopify_product(base_sku)
        sp = shopify.Product()
        sp.title = f"{(info.get('name') or base_sku)} (Clearance)"
        sp.status = 'active'
        sp.tags = 'Clearance'
        sp.body_html = (getattr(base, 'body_html', None)
                        or info.get('description_sale') or '')
        # Deliberately NO vendor and NO product_type on the mirror: those are
        # what the store's smart collections (Nestlé, Explore, vendor/type
        # pages) match on, so leaving them blank keeps clearance items out of
        # every collection except the manual Clearance one.
        sp.variants = [shopify.Variant({
            'option1': 'Default Title',
            'sku': clr_sku,
            'price': str(clr_price),
            'inventory_management': 'shopify',
        })]
        sp.save()
        if getattr(sp, 'errors', None) and sp.errors.full_messages():
            raise RuntimeError(f"Shopify rejected mirror: {sp.errors.full_messages()}")
        created = True

        # Copy images (once, on creation) so the clearance listing looks right
        if base is not None and getattr(base, 'images', None):
            for img in base.images:
                try:
                    src = getattr(img, 'src', None)
                    if src:
                        shopify.Image({'product_id': sp.id, 'src': src}).save()
                except Exception:
                    pass
    else:
        # --- Update existing mirror ---
        if sp.status != 'active':
            sp.status = 'active'
        variant = None
        for v in (sp.variants or []):
            if (v.sku or '').strip() == clr_sku:
                variant = v
                break
        if variant is None and sp.variants:
            variant = sp.variants[0]
            variant.sku = clr_sku
        if variant is not None:
            variant.price = str(clr_price)
            variant.inventory_management = 'shopify'
        # Keep the mirror "inert" so smart collections can't pull it in:
        # ONLY the Clearance tag (strips any 'New' etc. added by other apps),
        # and no vendor / product_type. Re-asserted every sync.
        sp.tags = 'Clearance'
        sp.vendor = ''
        sp.product_type = ''
        sp.save()

    variant = sp.variants[0]
    inv_item_id = variant.inventory_item_id

    # compare_at_price → shows the original crossed out on the storefront
    if base_price and base_price > clr_price:
        try:
            variant.compare_at_price = str(base_price)
            variant.save()
        except Exception:
            pass

    if collection_id and created:
        _add_to_collection(sp.id, collection_id)

    _set_inventory(shopify_location_id, inv_item_id, final_qty)

    try:
        _write_clearance_metafields(sp.id, expiry)
    except Exception as e:
        log_event('Clearance', 'Warning', f"Clearance metafields failed for {clr_sku}: {e}", shop_url=shop_url)

    # --- Persist the mapping ---
    if not row:
        row = ClearanceMirror(shop_url=shop_url, base_sku=base_sku, clr_sku=clr_sku,
                              odoo_product_id=pid)
        db.session.add(row)
    row.clr_sku = clr_sku
    row.odoo_product_id = pid
    row.shopify_product_id = str(sp.id)
    row.shopify_variant_id = str(variant.id)
    row.inventory_item_id = str(inv_item_id) if inv_item_id else None
    row.last_qty = int(final_qty)
    row.is_active = True
    row.last_synced_at = datetime.utcnow()

    # Normal-product lifecycle: hide it when its only stock is clearance,
    # bring it back when normal stock returns.
    if normal_qty is not None and normal_qty <= 0:
        _draft_base_product(shop_url, base_sku, row)
    else:
        _reactivate_base_if_ours(shop_url, row)

    db.session.commit()

    return created


def _zero_out_stale(shop_url, active_base_skus, shopify_location_id):
    """Any previously-active mirror whose base SKU no longer has clearance
    stock is zeroed and drafted (kept, not deleted, so it can come back)."""
    rows = ClearanceMirror.query.filter_by(shop_url=shop_url, is_active=True).all()
    drafted = 0
    for row in rows:
        if row.base_sku in active_base_skus:
            continue
        try:
            if row.inventory_item_id:
                _set_inventory(shopify_location_id, int(row.inventory_item_id), 0)
            if row.shopify_product_id:
                try:
                    p = shopify.Product.find(int(row.shopify_product_id))
                    if p and p.status != 'draft':
                        p.status = 'draft'
                        p.save()
                except Exception:
                    pass
            # Clearance stock is gone — bring the normal product back if we
            # had drafted it (only-clearance state has ended).
            _reactivate_base_if_ours(shop_url, row)
            row.is_active = False
            row.last_qty = 0
            row.last_synced_at = datetime.utcnow()
            db.session.commit()
            drafted += 1
        except Exception as e:
            db.session.rollback()
            log_event('Clearance', 'Warning',
                      f"Zero-out failed for {row.clr_sku}: {e}", shop_url=shop_url)
    return drafted


def perform_clearance_sync(shop_url):
    """
    Second inventory pass — push Clearance + Damaged stock onto discounted
    mirror products. Safe no-op when clearance is disabled. Relies on an active
    Flask app context (caller supplies it, matching the other service jobs).
    """
    if not _truthy(get_config('clearance_enabled', False, shop_url=shop_url)):
        return

    clearance_locs = _int_list(get_config('clearance_locations', [], shop_url=shop_url))
    if not clearance_locs:
        log_event('Clearance', 'Warning',
                  'Clearance sync enabled but no clearance locations selected.',
                  shop_url=shop_url)
        return

    log_event('Clearance', 'Info', 'Starting Clearance Sync...', shop_url=shop_url)

    # Discount + naming config
    try:
        discount_pct = float(get_config('clearance_discount_pct', 40, shop_url=shop_url))
    except (TypeError, ValueError):
        discount_pct = 40.0
    price_factor = max(0.0, 1.0 - discount_pct / 100.0)
    suffix = get_config('clearance_sku_suffix', '-CLR', shop_url=shop_url) or '-CLR'
    collection_id = get_config('clearance_collection_id', None, shop_url=shop_url)

    odoo = get_odoo_connection(shop_url)
    if not odoo or not setup_shopify_session(shop_url):
        log_event('Clearance', 'Error', 'Connection failed (Odoo or Shopify).', shop_url=shop_url)
        return

    shopify_location_id = _resolve_shopify_location_id(shop_url)
    if not shopify_location_id:
        log_event('Clearance', 'Error', 'No active Shopify location found.', shop_url=shop_url)
        return

    # Base SKU -> Odoo product id, from the local map (skip mirror SKUs).
    base_by_pid = {}
    maps = ProductMap.query.filter(
        ProductMap.shop_url == shop_url,
        ProductMap.odoo_product_id > 0,
    ).all()
    for m in maps:
        if not m.sku or m.sku.endswith(suffix):
            continue
        base_by_pid.setdefault(m.odoo_product_id, m.sku)

    all_pids = list(base_by_pid.keys())
    if not all_pids:
        log_event('Clearance', 'Info', 'No mapped products to check.', shop_url=shop_url)
        return

    # Stock sitting in the clearance/damaged locations, with lots for expiry.
    try:
        quant_domain = [
            ['product_id', 'in', all_pids],
            ['location_id', 'child_of', clearance_locs],
        ]
        quants = odoo.models.execute_kw(
            odoo.db, odoo.uid, odoo.password,
            'stock.quant', 'search_read', [quant_domain],
            {'fields': ['product_id', 'quantity', 'lot_id']})
    except Exception as e:
        log_event('Clearance', 'Error', f"Odoo clearance stock fetch failed: {e}", shop_url=shop_url)
        return

    pid_qty = {}
    pid_lots = {}
    for q in quants:
        prod = q.get('product_id')
        if not prod:
            continue
        pid = prod[0]
        pid_qty[pid] = pid_qty.get(pid, 0.0) + float(q.get('quantity') or 0.0)
        lot = q.get('lot_id')
        if lot and lot[0]:
            pid_lots.setdefault(pid, set()).add(lot[0])

    # Earliest expiry per product (most conservative when multiple lots present)
    lot_expiry = _read_lot_expiry(odoo, sorted({l for s in pid_lots.values() for l in s}))
    pid_expiry = {}
    for pid, lots in pid_lots.items():
        dates = [lot_expiry[l] for l in lots if lot_expiry.get(l)]
        if dates:
            pid_expiry[pid] = min(dates)

    in_stock_pids = [pid for pid, q in pid_qty.items() if q > 0]

    # Pack + pricing data only for products that actually have clearance stock.
    prod_info = {}
    if in_stock_pids:
        try:
            recs = odoo.models.execute_kw(
                odoo.db, odoo.uid, odoo.password,
                'product.product', 'read', [in_stock_pids],
                {'fields': ['default_code', 'name', 'list_price',
                            'sh_is_secondary_unit', 'qty_per_pack', 'description_sale']})
            for r in recs:
                prod_info[r['id']] = r
        except Exception as e:
            log_event('Clearance', 'Error', f"Odoo product read failed: {e}", shop_url=shop_url)
            return

    # Normal-location stock for the same products — decides whether the base
    # product is hidden (only clearance left) or shown. Computed the same way
    # the main sync computes displayed stock (target locations minus excludes),
    # so the draft decision matches what Shopify actually shows. Requires the
    # clearance locations to be in the exclude list (as the UI instructs).
    target_locs = _int_list(get_config('inventory_locations', [], shop_url=shop_url))
    exclude_locs = _int_list(get_config('inventory_locations_exclude', [], shop_url=shop_url))
    manage_base = bool(target_locs)
    normal_stock = {}
    if in_stock_pids and target_locs:
        try:
            ndomain = [['product_id', 'in', in_stock_pids], ['location_id', 'child_of', target_locs]]
            if exclude_locs:
                ndomain.append(['location_id', 'not in', exclude_locs])
            ngroups = odoo.models.execute_kw(
                odoo.db, odoo.uid, odoo.password,
                'stock.quant', 'read_group',
                [ndomain, ['product_id', 'quantity'], ['product_id']])
            for g in ngroups:
                if g.get('product_id'):
                    normal_stock[g['product_id'][0]] = float(g.get('quantity') or 0.0)
        except Exception as e:
            manage_base = False
            log_event('Clearance', 'Warning',
                      f"Normal-stock read failed — base products won't be drafted this run: {e}",
                      shop_url=shop_url)
    elif not target_locs:
        log_event('Clearance', 'Info',
                  'No inventory (normal) locations configured — skipping normal-product draft logic.',
                  shop_url=shop_url)

    log_event('Clearance', 'Info',
              f"{len(in_stock_pids)} products with clearance stock "
              f"(locations {clearance_locs}, {discount_pct:.0f}% off).",
              shop_url=shop_url)

    processed = 0
    active_base_skus = set()
    for pid in in_stock_pids:
        info = prod_info.get(pid, {})
        base_sku = info.get('default_code') or base_by_pid.get(pid)
        if not base_sku:
            continue

        raw_qty = pid_qty.get(pid, 0.0)
        is_pack = info.get('sh_is_secondary_unit', False)
        qty_per_pack = float(info.get('qty_per_pack') or 1.0)

        final_qty = raw_qty
        if is_pack and qty_per_pack > 1.0 and not base_sku.endswith('-UNIT'):
            divided = math.floor(raw_qty / qty_per_pack)
            final_qty = divided if divided > 0 else int(raw_qty)
        final_qty = int(final_qty)
        if final_qty <= 0:
            continue

        base_price = float(info.get('list_price') or 0.0)
        clr_price = round(base_price * price_factor, 2)
        clr_sku = f"{base_sku}{suffix}"

        normal_qty = normal_stock.get(pid, 0.0) if manage_base else None
        try:
            _upsert_mirror(shop_url, pid, base_sku, clr_sku, info,
                           clr_price, base_price, final_qty, pid_expiry.get(pid),
                           shopify_location_id, collection_id, normal_qty=normal_qty)
            active_base_skus.add(base_sku)
            processed += 1
        except Exception as e:
            db.session.rollback()
            log_event('Clearance', 'Warning',
                      f"Mirror upsert failed for {base_sku}: {e}", shop_url=shop_url)

    drafted = _zero_out_stale(shop_url, active_base_skus, shopify_location_id)

    log_event('Clearance', 'Success',
              f"Clearance sync complete. {processed} mirror(s) live, {drafted} drafted (stock gone).",
              shop_url=shop_url)
