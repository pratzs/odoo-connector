"""
Hide from storefront: sold-out products, and new products not yet received.

Two per-shop switches on the Settings page, both OFF by default:

  hide_sold_out_enabled      A product with nothing to sell is taken off the
                             Online Store, and put back the moment it can be
                             sold again.
  hide_not_received_enabled  A product that has never had stock received (no
                             done receipt, drop-ship receipt or stock count) and
                             has nothing to sell stays off the Online Store until
                             its first stock arrives.

HOW IT HIDES
  Online Store publication only: REST `published: false` to hide, and
  `published_at` + `published_scope: global` to show again (the same path the
  clearance pass and self-heal use). The product's status, data, SEO and
  collections are untouched. GraphQL publishableUnpublish would need the
  write_publications scope, which this app does not hold.

  Status (active / draft / archived) stays owned by exclusion rules, the
  clearance pass and auto-publish; this rule owns publication only, and only
  ever for ACTIVE products. So it cannot flip-flop against exclusion restore
  (which changes status) or clearance (which drafts and re-publishes bases).

WHAT IT TOUCHES
  Hide: only ACTIVE products that are currently published, not clearance
  mirrors. Show: only products on this rule's own list (AppSetting
  `stock_hidden_products`). A product staff unpublished by hand is never on the
  list, so it is never published by this rule.

  If staff re-publish a product this rule hid while it is still unsellable, the
  rule leaves it visible until it next becomes sellable (marked
  `staff_override`), instead of hiding it again every run.

SELLABLE
  A variant is sellable if Shopify does not track its stock, or it is set to
  continue selling when out of stock, or its quantity is above 0. A product is
  sellable if any variant is. So "Continue selling when out of stock" in Shopify
  is the way to keep one sold-out product visible.

SAFETY
  At most MAX_HIDE_PER_RUN hides per run (the rest follow next run). A run that
  would leave more than SANITY_SHARE of active products unsellable is refused
  outright: that looks like broken stock data, not a real sell-out.
"""

import json
from datetime import datetime

import shopify

from models import ProductMap
from utils import get_config, set_config, log_event, get_shop_company_id, get_odoo_connection, setup_shopify_session

CFG_SOLD_OUT = 'hide_sold_out_enabled'
CFG_NOT_RECEIVED = 'hide_not_received_enabled'
HIDDEN_KEY = 'stock_hidden_products'
LAST_RUN_KEY = 'stock_hide_last_run'
PREVIEW_KEY = 'stock_hide_last_preview'

MAX_HIDE_PER_RUN = 100
SANITY_SHARE = 0.8
SANITY_MIN_PRODUCTS = 20

SOLD_OUT = 'sold_out'
NOT_RECEIVED = 'not_received'
REASON_LABELS = {
    SOLD_OUT: 'Sold out',
    NOT_RECEIVED: 'Not received yet',
}

_PRODUCT_FIELDS = """
  legacyResourceId title status publishedAt
  variants(first: 5) { edges { node {
    sku inventoryQuantity inventoryPolicy inventoryItem { tracked }
  } } }
"""


# =====================================================
# Settings and the hidden list
# =====================================================

def _truthy(v):
    return v is True or str(v).lower() in ('true', '1', 'on', 'yes')


def get_settings(shop_url):
    return {
        'sold_out': _truthy(get_config(CFG_SOLD_OUT, False, shop_url=shop_url)),
        'not_received': _truthy(get_config(CFG_NOT_RECEIVED, False, shop_url=shop_url)),
    }


def any_enabled(shop_url):
    s = get_settings(shop_url)
    return s['sold_out'] or s['not_received']


def get_hidden(shop_url):
    """{shopify_product_id(str): {sku, title, reason, since, staff_override?}}"""
    val = get_config(HIDDEN_KEY, {}, shop_url=shop_url)
    return val if isinstance(val, dict) else {}


def hidden_product_ids(shop_url):
    """Product ids this rule is currently keeping off the storefront. Other
    passes (self-heal, publication repair, product sync) must not re-publish
    these."""
    try:
        return {pid for pid, e in get_hidden(shop_url).items() if not e.get('staff_override')}
    except Exception:
        return set()


def _save_hidden(shop_url, hidden):
    set_config(HIDDEN_KEY, hidden, shop_url=shop_url)


# =====================================================
# Pure decision logic (unit tested)
# =====================================================

def variant_sellable(v):
    if v.get('tracked') is False:
        return True
    if str(v.get('inventoryPolicy') or '').upper() == 'CONTINUE':
        return True
    return int(v.get('inventoryQuantity') or 0) > 0


def product_sellable(variants):
    return any(variant_sellable(v) for v in variants)


def normalise_product(node):
    """GraphQL product node -> flat dict used by plan_changes()."""
    variants = []
    for e in (node.get('variants') or {}).get('edges', []):
        n = e.get('node') or {}
        variants.append({
            'sku': (n.get('sku') or '').strip(),
            'inventoryQuantity': n.get('inventoryQuantity'),
            'inventoryPolicy': n.get('inventoryPolicy'),
            'tracked': (n.get('inventoryItem') or {}).get('tracked'),
        })
    return {
        'id': str(node.get('legacyResourceId')),
        'title': node.get('title') or '',
        'status': str(node.get('status') or '').upper(),
        'published': bool(node.get('publishedAt')),
        'skus': [v['sku'] for v in variants if v['sku']],
        'sellable': product_sellable(variants) if variants else True,
    }


def decide_reason(sellable, never_received, settings):
    """Why this product should be hidden right now, or None."""
    if sellable:
        return None
    if settings.get('not_received') and never_received:
        return NOT_RECEIVED
    if settings.get('sold_out'):
        return SOLD_OUT
    return None


def is_clearance(product, clearance_suffix):
    return bool(clearance_suffix) and any(s.endswith(clearance_suffix) for s in product['skus'])


def plan_changes(products, hidden, never_received, settings, clearance_suffix='-CLR',
                 cap=MAX_HIDE_PER_RUN, missing_ids=()):
    """Work out what to hide and show. Writes nothing.

    products        normalised products (ACTIVE listing, plus any hidden entries looked up by id)
    hidden          the current hidden list
    never_received  set of product ids that have never had stock come in
    missing_ids     hidden ids Shopify no longer has (deleted) -> dropped
    Returns {'hide': [...], 'show': [...], 'update': {pid: entry}, 'drop': [...], 'capped': n,
             'refused': str|None, 'counts': {...}}
    """
    hide, show, update, drop = [], [], {}, list(missing_ids)
    active = [p for p in products if p['status'] == 'ACTIVE' and not is_clearance(p, clearance_suffix)]

    for p in active:
        pid = p['id']
        reason = decide_reason(p['sellable'], pid in never_received, settings)
        entry = hidden.get(pid)
        if entry is not None:
            if entry.get('staff_override'):
                # Staff showed it while it was unsellable: leave it until it sells again.
                if p['sellable'] or reason is None:
                    drop.append(pid)
                continue
            if p['published']:
                # Someone else published it (staff, clearance restore, ...).
                if reason:
                    update[pid] = dict(entry, staff_override=True)
                else:
                    drop.append(pid)
                continue
            if reason is None:
                show.append(p)
            elif reason != entry.get('reason'):
                update[pid] = dict(entry, reason=reason)
            continue
        if reason and p['published']:
            hide.append(dict(p, reason=reason))

    # Sanity: if nearly everything active reads as unsellable, the stock data is broken.
    unsellable = sum(1 for p in active if not p['sellable'])
    refused = None
    if len(active) >= SANITY_MIN_PRODUCTS and unsellable > SANITY_SHARE * len(active) and hide:
        refused = (f"Stopped: {unsellable} of {len(active)} products show as out of stock, which looks like "
                   f"a stock data problem, not a real sell-out. Nothing was hidden.")
        hide = []

    hide.sort(key=lambda p: (p['reason'] != NOT_RECEIVED, p['title'].lower()))
    capped = max(0, len(hide) - cap)
    hide = hide[:cap]

    counts = {
        'active_checked': len(active),
        'would_hide_sold_out': sum(1 for p in hide if p['reason'] == SOLD_OUT),
        'would_hide_not_received': sum(1 for p in hide if p['reason'] == NOT_RECEIVED),
        'would_show': len(show),
        'capped': capped,
    }
    return {'hide': hide, 'show': show, 'update': update, 'drop': drop,
            'capped': capped, 'refused': refused, 'counts': counts}


# =====================================================
# Shopify reads / writes
# =====================================================

def _gql(query, variables=None):
    res = json.loads(shopify.GraphQL().execute(query, variables=variables or {}))
    if res.get('errors'):
        raise RuntimeError(f"Shopify GraphQL error: {res['errors']}")
    return res.get('data') or {}


def fetch_active_products():
    out, cursor = [], None
    q = ("query StockVisProducts($after: String) { products(first: 50, after: $after, "
         "query: \"status:active\") { pageInfo { hasNextPage endCursor } edges { node { %s } } } }"
         % _PRODUCT_FIELDS)
    for _ in range(400):
        data = _gql(q, {'after': cursor}).get('products') or {}
        out += [normalise_product(e['node']) for e in data.get('edges', [])]
        info = data.get('pageInfo') or {}
        if not info.get('hasNextPage'):
            break
        cursor = info.get('endCursor')
    return out


def fetch_products_by_id(ids):
    """Returns (found products, ids Shopify no longer has)."""
    found, missing = [], []
    q = "query StockVisNodes($ids: [ID!]!) { nodes(ids: $ids) { ... on Product { %s } } }" % _PRODUCT_FIELDS
    ids = list(ids)
    for i in range(0, len(ids), 50):
        chunk = ids[i:i + 50]
        nodes = _gql(q, {'ids': [f"gid://shopify/Product/{pid}" for pid in chunk]}).get('nodes') or []
        for pid, node in zip(chunk, nodes):
            if node and node.get('legacyResourceId'):
                found.append(normalise_product(node))
            else:
                missing.append(str(pid))
    return found, missing


def fetch_product_by_inventory_item(inventory_item_id):
    q = ("query StockVisItem($id: ID!) { inventoryItem(id: $id) { variant { product { %s } } } }"
         % _PRODUCT_FIELDS)
    data = _gql(q, {'id': f"gid://shopify/InventoryItem/{inventory_item_id}"})
    prod = (((data.get('inventoryItem') or {}).get('variant') or {}).get('product'))
    return normalise_product(prod) if prod else None


def unpublish_product(product_id):
    prod = shopify.Product({'id': int(product_id), 'published': False})
    if not prod.save():
        raise RuntimeError(f"Shopify refused: {prod.errors.full_messages()}")


def publish_product(product_id):
    prod = shopify.Product({'id': int(product_id),
                            'published_at': datetime.utcnow().isoformat(),
                            'published_scope': 'global'})
    if not prod.save():
        raise RuntimeError(f"Shopify refused: {prod.errors.full_messages()}")


# =====================================================
# Odoo: "never had stock come in"
# =====================================================

def odoo_ids_for_products(shop_url, products):
    """{shopify_product_id: [odoo product.product ids]} via ProductMap (SKU)."""
    skus = {s for p in products for s in p['skus']}
    if not skus:
        return {}
    rows = ProductMap.query.filter(ProductMap.shop_url == shop_url, ProductMap.sku.in_(list(skus)),
                                   ProductMap.odoo_product_id > 0).all()
    by_sku = {r.sku: r.odoo_product_id for r in rows}
    out = {}
    for p in products:
        ids = sorted({by_sku[s] for s in p['skus'] if s in by_sku})
        if ids:
            out[p['id']] = ids
    return out


def received_odoo_ids(odoo, company_id, odoo_ids):
    """Odoo product ids that have EVER had stock received: a done stock move
    line from outside the warehouse into an internal location (purchase
    receipt, stock count, return, manufacture), or a done line out of a vendor
    location (a drop-ship receipt that never touched the warehouse). Scoped to
    the shop's Odoo company.

    Move LINES, not moves: receipts here are booked to the warehouse view
    location (e.g. WPW01, usage 'view') and only the lines carry the real
    internal bin, so a move-level check misses every normal receipt."""
    got = set()
    odoo_ids = list(odoo_ids)
    for i in range(0, len(odoo_ids), 500):
        chunk = odoo_ids[i:i + 500]
        domain = [['product_id', 'in', chunk], ['state', '=', 'done'],
                  ['location_id.usage', 'not in', ['internal', 'transit']],
                  '|', ['location_dest_id.usage', '=', 'internal'], ['location_id.usage', '=', 'supplier'],
                  ['company_id', '=', int(company_id)]]
        groups = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'stock.move.line', 'read_group',
                                        [domain, ['product_id'], ['product_id']], {'lazy': False})
        for g in groups:
            if g.get('product_id'):
                got.add(g['product_id'][0])
    return got


def never_received_products(shop_url, odoo, products):
    """Shopify product ids (among the given unsellable products) whose mapped Odoo
    products have never had stock come in. Unmapped products are NOT counted
    (we cannot prove they were never received)."""
    company_id = get_shop_company_id(shop_url)
    if not company_id or not odoo or not products:
        return set()
    mapping = odoo_ids_for_products(shop_url, products)
    all_ids = {i for ids in mapping.values() for i in ids}
    if not all_ids:
        return set()
    received = received_odoo_ids(odoo, company_id, all_ids)
    return {pid for pid, ids in mapping.items() if not any(i in received for i in ids)}


# =====================================================
# Runs
# =====================================================

def _clearance_suffix(shop_url):
    return get_config('clearance_sku_suffix', '-CLR', shop_url=shop_url) or '-CLR'


def _gather(shop_url, settings, odoo=None):
    """Everything plan_changes() needs, read from Shopify (and Odoo when rule 2 is on)."""
    hidden = get_hidden(shop_url)
    products = fetch_active_products()
    seen = {p['id'] for p in products}
    extra, missing = fetch_products_by_id([pid for pid in hidden if pid not in seen])
    products += extra
    never = set()
    if settings.get('not_received'):
        unsellable = [p for p in products if not p['sellable'] and p['status'] == 'ACTIVE']
        odoo = odoo or get_odoo_connection(shop_url)
        never = never_received_products(shop_url, odoo, unsellable)
    return hidden, products, never, missing


def _apply(shop_url, plan, hidden):
    now = datetime.utcnow().strftime('%Y-%m-%d %H:%M')
    hidden_n, shown_n, failed = 0, 0, []
    for pid in plan['drop']:
        hidden.pop(pid, None)
    for pid, entry in plan['update'].items():
        hidden[pid] = entry
    for p in plan['show']:
        try:
            publish_product(p['id'])
            hidden.pop(p['id'], None)
            shown_n += 1
            log_event('Storefront', 'Info',
                      f"Brought back {p['skus'][0] if p['skus'] else p['id']}: it can be sold again.",
                      shop_url=shop_url)
        except Exception as e:
            failed.append(f"show {p['id']}: {e}")
    for p in plan['hide']:
        try:
            unpublish_product(p['id'])
            hidden[p['id']] = {'sku': p['skus'][0] if p['skus'] else '', 'title': p['title'],
                               'reason': p['reason'], 'since': now}
            hidden_n += 1
            log_event('Storefront', 'Info',
                      f"Hid {p['skus'][0] if p['skus'] else p['id']}: {REASON_LABELS[p['reason']].lower()}.", shop_url=shop_url)
        except Exception as e:
            failed.append(f"hide {p['id']}: {e}")
    _save_hidden(shop_url, hidden)
    return hidden_n, shown_n, failed


def run_stock_visibility(shop_url, dry_run=False, source='sync'):
    """Full pass. Caller holds an app context. With dry_run, writes nothing to
    Shopify and stores the preview (uncapped counts and examples)."""
    from utils import conn
    settings = get_settings(shop_url)
    if not dry_run and not (settings['sold_out'] or settings['not_received']) and not get_hidden(shop_url):
        return None
    if dry_run:
        settings = {'sold_out': True, 'not_received': True}  # preview shows both rules
    if not setup_shopify_session(shop_url):
        log_event('Storefront', 'Error', 'Hide-from-storefront: no Shopify session.', shop_url=shop_url)
        return None

    lock = conn.lock(f"stock_vis:{shop_url}", timeout=900, blocking_timeout=120)
    if not lock.acquire(blocking=True):
        log_event('Storefront', 'Warning', 'Hide-from-storefront: another run is still going; skipped.',
                  shop_url=shop_url)
        return None
    try:
        hidden, products, never, missing = _gather(shop_url, settings)
        if dry_run:
            return _store_preview(shop_url, products, hidden, never)

        plan = plan_changes(products, hidden, never, settings, _clearance_suffix(shop_url), missing_ids=missing)
        if plan['refused']:
            log_event('Storefront', 'Error', f"Hide-from-storefront refused: {plan['refused']}", shop_url=shop_url)
        hidden_n, shown_n, failed = _apply(shop_url, plan, hidden)
        if plan['capped']:
            log_event('Storefront', 'Warning',
                      f"Hide from storefront: hit the limit of {MAX_HIDE_PER_RUN} for one run. "
                      f"{plan['capped']} more will be hidden on the next run.", shop_url=shop_url)
        if failed:
            log_event('Storefront', 'Warning',
                      f"Hide-from-storefront: {len(failed)} change(s) failed: {failed[:5]}", shop_url=shop_url)
        summary = {'at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), 'source': source,
                   'hidden': hidden_n, 'shown': shown_n, 'failed': len(failed),
                   'capped': plan['capped'], 'refused': plan['refused'],
                   'now_hidden': len(hidden_product_ids(shop_url))}
        set_config(LAST_RUN_KEY, summary, shop_url=shop_url)
        if hidden_n or shown_n or plan['refused']:
            log_event('Storefront', 'Success' if not failed else 'Warning',
                      f"Hide from storefront ({source}): hid {hidden_n}, brought back {shown_n}. "
                      f"{summary['now_hidden']} hidden right now.", shop_url=shop_url)
        return summary
    finally:
        try:
            lock.release()
        except Exception:
            pass


def _store_preview(shop_url, products, hidden, never):
    sfx = _clearance_suffix(shop_url)
    sold_only = plan_changes(products, hidden, set(), {'sold_out': True}, sfx, cap=10 ** 9)
    recv_only = plan_changes(products, hidden, never, {'not_received': True}, sfx, cap=10 ** 9)

    def ex(items):
        return [{'sku': p['skus'][0] if p['skus'] else '', 'title': p['title']} for p in items[:8]]

    preview = {
        'at': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        'active_checked': sold_only['counts']['active_checked'],
        'sold_out': len(sold_only['hide']),
        'not_received': len(recv_only['hide']),
        'sold_out_examples': ex(sold_only['hide']),
        'not_received_examples': ex(recv_only['hide']),
        'refused': sold_only['refused'],
        'cap': MAX_HIDE_PER_RUN,
    }
    set_config(PREVIEW_KEY, preview, shop_url=shop_url)
    log_event('Storefront', 'Info',
              f"Hide from storefront check: would hide {preview['sold_out']} sold out and "
              f"{preview['not_received']} not received yet (of {preview['active_checked']} products).",
              shop_url=shop_url)
    return preview


def check_one_inventory_item(shop_url, inventory_item_id):
    """Inventory webhook path: re-check the one product this inventory item belongs to."""
    from app import app
    from utils import conn
    with app.app_context():
        settings = get_settings(shop_url)
        if not (settings['sold_out'] or settings['not_received']):
            return
        if not setup_shopify_session(shop_url):
            return
        lock = conn.lock(f"stock_vis:{shop_url}", timeout=120, blocking_timeout=30)
        if not lock.acquire(blocking=True):
            return  # a full pass is running; it will cover this product
        try:
            p = fetch_product_by_inventory_item(inventory_item_id)
            if not p:
                return
            hidden = get_hidden(shop_url)
            never = set()
            if settings['not_received'] and not p['sellable']:
                never = never_received_products(shop_url, get_odoo_connection(shop_url), [p])
            plan = plan_changes([p], hidden, never, settings, _clearance_suffix(shop_url), cap=1)
            plan['refused'] = None  # one product: the share check does not apply
            if plan['hide'] or plan['show'] or plan['update'] or plan['drop']:
                _apply(shop_url, plan, hidden)
        finally:
            try:
                lock.release()
            except Exception:
                pass


def hold_new_product(shop_url, odoo, shopify_product_id, odoo_product_id, sku, title):
    """Called right after the product sync CREATES a published product: if rule 2
    is on and this Odoo product has never had stock come in, take it straight
    off the storefront so it never shows before its first receipt."""
    from utils import conn
    if not get_settings(shop_url)['not_received']:
        return False
    company_id = get_shop_company_id(shop_url)
    if not company_id or not odoo:
        return False
    if received_odoo_ids(odoo, company_id, [odoo_product_id]):
        return False
    lock = conn.lock(f"stock_vis:{shop_url}", timeout=120, blocking_timeout=60)
    if not lock.acquire(blocking=True):
        return False  # the next inventory-sync pass will hide it
    try:
        unpublish_product(shopify_product_id)
        hidden = get_hidden(shop_url)
        hidden[str(shopify_product_id)] = {'sku': sku, 'title': title, 'reason': NOT_RECEIVED,
                                           'since': datetime.utcnow().strftime('%Y-%m-%d %H:%M')}
        _save_hidden(shop_url, hidden)
        log_event('Storefront', 'Info',
                  f"Hid new product {sku}: not received yet.", shop_url=shop_url)
        return True
    finally:
        try:
            lock.release()
        except Exception:
            pass
