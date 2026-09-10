"""
Storefront self-heal.

Every fault this repairs shares one property: the connector reports success,
Odoo looks correct, the Shopify admin looks correct, and the product is still
unbuyable on the storefront. Nothing in the normal sync path notices, because
each system is individually consistent. This pass is the only thing that looks
at the end state a customer actually sees.

It repairs what it can prove is wrong, and raises what only a human can decide.

REPAIRS (applied automatically)

  1. Lost Online Store publication.
     Setting a Shopify product to 'draft' removes its Online Store publication.
     Setting status back to 'active' does NOT restore it. The clearance pass
     drafts a base product when only clearance stock remains and re-activates
     it when normal stock returns, so every product that completed that round
     trip ended ACTIVE in the admin and absent from the storefront. Any ACTIVE
     product with stock and no Online Store publication is re-published.

  2. Base products still flagged as drafted-by-us that have normal stock again.
     Belt and braces for the clearance lifecycle: if a mirror row still carries
     base_drafted while the base has sellable stock, restore the base.

ALERTS (reported, never auto-applied)

  3. Stock in internal locations the sync does not count.
     Odoo's "On Hand" covers every internal location; the storefront only ever
     shows `inventory_locations` minus `inventory_locations_exclude`. Stock in
     any other internal location is invisible to customers while reading as in
     stock everywhere staff look. Whether such a location SHOULD be sellable is
     a warehouse decision (a receiving bay usually yes, a damaged-goods bin
     never), so this is surfaced with real numbers rather than auto-enabled.

  4. Clearance stock that no listing can sell.
     A lot with no best-before date is refused by the clearance pass, and the
     clearance location is excluded from the main sync, so undated clearance
     stock is counted by nobody. Selling it automatically would put undated
     food on the site, so this is surfaced for Worthy to date or write off.

SETTLING
     Each run records how many repairs it made. Consecutive runs must reach
     zero; a count that never falls means the repair is not sticking, and the
     pass says so explicitly instead of quietly repeating itself for ever.
"""

import json
from datetime import datetime

import shopify

from models import db, ClearanceMirror
from utils import (
    get_odoo_connection, log_event, setup_shopify_session,
    get_config, get_shop_company_id,
)


def _int_list(val):
    out = []
    for v in (val or []):
        try:
            out.append(int(v))
        except (TypeError, ValueError):
            continue
    return out


def _active_unpublished_products():
    """ACTIVE products carrying no Online Store URL, i.e. present in the admin
    and unreachable on the storefront. Returns [{id, title, sku, inventory}]."""
    out, cursor, pages = [], None, 0
    client = shopify.GraphQL()
    while pages < 40:
        pages += 1
        after = f', after: "{cursor}"' if cursor else ''
        q = """
        { products(first: 250, query: "status:active"%s) {
            pageInfo { hasNextPage endCursor }
            edges { node {
              legacyResourceId title onlineStoreUrl totalInventory
              variants(first: 1) { edges { node { sku } } }
            } } } }
        """ % after
        data = json.loads(client.execute(q)).get('data', {}).get('products', {})
        for e in data.get('edges', []):
            n = e['node']
            if n.get('onlineStoreUrl'):
                continue
            vs = n.get('variants', {}).get('edges', [])
            out.append({
                'id': n['legacyResourceId'],
                'title': n['title'],
                'sku': (vs[0]['node'].get('sku') if vs else None),
                'inventory': n.get('totalInventory') or 0,
            })
        info = data.get('pageInfo', {})
        if not info.get('hasNextPage'):
            break
        cursor = info.get('endCursor')
    return out


def _republish(product_id):
    prod = shopify.Product.find(int(product_id))
    if not prod:
        return False
    prod.published_at = datetime.utcnow().isoformat()
    prod.published_scope = 'web'
    return bool(prod.save())


def _uncounted_stock_locations(odoo, shop_url, company_id):
    """Internal locations holding stock that the storefront never counts.

    Returns (rows, counted_ids) where rows is
    [{location, units, products}] sorted by units desc.
    """
    target_locs = _int_list(get_config('inventory_locations', [], shop_url=shop_url))
    exclude_locs = _int_list(get_config('inventory_locations_exclude', [], shop_url=shop_url))
    if not target_locs:
        return [], set()

    # Every internal location for this company.
    loc_domain = [['usage', '=', 'internal']]
    if company_id:
        loc_domain += ['|', ['company_id', '=', company_id], ['company_id', '=', False]]
    all_locs = odoo.models.execute_kw(
        odoo.db, odoo.uid, odoo.password, 'stock.location', 'search_read',
        [loc_domain], {'fields': ['id', 'complete_name']})
    names = {l['id']: l.get('complete_name') or str(l['id']) for l in all_locs}

    # The ones the sync counts (child_of the targets, minus explicit excludes).
    counted_domain = [['id', 'child_of', target_locs], ['usage', '=', 'internal']]
    counted = odoo.models.execute_kw(
        odoo.db, odoo.uid, odoo.password, 'stock.location', 'search',
        [counted_domain])
    counted_ids = set(counted) - set(exclude_locs)

    # Locations the operator has DELIBERATELY carved out are not faults:
    # inventory_locations_exclude is an explicit "do not sell from here", and
    # clearance_locations are handled by the clearance pass instead. Alerting
    # on those would fire every hour for ever, and an alert that never settles
    # is an alert nobody reads. Only locations nobody has ruled on are faults.
    clearance_locs = _int_list(get_config('clearance_locations', [], shop_url=shop_url))
    deliberate = set(exclude_locs) | set(clearance_locs)
    if deliberate:
        try:
            deliberate |= set(odoo.models.execute_kw(
                odoo.db, odoo.uid, odoo.password, 'stock.location', 'search',
                [[['id', 'child_of', list(deliberate)]]]))
        except Exception:
            pass

    uncounted_ids = [i for i in names if i not in counted_ids and i not in deliberate]
    if not uncounted_ids:
        return [], counted_ids

    groups = odoo.models.execute_kw(
        odoo.db, odoo.uid, odoo.password, 'stock.quant', 'read_group',
        [[['location_id', 'in', uncounted_ids], ['quantity', '>', 0]],
         ['location_id', 'quantity'], ['location_id']])

    rows = []
    for g in groups:
        loc = g.get('location_id')
        if not loc:
            continue
        rows.append({
            'location_id': loc[0],
            'location': names.get(loc[0], loc[1]),
            'units': float(g.get('quantity') or 0.0),
            'products': int(g.get('location_id_count') or g.get('__count') or 0),
        })
    rows.sort(key=lambda r: -r['units'])
    return rows, counted_ids


def perform_self_heal(shop_url, apply_changes=True):
    """Run every check. Returns a dict summary; safe to call from a job or a
    dashboard button. apply_changes=False makes it a pure report."""
    report = {
        'shop_url': shop_url,
        'ran_at': datetime.utcnow().isoformat(),
        'republished': [],
        'republish_failed': [],
        'bases_restored': [],
        'uncounted_locations': [],
        'unsellable_clearance': [],
        'repairs_made': 0,
    }

    if not setup_shopify_session(shop_url):
        log_event('Self-Heal', 'Error', 'Could not start Shopify session.', shop_url=shop_url)
        return report

    # --- 1. Lost Online Store publication -------------------------------
    try:
        unpublished = _active_unpublished_products()
        targets = [u for u in unpublished if (u['inventory'] or 0) > 0]
        for t in targets:
            if not apply_changes:
                report['republished'].append(t['sku'] or t['id'])
                continue
            try:
                if _republish(t['id']):
                    report['republished'].append(t['sku'] or t['id'])
                else:
                    report['republish_failed'].append(t['sku'] or t['id'])
            except Exception as e:
                report['republish_failed'].append(f"{t['sku']}: {e}")
        if targets:
            log_event('Self-Heal', 'Warning',
                      f"{len(targets)} product(s) were ACTIVE with stock but not published to the "
                      f"Online Store — invisible to customers. "
                      f"{'Re-published' if apply_changes else 'Would re-publish'} "
                      f"{len(report['republished'])}.",
                      shop_url=shop_url)
    except Exception as e:
        log_event('Self-Heal', 'Warning', f"Publication check failed: {e}", shop_url=shop_url)

    # --- 2. Bases still flagged drafted-by-us ---------------------------
    try:
        from services.clearance import _reactivate_base_if_ours
        stuck = ClearanceMirror.query.filter_by(shop_url=shop_url, base_drafted=True).all()
        for row in stuck:
            if not row.is_active and apply_changes:
                try:
                    _reactivate_base_if_ours(shop_url, row)
                    db.session.commit()
                    if not row.base_drafted:
                        report['bases_restored'].append(row.base_sku)
                except Exception:
                    db.session.rollback()
    except Exception as e:
        log_event('Self-Heal', 'Warning', f"Base restore check failed: {e}", shop_url=shop_url)

    # --- 3 & 4. Odoo-side alerts ----------------------------------------
    odoo = get_odoo_connection(shop_url)
    if odoo:
        company_id = get_shop_company_id(shop_url)
        try:
            company_id = int(company_id) if company_id else None
        except (TypeError, ValueError):
            company_id = None

        try:
            rows, _ = _uncounted_stock_locations(odoo, shop_url, company_id)
            report['uncounted_locations'] = rows
            if rows:
                total = sum(r['units'] for r in rows)
                detail = "; ".join(f"{r['location']}: {r['units']:.0f}" for r in rows[:6])
                log_event('Self-Heal', 'Warning',
                          f"{total:.0f} unit(s) of stock sit in internal locations the website "
                          f"never counts, so those products read as in stock in Odoo and sold out "
                          f"on the site. {detail}. "
                          f"Add a location under Settings > Inventory Locations if it should sell.",
                          shop_url=shop_url)
        except Exception as e:
            log_event('Self-Heal', 'Warning', f"Location check failed: {e}", shop_url=shop_url)

        # Clearance stock no listing can sell.
        try:
            clearance_locs = _int_list(get_config('clearance_locations', [], shop_url=shop_url))
            if clearance_locs:
                quants = odoo.models.execute_kw(
                    odoo.db, odoo.uid, odoo.password, 'stock.quant', 'search_read',
                    [[['location_id', 'child_of', clearance_locs], ['quantity', '>', 0]]],
                    {'fields': ['product_id', 'quantity', 'lot_id']})
                lot_ids = list({q['lot_id'][0] for q in quants if q.get('lot_id')})
                dated = set()
                for i in range(0, len(lot_ids), 200):
                    chunk = lot_ids[i:i + 200]
                    lots = odoo.models.execute_kw(
                        odoo.db, odoo.uid, odoo.password, 'stock.lot', 'read', [chunk],
                        {'fields': ['use_date', 'expiration_date', 'removal_date']})
                    for l in lots:
                        if l.get('use_date') or l.get('expiration_date') or l.get('removal_date'):
                            dated.add(l['id'])
                undated_units = 0.0
                undated_pids = set()
                for q in quants:
                    lot = q.get('lot_id')
                    if not lot or lot[0] not in dated:
                        undated_units += float(q.get('quantity') or 0.0)
                        if q.get('product_id'):
                            undated_pids.add(q['product_id'][0])
                if undated_pids:
                    report['unsellable_clearance'] = {
                        'products': len(undated_pids), 'units': undated_units}
                    log_event('Self-Heal', 'Warning',
                              f"{undated_units:.0f} unit(s) across {len(undated_pids)} product(s) sit "
                              f"in Clearance with no best-before date. The clearance listing refuses "
                              f"undated stock and the normal listing excludes the clearance location, "
                              f"so nothing can sell them. Add a best-before date in Odoo or write "
                              f"the stock off.", shop_url=shop_url)
        except Exception as e:
            log_event('Self-Heal', 'Warning', f"Clearance check failed: {e}", shop_url=shop_url)

    report['repairs_made'] = len(report['republished']) + len(report['bases_restored'])

    # --- Settling ---------------------------------------------------------
    # A self-healing pass that repairs the same count every run is not healing,
    # it is looping. Say so rather than repeating quietly.
    try:
        prev_raw = get_config('self_heal_last_repairs', None, shop_url=shop_url)
        prev = int(prev_raw) if prev_raw not in (None, '') else None
        streak_raw = get_config('self_heal_nonzero_streak', 0, shop_url=shop_url)
        streak = int(streak_raw or 0)
        if report['repairs_made'] == 0:
            streak = 0
            log_event('Self-Heal', 'Success',
                      'Self-heal clean — every active product with stock is published and '
                      'reachable on the storefront.', shop_url=shop_url)
        else:
            streak += 1
            if prev is not None and streak >= 3 and report['repairs_made'] >= prev:
                log_event('Self-Heal', 'Error',
                          f"Self-heal has repaired {report['repairs_made']} product(s) on "
                          f"{streak} consecutive runs without the number falling. The repair is "
                          f"not sticking — something is re-hiding these products after each pass. "
                          f"Investigate before trusting the storefront.", shop_url=shop_url)
        from utils import set_config
        set_config('self_heal_last_repairs', report['repairs_made'], shop_url=shop_url)
        set_config('self_heal_nonzero_streak', streak, shop_url=shop_url)
        set_config('self_heal_last_run', report['ran_at'], shop_url=shop_url)
    except Exception as e:
        log_event('Self-Heal', 'Warning', f"Settle tracking failed: {e}", shop_url=shop_url)

    return report
