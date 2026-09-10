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

  4. Clearance stock with no best-before date (INFORMATIONAL ONLY).
     Confirmed as the intended rule: clearance only ever lists stock that
     carries a best-before date in Odoo, so a lot with no BBD is refused by the
     clearance pass, and the clearance location is excluded from the main sync.
     Undated clearance stock is therefore MEANT to be absent from the site.
     This is never repaired and never warned about; the figure is recorded and
     mentioned only when it changes, so adding a BBD in Odoo remains the one
     way to list any of it.

SETTLING
     Each run records how many repairs it made. Consecutive runs must reach
     zero; a count that never falls means the repair is not sticking, and the
     pass says so explicitly instead of quietly repeating itself for ever.
"""

import json
import smtplib
from datetime import datetime
from email.message import EmailMessage

import shopify

from models import db, ClearanceMirror
from utils import (
    get_odoo_connection, log_event, setup_shopify_session,
    get_config, get_shop_company_id,
)


def _alert_email(shop_url, subject, body):
    """Email an un-repairable finding to the shop's configured alert address.

    Only the checks a human must decide on are ever emailed — anything the
    pass repairs itself stays in the log. Sends at most once a day per
    distinct finding, because an alert that repeats every hour trains people
    to ignore it, which is how the original faults survived for months.

    No-ops silently when no alert_email is configured. The connector had no
    alert address set at all when these faults were found, so every warning it
    had been raising went to a log nobody opened.
    """
    to = (get_config('alert_email', '', shop_url=shop_url) or '').strip()
    if not to:
        return False
    try:
        from security_utils import decrypt_val
        host = get_config('smtp_host', '', shop_url=shop_url)
        port = int(get_config('smtp_port', 587, shop_url=shop_url) or 587)
        user = get_config('smtp_user', '', shop_url=shop_url)
        raw_pass = get_config('smtp_pass', '', shop_url=shop_url)
        pw = decrypt_val(raw_pass) if raw_pass else ''
        if not (host and user and pw):
            return False

        msg = EmailMessage()
        msg['Subject'] = subject
        msg['From'] = f"Worthy Storefront Monitor <{user}>"
        msg['To'] = to
        msg.set_content(body)

        if port == 465:
            with smtplib.SMTP_SSL(host, port) as smtp:
                smtp.login(user, pw)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(host, port) as smtp:
                smtp.ehlo()
                smtp.starttls()
                smtp.login(user, pw)
                smtp.send_message(msg)
        return True
    except Exception as e:
        log_event('Self-Heal', 'Warning', f"Alert email failed: {e}", shop_url=shop_url)
        return False


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


def reconcile_stock(shop_url):
    """Compare EVERY active Shopify variant's quantity against what the sync
    would push, without writing anything.

    The inventory sync only ever reports what it CHANGED, so a variant that has
    silently drifted and is not currently being corrected never appears in any
    log. Checking sold-out products alone cannot find a product listed at 5
    when the warehouse holds 500, or at 500 when it holds none — the second
    oversells. This walks the whole catalogue and returns both numbers for
    every SKU that disagrees.

    Mirrors perform_inventory_sync's arithmetic exactly (same locations, same
    excludes, same pack division), so a disagreement here is real drift and not
    a difference of method.
    """
    out = {'checked': 0, 'drifted': [], 'unmapped': [], 'error': None}
    if not setup_shopify_session(shop_url):
        out['error'] = 'no shopify session'
        return out
    odoo = get_odoo_connection(shop_url)
    if not odoo:
        out['error'] = 'no odoo connection'
        return out

    from models import ProductMap
    import math

    target_locs = _int_list(get_config('inventory_locations', [], shop_url=shop_url))
    exclude_locs = _int_list(get_config('inventory_locations_exclude', [], shop_url=shop_url))
    suffix = get_config('clearance_sku_suffix', '-CLR', shop_url=shop_url) or '-CLR'

    # 1. Every active Shopify variant and the quantity it currently shows.
    variants = {}
    page = shopify.Product.find(limit=250, status='active')
    while page:
        for p in page:
            for v in p.variants:
                if v.sku and not v.sku.endswith(suffix):
                    variants[v.sku] = int(v.inventory_quantity or 0)
            p.__dict__.clear()
        if page.has_next_page():
            page = page.next_page()
        else:
            break
    out['checked'] = len(variants)

    # 2. Odoo ids for those SKUs.
    skus = list(variants.keys())
    sku_to_pid = {}
    for i in range(0, len(skus), 500):
        chunk = skus[i:i + 500]
        for m in ProductMap.query.filter(ProductMap.shop_url == shop_url,
                                         ProductMap.sku.in_(chunk)).all():
            if m.odoo_product_id and m.odoo_product_id > 0:
                sku_to_pid[m.sku] = m.odoo_product_id
    out['unmapped'] = [s for s in skus if s not in sku_to_pid]

    pids = list(set(sku_to_pid.values()))
    if not pids or not target_locs:
        return out

    # 3. Stock the sync would count, same domain the sync uses.
    pid_qty = {pid: 0.0 for pid in pids}
    domain = [['product_id', 'in', pids], ['location_id', 'child_of', target_locs]]
    if exclude_locs:
        domain.append(['location_id', 'not in', exclude_locs])
    for g in odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                                    'stock.quant', 'read_group',
                                    [domain, ['product_id', 'quantity'], ['product_id']]):
        if g.get('product_id'):
            pid_qty[g['product_id'][0]] = float(g.get('quantity') or 0.0)

    # 4. Pack division, same rule and same zero-guard as the sync.
    packs = {}
    with_stock = [pid for pid, q in pid_qty.items() if q > 0]
    for i in range(0, len(with_stock), 500):
        chunk = with_stock[i:i + 500]
        for r in odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                                        'product.product', 'read', [chunk],
                                        {'fields': ['sh_is_secondary_unit', 'qty_per_pack']}):
            packs[r['id']] = r

    for sku, pid in sku_to_pid.items():
        raw = pid_qty.get(pid, 0.0)
        info = packs.get(pid, {})
        if info.get('sh_is_secondary_unit') and float(info.get('qty_per_pack') or 1.0) > 1.0 \
                and not sku.endswith('-UNIT'):
            per = float(info.get('qty_per_pack') or 1.0)
            divided = math.floor(raw / per)
            expected = int(raw) if (divided == 0 and raw > 0) else int(divided)
        else:
            expected = int(raw)
        shown = variants.get(sku, 0)
        if expected != shown:
            out['drifted'].append({'sku': sku, 'expected': expected, 'on_site': shown,
                                   'diff': shown - expected})
    out['drifted'].sort(key=lambda r: -abs(r['diff']))
    return out


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
        'undated_clearance': {},
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
                          f"Add a location under Settings > Inventory Locations if it should sell, "
                          f"or add it to the exclude list if it never should.",
                          shop_url=shop_url)
                # This is the one fault class the pass cannot repair — whether a
                # location should sell is a warehouse decision. It is also the
                # class that silently costs sales, so it is the one that leaves
                # the log and reaches a person.
                from utils import set_config as _sc
                fingerprint = "|".join(f"{r['location_id']}:{r['units']:.0f}" for r in rows)
                today = datetime.utcnow().strftime('%Y-%m-%d')
                seen = get_config('self_heal_loc_alert', '', shop_url=shop_url)
                if str(seen) != f"{today}#{fingerprint}":
                    body = (
                        f"{total:.0f} units of stock are sitting in Odoo locations that "
                        f"{shop_url} never counts, so these products show as IN STOCK in Odoo "
                        f"and SOLD OUT on the website.\n\n"
                        + "\n".join(f"  {r['units']:>10.0f} units   {r['location']}" for r in rows)
                        + "\n\nIf a location should sell, add it in the connector under "
                          "Settings > Inventory Locations.\nIf it never should (damaged goods, "
                          "quarantine), add it to the exclude list and this alert stops.\n"
                    )
                    if _alert_email(shop_url,
                                    f"[{shop_url}] {total:.0f} units invisible to customers",
                                    body):
                        _sc('self_heal_loc_alert', f"{today}#{fingerprint}", shop_url=shop_url)
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
                    report['undated_clearance'] = {
                        'products': len(undated_pids), 'units': undated_units}
                    # This is the CONFIGURED RULE, not a fault: clearance stock
                    # is only ever listed when Odoo carries a best-before date,
                    # so undated stock is meant to stay off the site. Logging it
                    # as a Warning every hour would fire for ever with nothing to
                    # act on. Record the figure, and only say so when it moves.
                    from utils import set_config as _set_config
                    prev = get_config('self_heal_undated_clearance', None, shop_url=shop_url)
                    now_val = f"{len(undated_pids)}:{undated_units:.0f}"
                    if str(prev) != now_val:
                        log_event('Self-Heal', 'Info',
                                  f"Clearance holding {undated_units:.0f} unit(s) across "
                                  f"{len(undated_pids)} product(s) with no best-before date in Odoo. "
                                  f"Correctly not listed — clearance only shows stock that carries a "
                                  f"BBD. Figure shown because it changed; add a BBD in Odoo to list "
                                  f"any of it.", shop_url=shop_url)
                        _set_config('self_heal_undated_clearance', now_val, shop_url=shop_url)
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
