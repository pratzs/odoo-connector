"""
Odoo -> Shopify payment status sync.

Shopify orders that are paid on account (B2B terms, bank transfer) stay
"Payment pending" in Shopify forever, because the money is recorded in Odoo,
not in Shopify. This job closes that gap: when the Odoo sale order behind a
Shopify order is fully invoiced and every one of its customer invoices is paid,
the Shopify order is marked as paid (orderMarkAsPaid).

How it works, cheapest side first:
  1. Ask Shopify for the open orders that are still unpaid
     (financial status pending or partially paid, not cancelled).
  2. Look up their Odoo sale orders by client_order_ref = "ONLINE_<name>",
     scoped to the shop's Odoo company.
  3. Read those orders' customer invoices and decide (odoo_order_is_paid).
  4. Mark the paid ones as paid in Shopify.

Nothing in Odoo is ever written. Invoices are never created or posted here:
the office invoices by hand, and this job only reads the result.

Limits:
  * Shopify only returns orders from the last 60 days without the
    read_all_orders scope, which this app does not hold. Older unpaid orders
    are invisible to the API and are not touched.
  * Orders with a card authorisation (financial status AUTHORIZED) are left
    alone: those are captured, not marked as paid.
"""
import json
import re
from datetime import datetime

import shopify

from utils import (get_odoo_connection, get_shop_company_id, log_event,
                   setup_shopify_session, get_config)

ENTITY = 'Payment Sync'

# Odoo invoice payment states that mean the customer has paid. 'in_payment'
# is a registered payment not yet matched to a bank statement line, which is
# what Odoo shows as "In Payment"; the money has been recorded as received.
PAID_STATES = {'paid', 'in_payment'}

UNPAID_ORDERS_QUERY = """
query UnpaidOrders($cursor: String, $q: String!) {
  orders(first: 100, after: $cursor, query: $q, sortKey: CREATED_AT, reverse: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      name
      createdAt
      cancelledAt
      test
      displayFinancialStatus
      totalOutstandingSet { shopMoney { amount currencyCode } }
    }
  }
}
"""

MARK_PAID_MUTATION = """
mutation MarkPaid($input: OrderMarkAsPaidInput!) {
  orderMarkAsPaid(input: $input) {
    order { id name displayFinancialStatus totalOutstandingSet { shopMoney { amount } } }
    userErrors { field message }
  }
}
"""

# Only these Shopify states are marked as paid. AUTHORIZED is deliberately
# excluded (see module docstring).
MARKABLE_STATUSES = {'PENDING', 'PARTIALLY_PAID'}

# Odoo became Worthy's ledger in April 2025 (first ONLINE_ order 29 Apr 2025).
# Orders from this date are matched against Odoo; orders before it were paid
# in the previous accounting system (mark_historic_orders_paid).
ODOO_ERA_START = '2025-04-01'


def _gql(query, variables):
    raw = shopify.GraphQL().execute(query, variables)
    data = json.loads(raw)
    if data.get('errors'):
        raise RuntimeError(f"Shopify GraphQL error: {data['errors']}")
    return data.get('data') or {}


def fetch_unpaid_shopify_orders(extra_query=None):
    """All open Shopify orders still awaiting payment, newest first.
    extra_query narrows it, e.g. "created_at:<2026-09-19"."""
    q = "(financial_status:pending OR financial_status:partially_paid) AND -status:cancelled"
    if extra_query:
        q += f" AND {extra_query}"
    orders, cursor = [], None
    while True:
        data = _gql(UNPAID_ORDERS_QUERY, {'cursor': cursor, 'q': q})
        page = data.get('orders') or {}
        for node in page.get('nodes') or []:
            if node.get('cancelledAt') or node.get('test'):
                continue
            if node.get('displayFinancialStatus') not in MARKABLE_STATUSES:
                continue
            orders.append(node)
        info = page.get('pageInfo') or {}
        if not info.get('hasNextPage'):
            return orders
        cursor = info.get('endCursor')


def shopify_name_from_ref(ref):
    """Shopify order name ('#2968') from an Odoo client_order_ref, or None.

    Odoo holds Shopify orders under three shapes of reference:
      'ONLINE_#5572'        - created by this connector (from 20 Jan 2026)
      '#2968'               - keyed in by hand, Apr 2025 to Jan 2026
      'ONLINE_#5771 Emma'   - either of the above with a note typed after it
    Only the first word counts, and it must be '#' followed by digits.
    """
    first = (ref or '').strip().split(' ')[0] if ref else ''
    if first.startswith('ONLINE_'):
        first = first[len('ONLINE_'):]
    return first if re.fullmatch(r'#\d+', first) else None


def _dates_close(so_date, shopify_created, days=30):
    try:
        a = datetime.strptime(str(so_date)[:10], '%Y-%m-%d')
        b = datetime.strptime(str(shopify_created)[:10], '%Y-%m-%d')
        return abs((a - b).days) <= days
    except (TypeError, ValueError):
        return False


def match_odoo_sale_orders(odoo, company_id, shopify_orders):
    """Map Shopify order name -> Odoo sale.order for the given orders.

    Exact 'ONLINE_#n' / '#n' references first, then the same with a note
    typed after them. A reference without the ONLINE_ prefix is only trusted
    when the Odoo order is dated within 30 days of the Shopify order, because
    '#2968' could in theory be some other customer's PO number.
    """
    fields = ['name', 'client_order_ref', 'state', 'delivery_status', 'invoice_ids',
              'order_line', 'amount_total', 'date_order']
    base = [['company_id', '=', int(company_id)]] if company_id else []
    created = {o['name']: o.get('createdAt') for o in shopify_orders}
    names = list(created)
    found = []
    for i in range(0, len(names), 500):
        chunk = names[i:i + 500]
        refs = [f"ONLINE_{n}" for n in chunk] + chunk
        found += odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search_read',
                                        [base + [['client_order_ref', 'in', refs]]], {'fields': fields})
    seen = {shopify_name_from_ref(so['client_order_ref']) for so in found}
    missing = [n for n in names if n not in seen]
    for i in range(0, len(missing), 100):
        chunk = missing[i:i + 100]
        pats = [p for n in chunk for p in (f"ONLINE_{n} %", f"{n} %")]
        dom = ['|'] * (len(pats) - 1) + [['client_order_ref', '=like', p] for p in pats]
        found += odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search_read',
                                        [base + dom], {'fields': fields})

    out = {}
    for so in found:
        name = shopify_name_from_ref(so.get('client_order_ref'))
        if name not in created:
            continue
        if not (so.get('client_order_ref') or '').startswith('ONLINE_') and                 not _dates_close(so.get('date_order'), created[name]):
            continue
        prev = out.get(name)
        # If a ref exists twice, prefer the confirmed one.
        if not prev or prev.get('state') not in ('sale', 'done'):
            out[name] = so
    return out


def odoo_order_is_paid(sale_order, invoices, lines):
    """
    Decide whether an Odoo sale order counts as paid.

    sale_order: dict with 'state' and 'delivery_status'.
    invoices:   list of account.move dicts (move_type, state, payment_state,
                amount_total) linked to the order.
    lines:      sale.order.line dicts (qty_to_invoice, display_type).

    Returns (is_paid: bool, reason: str, odoo_net_paid: float).

    Paid means ALL of:
      * the sale order is confirmed (state sale/done), not cancelled;
      * no delivery is still open, so a backorder still to ship (and be
        invoiced) keeps the order unpaid;
      * no line has anything left to invoice. The order-level invoice_status
        is NOT used: on Worthy the orders are locked (state 'done') and Odoo
        then reports invoice_status 'no' even when every line is invoiced;
      * it has at least one posted customer invoice;
      * every posted customer invoice that has not been reversed by a credit
        note is paid or in payment.
    """
    if sale_order.get('state') not in ('sale', 'done'):
        return False, f"sale order state is {sale_order.get('state')}", 0.0
    if sale_order.get('delivery_status') in ('pending', 'partial', 'started'):
        return False, f"delivery is {sale_order.get('delivery_status')}", 0.0
    if any(float(l.get('qty_to_invoice') or 0) > 0 for l in lines if not l.get('display_type')):
        return False, "lines still to invoice", 0.0

    posted = [m for m in invoices if m.get('state') == 'posted']
    out_invoices = [m for m in posted if m.get('move_type') == 'out_invoice'
                    and m.get('payment_state') != 'reversed']
    refunds = [m for m in posted if m.get('move_type') == 'out_refund']
    reversed_total = sum(float(m.get('amount_total') or 0) for m in posted
                         if m.get('move_type') == 'out_invoice'
                         and m.get('payment_state') == 'reversed')

    if not out_invoices:
        return False, "no posted customer invoice", 0.0

    unpaid = [m for m in out_invoices if m.get('payment_state') not in PAID_STATES]
    if unpaid:
        states = ', '.join(sorted({m.get('payment_state') or '?' for m in unpaid}))
        return False, f"{len(unpaid)} invoice(s) not paid ({states})", 0.0

    invoiced = sum(float(m.get('amount_total') or 0) for m in out_invoices)
    # Credit notes that cancel a reversed invoice are already netted out by
    # dropping that invoice; only count the rest against what was paid.
    refunded = max(0.0, sum(float(m.get('amount_total') or 0) for m in refunds) - reversed_total)
    return True, "fully invoiced and paid", round(invoiced - refunded, 2)


def sync_odoo_payments(shop_url, apply_changes=True, limit=None,
                       extra_query=f"created_at:>={ODOO_ERA_START}"):
    """
    Mark Shopify orders as paid once Odoo has them fully invoiced and paid.

    apply_changes=False is a dry run: it reads both sides and logs what it
    would do, without writing to Shopify. Returns a summary dict.
    """
    from app import app

    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url):
            raise RuntimeError("Connection failed: could not reach Odoo or Shopify")

        mode = 'APPLY' if apply_changes else 'DRY RUN'
        summary = {'mode': mode, 'unpaid_in_shopify': 0, 'not_in_odoo': 0,
                   'not_paid_in_odoo': 0, 'marked_paid': 0, 'would_mark_paid': 0,
                   'amount_mismatch': 0, 'errors': 0, 'paid_orders': [],
                   'not_paid_reasons': {}}

        unpaid = fetch_unpaid_shopify_orders(extra_query)
        if limit:
            unpaid = unpaid[:int(limit)]
        summary['unpaid_in_shopify'] = len(unpaid)
        if not unpaid:
            return summary

        company_id = get_shop_company_id(shop_url)
        so_by_name = match_odoo_sale_orders(odoo, company_id, unpaid)

        move_ids = sorted({mid for so in so_by_name.values() for mid in so.get('invoice_ids') or []})
        moves = {}
        for i in range(0, len(move_ids), 500):
            chunk = odoo.models.execute_kw(
                odoo.db, odoo.uid, odoo.password, 'account.move', 'read', [move_ids[i:i + 500]],
                {'fields': ['name', 'move_type', 'state', 'payment_state', 'amount_total']})
            for m in chunk:
                moves[m['id']] = m

        line_ids = sorted({lid for so in so_by_name.values() for lid in so.get('order_line') or []})
        so_lines = {}
        for i in range(0, len(line_ids), 1000):
            for l in odoo.models.execute_kw(
                    odoo.db, odoo.uid, odoo.password, 'sale.order.line', 'read', [line_ids[i:i + 1000]],
                    {'fields': ['qty_to_invoice', 'display_type']}):
                so_lines[l['id']] = l

        for order in unpaid:
            name = order['name']
            so = so_by_name.get(name)
            if not so:
                summary['not_in_odoo'] += 1
                continue

            invoices = [moves[i] for i in so.get('invoice_ids') or [] if i in moves]
            lines = [so_lines[i] for i in so.get('order_line') or [] if i in so_lines]
            is_paid, reason, odoo_paid = odoo_order_is_paid(so, invoices, lines)
            if not is_paid:
                summary['not_paid_in_odoo'] += 1
                key = reason.split(' (')[0]
                summary['not_paid_reasons'][key] = summary['not_paid_reasons'].get(key, 0) + 1
                continue

            outstanding = float(((order.get('totalOutstandingSet') or {}).get('shopMoney') or {}).get('amount') or 0)
            inv_names = ', '.join(m['name'] for m in invoices if m.get('move_type') == 'out_invoice')
            note = f"{name} ({so['name']}, {inv_names}): Shopify outstanding ${outstanding:.2f}, Odoo paid ${odoo_paid:.2f}"
            # Short deliveries and price edits in Odoo leave the Shopify total
            # different from what was invoiced. Odoo is the ledger, so the order
            # is still marked paid; the difference is logged so it can be seen.
            mismatch = abs(outstanding - odoo_paid) > 1.00
            if mismatch:
                summary['amount_mismatch'] += 1
                note += " [amounts differ]"
            summary['paid_orders'].append(note)

            if not apply_changes:
                summary['would_mark_paid'] += 1
                continue

            try:
                data = _gql(MARK_PAID_MUTATION, {'input': {'id': order['id']}})
                res = data.get('orderMarkAsPaid') or {}
                errs = res.get('userErrors') or []
                if errs:
                    summary['errors'] += 1
                    log_event(ENTITY, 'Error',
                              f"Shopify refused to mark {name} paid: "
                              + '; '.join(e.get('message', '') for e in errs),
                              shop_url=shop_url)
                    continue
                summary['marked_paid'] += 1
                log_event(ENTITY, 'Warning' if mismatch else 'Success',
                          f"Marked paid: {note}", shop_url=shop_url)
            except Exception as e:
                summary['errors'] += 1
                log_event(ENTITY, 'Error', f"Failed to mark {name} paid: {e}", shop_url=shop_url)

        if summary['marked_paid'] or summary['would_mark_paid'] or summary['errors']:
            log_event(ENTITY, 'Info' if not summary['errors'] else 'Warning',
                      f"[{mode}] {summary['unpaid_in_shopify']} unpaid in Shopify; "
                      f"paid in Odoo: {summary['marked_paid'] or summary['would_mark_paid']}; "
                      f"not yet paid in Odoo: {summary['not_paid_in_odoo']}; "
                      f"no Odoo order: {summary['not_in_odoo']}; errors: {summary['errors']}",
                      shop_url=shop_url)
        return summary


def scheduled_payment_sync(shop_url):
    """Scheduler entry point. Off unless the shop has payment_sync_enabled."""
    from app import app
    with app.app_context():
        enabled = get_config('payment_sync_enabled', False, shop_url=shop_url)
        if str(enabled).lower() not in ('true', '1', 'yes'):
            return None
    return sync_odoo_payments(shop_url, apply_changes=True)


def mark_historic_orders_paid(shop_url, created_before, apply_changes=False):
    """
    One-off backfill for orders older than the live sync looks at.

    Business decision (Pratham, 18 Nov 2026): every Worthy Products order
    from the store's launch (2022) up to `created_before` has been paid -
    before April 2025 in the previous accounting system, after that in Odoo.
    So these orders are marked paid without needing a paid Odoo invoice.

    One exception, from the data rather than the decision: if Odoo still has
    an open (not paid / partly paid) customer invoice for the order, it is
    owed money, so it is skipped and listed, and the live sync marks it paid
    when Odoo does.

    Needs read_all_orders alongside write_orders: without it Shopify only
    returns the last 60 days and this finds nothing older. Returns a summary.
    """
    from app import app

    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url):
            raise RuntimeError("Connection failed: could not reach Odoo or Shopify")

        mode = 'APPLY' if apply_changes else 'DRY RUN'
        orders = fetch_unpaid_shopify_orders(f"created_at:<{created_before}")
        summary = {'mode': mode, 'created_before': created_before, 'unpaid_found': len(orders),
                   'oldest': orders[-1]['createdAt'] if orders else None,
                   'marked_paid': 0, 'would_mark_paid': 0, 'skipped_open_in_odoo': [],
                   'errors': [], 'by_year': {}}
        if not orders:
            return summary

        company_id = get_shop_company_id(shop_url)
        domain = [['move_type', '=', 'out_invoice'], ['state', '=', 'posted'],
                  ['payment_state', 'in', ['not_paid', 'partial']],
                  ['invoice_origin', 'like', 'ONLINE_']]
        if company_id:
            domain.append(['company_id', '=', int(company_id)])
        open_invs = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'account.move',
                                           'search_read', [domain],
                                           {'fields': ['name', 'invoice_origin', 'amount_residual']})
        open_by_ref = {}
        for m in open_invs:
            for ref in (m.get('invoice_origin') or '').split(','):
                open_by_ref.setdefault(ref.strip(), []).append(m)

        for o in orders:
            name = o['name']
            year = (o.get('createdAt') or '????')[:4]
            summary['by_year'][year] = summary['by_year'].get(year, 0) + 1
            owed = open_by_ref.get(f"ONLINE_{name}")
            if owed:
                summary['skipped_open_in_odoo'].append(
                    f"{name}: " + ', '.join(f"{m['name']} ${m['amount_residual']:.2f} owed" for m in owed))
                continue
            if not apply_changes:
                summary['would_mark_paid'] += 1
                continue
            try:
                data = _gql(MARK_PAID_MUTATION, {'input': {'id': o['id']}})
                errs = (data.get('orderMarkAsPaid') or {}).get('userErrors') or []
                if errs:
                    summary['errors'].append(f"{name}: " + '; '.join(e.get('message', '') for e in errs))
                    continue
                summary['marked_paid'] += 1
            except Exception as e:
                summary['errors'].append(f"{name}: {e}")

        log_event(ENTITY, 'Warning' if summary['errors'] else 'Info',
                  f"[{mode}] Historic backfill before {created_before}: {len(orders)} unpaid found, "
                  f"marked paid {summary['marked_paid'] or summary['would_mark_paid']}, "
                  f"skipped (open in Odoo) {len(summary['skipped_open_in_odoo'])}, "
                  f"errors {len(summary['errors'])}", shop_url=shop_url)
        return summary
