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


def _gql(query, variables):
    raw = shopify.GraphQL().execute(query, variables)
    data = json.loads(raw)
    if data.get('errors'):
        raise RuntimeError(f"Shopify GraphQL error: {data['errors']}")
    return data.get('data') or {}


def fetch_unpaid_shopify_orders():
    """All open Shopify orders still awaiting payment, newest first."""
    q = "(financial_status:pending OR financial_status:partially_paid) AND -status:cancelled"
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


def sync_odoo_payments(shop_url, apply_changes=True, limit=None):
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

        unpaid = fetch_unpaid_shopify_orders()
        if limit:
            unpaid = unpaid[:int(limit)]
        summary['unpaid_in_shopify'] = len(unpaid)
        if not unpaid:
            return summary

        # Odoo sale orders for these Shopify orders, one query.
        company_id = get_shop_company_id(shop_url)
        refs = [f"ONLINE_{o['name']}" for o in unpaid]
        domain = [['client_order_ref', 'in', refs]]
        if company_id:
            domain.append(['company_id', '=', int(company_id)])
        sos = odoo.models.execute_kw(
            odoo.db, odoo.uid, odoo.password, 'sale.order', 'search_read', [domain],
            {'fields': ['name', 'client_order_ref', 'state', 'delivery_status',
                        'invoice_ids', 'order_line', 'amount_total']})
        so_by_ref = {}
        for so in sos:
            # If a ref somehow exists twice, prefer the confirmed one.
            prev = so_by_ref.get(so['client_order_ref'])
            if not prev or (prev.get('state') not in ('sale', 'done')):
                so_by_ref[so['client_order_ref']] = so

        move_ids = sorted({mid for so in so_by_ref.values() for mid in so.get('invoice_ids') or []})
        moves = {}
        for i in range(0, len(move_ids), 500):
            chunk = odoo.models.execute_kw(
                odoo.db, odoo.uid, odoo.password, 'account.move', 'read', [move_ids[i:i + 500]],
                {'fields': ['name', 'move_type', 'state', 'payment_state', 'amount_total']})
            for m in chunk:
                moves[m['id']] = m

        line_ids = sorted({lid for so in so_by_ref.values() for lid in so.get('order_line') or []})
        so_lines = {}
        for i in range(0, len(line_ids), 1000):
            for l in odoo.models.execute_kw(
                    odoo.db, odoo.uid, odoo.password, 'sale.order.line', 'read', [line_ids[i:i + 1000]],
                    {'fields': ['qty_to_invoice', 'display_type']}):
                so_lines[l['id']] = l

        for order in unpaid:
            name = order['name']
            so = so_by_ref.get(f"ONLINE_{name}")
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
