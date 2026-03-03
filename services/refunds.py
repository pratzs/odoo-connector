import shopify
from utils import get_odoo_connection, log_event, setup_shopify_session

def process_refund_data(data, shop_url):
    """
    Handles Shopify Refund Webhook -> Odoo Credit Note (Reverse Transfer).
    Strategy: Find the confirmed invoice for the sale order, then reverse it.
    """
    from app import app

    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo:
            return False, "No Odoo connection"

        # ── 1. Get the Shopify Order Name ──────────────────────────────
        order_id = data.get('order_id')
        shopify_order_name = ""

        try:
            if setup_shopify_session(shop_url):
                sp_order = shopify.Order.find(order_id)
                shopify_order_name = sp_order.name
        except Exception as e:
            log_event('Refund', 'Warning', f"Could not fetch Shopify order {order_id}: {e}", shop_url=shop_url)

        if not shopify_order_name:
            return False, f"Could not determine Order Name for Refund {data.get('id')}"

        client_ref = f"ONLINE_{shopify_order_name}"

        try:
            # ── 2. Find the Sale Order in Odoo ─────────────────────────
            so_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])

            if not so_ids:
                return False, f"Sale order {client_ref} not found in Odoo"

            so_id = so_ids[0]

            # ── 3. Find the Confirmed Invoice for this Sale Order ───────
            inv_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'account.move', 'search', [[
                    ['invoice_origin', 'like', client_ref],
                    ['move_type', '=', 'out_invoice'],
                    ['state', '=', 'posted']   # Only confirmed invoices can be reversed
                ]])

            if not inv_ids:
                # Invoice not confirmed yet — log it for manual follow-up
                log_event('Refund', 'Warning',
                    f"Refund received for {client_ref} but no confirmed invoice found. "
                    f"Please create credit note manually.",
                    shop_url=shop_url)
                return True, f"Skipped: No confirmed invoice for {client_ref}"

            invoice_id = inv_ids[0]

            # ── 4. Check if a Credit Note already exists ────────────────
            existing_credit = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'account.move', 'search', [[
                    ['reversed_entry_id', '=', invoice_id],
                    ['move_type', '=', 'out_refund']
                ]])

            if existing_credit:
                return True, f"Skipped: Credit note already exists for {client_ref}"

            # ── 5. Create the Credit Note (Reverse the Invoice) ─────────
            # This calls Odoo's built-in reversal wizard
            reversal_result = odoo.models.execute_kw(
                odoo.db, odoo.uid, odoo.password,
                'account.move.reversal', 'create',
                [{
                    'move_ids': [(4, invoice_id)],
                    'reason': f"Shopify Refund - {shopify_order_name}",
                    'refund_method': 'refund',   # 'refund' = partial, 'cancel' = full reversal
                    'journal_id': False           # Uses the invoice's journal automatically
                }]
            )

            # Call the action to actually generate the credit note
            odoo.models.execute_kw(
                odoo.db, odoo.uid, odoo.password,
                'account.move.reversal', 'reverse_moves',
                [[reversal_result]]
            )

            log_event('Refund', 'Success',
                f"Credit note created for {client_ref} (Invoice ID: {invoice_id})",
                shop_url=shop_url)
            return True, f"Credit note created for {client_ref}"

        except Exception as e:
            log_event('Refund', 'Error', f"Refund processing failed for {client_ref}: {e}", shop_url=shop_url)
            return False, str(e)
