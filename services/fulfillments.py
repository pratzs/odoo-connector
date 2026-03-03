import shopify
import json
from datetime import datetime, timedelta
from utils import get_odoo_connection, log_event, setup_shopify_session
from models import db, Shop


def sync_odoo_fulfillments(shop_url):
    """
    Background Task: Checks Odoo for 'Done' Outgoing Shipments
    and fulfills the order in Shopify using the FulfillmentOrder API.
    """
    from app import app

    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url):
            return

        # ── 1. Fetch Done Outgoing Shipments from Odoo ─────────────────
        cutoff = datetime.utcnow() - timedelta(minutes=120)

        domain = [
            ['state', '=', 'done'],
            ['picking_type_id.code', '=', 'outgoing'],
            ['date_done', '>=', str(cutoff)],
            ['origin', 'like', 'ONLINE_']
        ]

        try:
            pickings = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'stock.picking', 'search_read', [domain],
                {'fields': ['origin', 'carrier_tracking_ref', 'carrier_id', 'name']})
        except Exception as e:
            log_event('Fulfillment', 'Error', f"Odoo Search Failed: {e}", shop_url=shop_url)
            return

        if not pickings:
            log_event('Fulfillment', 'Info', "Job Complete: No new fulfilled orders found.", shop_url=shop_url)
            return

        synced_count = 0

        for pick in pickings:
            order_name = pick['origin'].replace('ONLINE_', '').strip()
            tracking_number = pick.get('carrier_tracking_ref')
            carrier_name = pick['carrier_id'][1] if pick.get('carrier_id') else 'Other'

            if not tracking_number:
                log_event('Fulfillment', 'Info',
                    f"Skipped {order_name}: No tracking number on picking {pick['name']}",
                    shop_url=shop_url)
                continue

            try:
                # ── 2. Find the Shopify Order ───────────────────────────
                orders = shopify.Order.find(name=order_name, status='open')
                if not orders:
                    log_event('Fulfillment', 'Warning',
                        f"Shopify order {order_name} not found or already closed.",
                        shop_url=shop_url)
                    continue

                order = orders[0]

                # ── 3. Skip if already fully fulfilled ──────────────────
                if order.fulfillment_status == 'fulfilled':
                    continue

                # ── 4. Fetch FulfillmentOrders for this Order ───────────
                # This is the new required approach from Shopify API 2022-07+
                response = shopify.FulfillmentOrder.find(order_id=order.id)

                # Filter to only open/in_progress fulfillment orders
                open_fulfillment_orders = [
                    fo for fo in response
                    if fo.status in ('open', 'in_progress')
                ]

                if not open_fulfillment_orders:
                    log_event('Fulfillment', 'Warning',
                        f"No open FulfillmentOrders found for {order_name}.",
                        shop_url=shop_url)
                    continue

                # ── 5. Create Fulfillment via FulfillmentOrder API ──────
                # Build the line_items_by_fulfillment_order list
                # (tells Shopify which fulfillment order IDs we are fulfilling)
                fulfillment_order_line_items = [
                    {"fulfillment_order_id": fo.id}
                    for fo in open_fulfillment_orders
                ]

                fulfillment = shopify.Fulfillment()
                fulfillment.line_items_by_fulfillment_order = fulfillment_order_line_items
                fulfillment.tracking_info = {
                    "number": tracking_number,
                    "company": carrier_name,
                }
                fulfillment.notify_customer = True  # Sends Shopify shipping email

                fulfillment.save()

                # ── 6. Check for errors ─────────────────────────────────
                if fulfillment.errors and fulfillment.errors.full_messages():
                    error_msg = ", ".join(fulfillment.errors.full_messages())
                    log_event('Fulfillment', 'Error',
                        f"Shopify rejected fulfillment for {order_name}: {error_msg}",
                        shop_url=shop_url)
                    continue

                synced_count += 1
                log_event('Fulfillment', 'Success',
                    f"Fulfilled {order_name} with tracking {tracking_number} via {carrier_name}",
                    shop_url=shop_url)

            except Exception as e:
                err_str = str(e)
                if "already fulfilled" not in err_str.lower():
                    log_event('Fulfillment', 'Error',
                        f"Failed to fulfill {order_name}: {err_str}",
                        shop_url=shop_url)

        # ── 7. Update Dashboard Timestamp ───────────────────────────────
        if synced_count > 0:
            try:
                shop = Shop.query.filter_by(shop_url=shop_url).first()
                if shop:
                    shop.last_order_sync_success = datetime.utcnow()
                    db.session.commit()
            except Exception as e:
                print(f"Timestamp update error: {e}")

        if synced_count > 0:
            log_event('Fulfillment', 'Success',
                f"Fulfillment sync complete. Pushed {synced_count} shipments to Shopify.",
                shop_url=shop_url)
```
