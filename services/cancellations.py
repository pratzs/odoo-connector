import shopify
from datetime import datetime, timedelta
from utils import get_odoo_connection, log_event, setup_shopify_session, get_config

def sync_odoo_cancellations(shop_url):
    """
    Background Task: Checks for orders cancelled in Odoo and cancels them in Shopify.
    Direction: Odoo -> Shopify
    """
    # Import app inside function to prevent circular errors
    from app import app
    
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): 
            return

        # 1. Look back 7 days
        cutoff = datetime.utcnow() - timedelta(days=7)
        
        # Search Odoo for cancelled orders that originated from Shopify
        domain = [
            ['state', '=', 'cancel'],
            ['write_date', '>=', str(cutoff)],
            ['client_order_ref', 'like', 'ONLINE_'] # Ensure it's a Shopify order
        ]

        try:
            # Using standard execute_kw (safer than custom methods)
            cancelled_orders = odoo.execute('sale.order', 'search_read', domain, ['client_order_ref', 'name'])
        except Exception as e:
            log_event('Cancel Sync', 'Error', f"Odoo Search Failed: {e}", shop_url=shop_url)
            return

        sync_count = 0
        for o_order in cancelled_orders:
            ref = o_order.get('client_order_ref', '')
            if not ref: continue
            
            shopify_name = ref.replace('ONLINE_', '').strip()
            
            try:
                # Find Shopify Order
                orders = shopify.Order.find(name=shopify_name, status='any')
                if not orders: continue
                sp_order = orders[0]

                # If not already cancelled in Shopify, cancel it
                if sp_order.cancelled_at is None:
                    sp_order.cancel(reason="other", email=False)
                    sync_count += 1
                    log_event('Cancel Sync', 'Success', f"Cancelled Shopify Order {shopify_name}", shop_url=shop_url)
            
            except Exception as e:
                # Ignore 422 (Unprocessable Entity) which usually means "Already Cancelled"
                if "422" not in str(e):
                    log_event('Cancel Sync', 'Error', f"Failed to cancel {shopify_name}: {e}", shop_url=shop_url)

        if sync_count > 0:
            log_event('Cancel Sync', 'Success', f"Synced {sync_count} cancellations from Odoo.", shop_url=shop_url)


def process_cancellation(data, shop_url):
    """
    Webhook Handler: triggered when an order is cancelled in Shopify.
    Direction: Shopify -> Odoo
    """
    # Note: We don't need 'with app.app_context()' here because 
    # RQ workers load the context automatically when running the job.
    
    shopify_name = data.get('name')
    client_ref = f"ONLINE_{shopify_name}"
    
    odoo = get_odoo_connection(shop_url)
    if not odoo:
        log_event('Order Cancel', 'Error', f"Could not connect to Odoo for {shop_url}", shop_url=shop_url)
        return

    try:
        # 1. Find the Order ID in Odoo
        domain = [['client_order_ref', '=', client_ref]]
        ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
            'sale.order', 'search', [domain])
        
        if not ids:
            log_event('Order Cancel', 'Warning', f"Order {client_ref} not found in Odoo. Skipping.", shop_url=shop_url)
            return

        order_id = ids[0]

        # 2. Check current state & Cancel
        try:
            odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'action_cancel', [[order_id]])
            log_event('Order Cancel', 'Success', f"Cancelled Odoo Order {client_ref}", shop_url=shop_url)
        except Exception as e:
             log_event('Order Cancel', 'Warning', f"Could not cancel {client_ref}. Odoo said: {e}", shop_url=shop_url)

    except Exception as e:
        log_event('Order Cancel', 'Error', f"Error processing cancellation for {shopify_name}: {e}", shop_url=shop_url)
