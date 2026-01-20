import shopify
from datetime import datetime, timedelta
from utils import get_odoo_connection, log_event, setup_shopify_session, get_config

def sync_odoo_cancellations(shop_url):
    """
    Background Task: Checks for orders cancelled in Odoo and cancels them in Shopify.
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
