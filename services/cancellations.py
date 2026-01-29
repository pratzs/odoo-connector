import shopify
from datetime import datetime, timedelta
from utils import get_odoo_connection, log_event, setup_shopify_session, get_config

# --- 1. Odoo -> Shopify (Sync Job) ---
def sync_odoo_cancellations(shop_url):
    """
    Background Task: Checks for orders cancelled in Odoo and cancels them in Shopify.
    """
    from app import app
    
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): 
            return

        # Look back 7 days
        cutoff = datetime.utcnow() - timedelta(days=7)
        
        domain = [
            ['state', '=', 'cancel'],
            ['write_date', '>=', str(cutoff)],
            ['client_order_ref', 'like', 'ONLINE_'] 
        ]

        try:
            cancelled_orders = odoo.execute('sale.order', 'search_read', domain, ['client_order_ref', 'name'])
        except Exception as e:
            log_event('Cancel Sync', 'Error', f"Odoo Search Failed: {e}", shop_url=shop_url)
            return

        # --- FIX START: Log if nothing found so the user isn't left guessing ---
        if not cancelled_orders:
            log_event('Cancel Sync', 'Info', "Job Complete: No new cancellations found in Odoo.", shop_url=shop_url)
            return
        # --- FIX END -----------------------------------------------------------

        sync_count = 0
        for o_order in cancelled_orders:
            ref = o_order.get('client_order_ref', '')
            if not ref: continue
            
            shopify_name = ref.replace('ONLINE_', '').strip()
            
            try:
                orders = shopify.Order.find(name=shopify_name, status='any')
                if not orders: continue
                sp_order = orders[0]

                if sp_order.cancelled_at is None:
                    sp_order.cancel(reason="other", email=False)
                    sync_count += 1
                    log_event('Cancel Sync', 'Success', f"Cancelled Shopify Order {shopify_name}", shop_url=shop_url)
            
            except Exception as e:
                # Ignore 422 errors (usually means "Already Cancelled")
                if "422" not in str(e):
                    log_event('Cancel Sync', 'Error', f"Failed to cancel {shopify_name}: {e}", shop_url=shop_url)

        if sync_count > 0:
            log_event('Cancel Sync', 'Success', f"Synced {sync_count} cancellations from Odoo.", shop_url=shop_url)


# --- 2. Shopify -> Odoo (Webhook Handler) ---
def process_cancellation(data, shop_url):
    """
    Webhook Handler: triggered when an order is cancelled in Shopify.
    """
    # ✅ FIX: Load App Context so DB access works
    from app import app
    
    with app.app_context():
        shopify_name = data.get('name')
        # We assume the Odoo 'client_order_ref' is 'ONLINE_#1001'
        client_ref = f"ONLINE_{shopify_name}"
        
        odoo = get_odoo_connection(shop_url)
        if not odoo:
            log_event('Order Cancel', 'Error', f"Could not connect to Odoo for {shop_url}", shop_url=shop_url)
            return

        try:
            # 1. Find the Order ID in Odoo
            # Note: We search by 'client_order_ref', NOT 'shopify_order_id'
            domain = [['client_order_ref', '=', client_ref]]
            ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                'sale.order', 'search', [domain])
            
            if not ids:
                log_event('Order Cancel', 'Warning', f"Order {client_ref} not found in Odoo. Skipping.", shop_url=shop_url)
                return

            order_id = ids[0]

            # 2. Cancel in Odoo
            try:
                odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'action_cancel', [[order_id]])
                log_event('Order Cancel', 'Success', f"Cancelled Odoo Order {client_ref}", shop_url=shop_url)
            except Exception as e:
                 log_event('Order Cancel', 'Warning', f"Could not cancel {client_ref}. Odoo said: {e}", shop_url=shop_url)

        except Exception as e:
            log_event('Order Cancel', 'Error', f"Error processing cancellation for {shopify_name}: {e}", shop_url=shop_url)
