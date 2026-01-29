import shopify
from datetime import datetime, timedelta
from utils import get_odoo_connection, log_event, setup_shopify_session
from models import db, Shop

def sync_odoo_fulfillments(shop_url):
    """
    Background Task: Checks Odoo for 'Done' Outgoing Shipments
    and fulfills the order in Shopify with tracking info.
    """
    from app import app
    
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url): 
            return

        # 1. Look back 2 hours for 'Done' shipments
        cutoff = datetime.utcnow() - timedelta(minutes=120)
        
        domain = [
            ['state', '=', 'done'],
            ['picking_type_id.code', '=', 'outgoing'], # Only outgoing shipments
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

        # --- FIX START: Log if nothing found so the user isn't left guessing ---
        if not pickings:
            log_event('Fulfillment', 'Info', "Job Complete: No new fulfilled orders found.", shop_url=shop_url)
            return
        # --- FIX END -----------------------------------------------------------

        synced_count = 0
        for pick in pickings:
            order_name = pick['origin'].replace('ONLINE_', '').strip()
            tracking_number = pick.get('carrier_tracking_ref')
            
            if not tracking_number: continue

            try:
                # A. Find Shopify Order
                orders = shopify.Order.find(name=order_name, status='open')
                if not orders: continue
                order = orders[0]

                # B. Skip if already fulfilled
                if order.fulfillment_status == 'fulfilled': continue

                # C. Create Fulfillment
                fulfillment = shopify.Fulfillment({
                    'order_id': order.id,
                    'location_id': order.location_id,
                    'tracking_info': {
                        'number': tracking_number,
                        'company': pick['carrier_id'][1] if pick['carrier_id'] else 'Other'
                    }
                })
                
                fulfillment.save()
                synced_count += 1
                log_event('Fulfillment', 'Success', f"Fulfilled {order_name} with tracking {tracking_number}", shop_url=shop_url)

            except Exception as e:
                if "already fulfilled" not in str(e).lower():
                    log_event('Fulfillment', 'Error', f"Failed {order_name}: {e}", shop_url=shop_url)

        # Update Last Sync Timestamp
        if synced_count > 0:
            try:
                shop = Shop.query.filter_by(shop_url=shop_url).first()
                if shop:
                    shop.last_order_sync_success = datetime.utcnow()
                    db.session.commit()
            except Exception as e:
                print(f"Error updating timestamp: {e}")
