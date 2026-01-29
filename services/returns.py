import shopify
from utils import get_odoo_connection, log_event, setup_shopify_session
from datetime import datetime, timedelta
from models import db, Shop

def sync_odoo_returns(shop_url):
    """
    Checks Odoo for 'Done' Return Pickings (Incoming shipments) 
    and notifies Shopify.
    """
    # --- CRITICAL FIX: IMPORT INSIDE THE FUNCTION ---
    # This prevents the Circular Import crash
    from app import app 

    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url):
            return

        # 1. Look back 2 hours for completed returns
        cutoff = datetime.utcnow() - timedelta(minutes=120)
        
        domain = [
            ['state', '=', 'done'],
            ['picking_type_id.code', '=', 'incoming'],
            ['date_done', '>=', str(cutoff)],
            ['origin', 'like', 'ONLINE_']
        ]

        try:
            returns = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'stock.picking', 'search_read', [domain], 
                {'fields': ['origin', 'name', 'move_ids_without_package']})
        except Exception as e:
            log_event('Return Sync', 'Error', f"Odoo Search Failed: {e}", shop_url=shop_url)
            return

        # --- FIX START: Log if nothing found so the user isn't left guessing ---
        if not returns:
            log_event('Return Sync', 'Info', "Job Complete: No new returns (incoming shipments) found.", shop_url=shop_url)
            return
        # --- FIX END -----------------------------------------------------------

        for ret in returns:
            if not ret.get('origin'): continue
            shopify_order_name = ret['origin'].replace('ONLINE_', '').strip()
            
            try:
                orders = shopify.Order.find(name=shopify_order_name, status='any')
                if not orders: continue
                order = orders[0]

                shopify.Comment.create({
                    'body': f"Inventory Return received in Odoo ({ret['name']}). Items are back in stock.",
                    'order_id': order.id
                })
                
                log_event('Return Sync', 'Success', f"Synced return {ret['name']} to Shopify {shopify_order_name}", shop_url=shop_url)

            except Exception as e:
                log_event('Return Sync', 'Error', f"Failed to sync return for {shopify_order_name}: {e}", shop_url=shop_url)

        # Update Dashboard Timestamp
        try:
            shop = Shop.query.filter_by(shop_url=shop_url).first()
            if shop:
                shop.last_return_sync_success = datetime.utcnow()
                db.session.commit()
        except Exception as e:
            print(f"Timestamp Error: {e}")
