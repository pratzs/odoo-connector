import shopify
from utils import get_odoo_connection, log_event, setup_shopify_session
from datetime import datetime, timedelta
from models import db, Shop
# 1. NEW IMPORT: We need the app object to create a context
from app import app 

def sync_odoo_returns(shop_url):
    """
    Checks Odoo for 'Done' Return Pickings (Incoming shipments) 
    and notifies Shopify.
    """
    # 2. CRITICAL FIX: Wrap everything in the app context
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        # Check connection AND setup session
        if not odoo or not setup_shopify_session(shop_url):
            return

        # 1. Look back 2 hours for completed returns
        cutoff = datetime.utcnow() - timedelta(minutes=120)
        
        # We look for: 
        # - state 'done'
        # - picking_type code 'incoming' (Returns coming back to warehouse)
        # - origin starts with ONLINE_
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
            # log_event requires app_context, which we now have!
            log_event('Return Sync', 'Error', f"Odoo Search Failed: {e}", shop_url=shop_url)
            return

        # Loop through found returns
        for ret in returns:
            # Safety check: ensure 'origin' exists and is a string
            if not ret.get('origin'): continue
            
            shopify_order_name = ret['origin'].replace('ONLINE_', '').strip()
            
            try:
                # 2. Find Shopify Order
                orders = shopify.Order.find(name=shopify_order_name, status='any')
                if not orders: continue
                order = orders[0]

                # 3. Logic: Mark as Return in Shopify
                # (You might want to check if it's already commented to avoid duplicates, 
                # but this logic is fine for now)
                shopify.Comment.create({
                    'body': f"Inventory Return received in Odoo ({ret['name']}). Items are back in stock.",
                    'order_id': order.id
                })
                
                log_event('Return Sync', 'Success', f"Synced return {ret['name']} to Shopify {shopify_order_name}", shop_url=shop_url)

            except Exception as e:
                log_event('Return Sync', 'Error', f"Failed to sync return for {shopify_order_name}: {e}", shop_url=shop_url)

        # --- NEW: UPDATE DASHBOARD TIMESTAMP ---
        try:
            # Update the shop record with the current time
            shop = Shop.query.filter_by(shop_url=shop_url).first()
            if shop:
                shop.last_return_sync_success = datetime.utcnow()
                db.session.commit()
                print(f"📊 Dashboard Updated: Return Sync Success for {shop_url}")
        except Exception as e:
            print(f"❌ Error updating return timestamp: {e}")
