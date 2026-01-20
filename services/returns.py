import shopify
from utils import get_odoo_connection, log_event, setup_shopify_session
from datetime import datetime, timedelta

def sync_odoo_returns(shop_url):
    """
    Checks Odoo for 'Done' Return Pickings (Incoming shipments) 
    and notifies Shopify.
    """
    odoo = get_odoo_connection(shop_url)
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
        log_event('Return Sync', 'Error', f"Odoo Search Failed: {e}", shop_url=shop_url)
        return

    for ret in returns:
        shopify_order_name = ret['origin'].replace('ONLINE_', '').strip()
        
        try:
            # 2. Find Shopify Order
            orders = shopify.Order.find(name=shopify_order_name, status='any')
            if not orders: continue
            order = orders[0]

            # 3. Logic: Mark as Return in Shopify
            # Note: We don't automatically trigger a "Refund" (Money) here
            # because money should usually be handled by a manager. 
            # We just want to log that the items are back.
            
            shopify.Comment.create({
                'body': f"Inventory Return received in Odoo ({ret['name']}). Items are back in stock.",
                'order_id': order.id
            })
            
            log_event('Return Sync', 'Success', f"Synced return {ret['name']} to Shopify {shopify_order_name}", shop_url=shop_url)

        except Exception as e:
            log_event('Return Sync', 'Error', f"Failed to sync return for {shopify_order_name}: {e}", shop_url=shop_url)
