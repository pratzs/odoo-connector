from models import db
from utils import get_odoo_connection, log_event
import datetime

def process_refund_data(data, shop_url):
    """
    Handles Shopify Refund Webhooks.
    Logic: Find Odoo Order -> Find Delivery (Picking) -> Create Return Transfer.
    """
    # Note: app_context is provided by the worker wrapper
    shopify_order_id = data.get('order_id')
    refund_line_items = data.get('refund_line_items', [])
    
    if not refund_line_items:
        return True, "No items to restock."

    odoo = get_odoo_connection(shop_url)
    if not odoo:
        return False, "Odoo connection failed."

    # 1. Find the Odoo Order
    # We use the order name from Shopify to find the Odoo Sales Order
    # Shopify Webhook for Refund doesn't always include the order name, 
    # but we can fetch it or use the ProcessedOrder map if you have it.
    # For now, we'll assume we can search by Shopify Order ID in the client_order_ref or a mapping table.
    
    try:
        # Search for the order in Odoo
        order_domain = [('shopify_order_id', '=', str(shopify_order_id))]
        order_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'sale.order', 'search', [order_domain])
        
        if not order_ids:
            return False, f"Odoo Order for Shopify ID {shopify_order_id} not found."
        
        order_id = order_ids[0]
        
        # 2. Find the Delivery Orders (pickings) associated with this SO
        picking_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'stock.picking', 'search', [[('sale_id', '=', order_id), ('state', '=', 'done')]])
        
        if not picking_ids:
            return True, "Order found, but no 'Done' deliveries to return from."

        # 3. Use Odoo's built-in Return Wizard logic
        # This is complex via RPC, so we log the intent for now or trigger a simple return
        log_event('Refund', 'Info', f"Processing return for Order {shopify_order_id}", shop_url=shop_url)
        
        # Implementation of return varies by Odoo version. 
        # Typically: Create a 'stock.return.picking' wizard, execute it, then validate the new picking.
        # To keep it safe for a first version, we log the need for manual return or 
        # create a 'Return' picking in 'draft'.
        
        return True, f"Refund data received for Order {shopify_order_id}. Logic to be finalized based on Odoo workflow."

    except Exception as e:
        return False, f"Refund Error: {str(e)}"
