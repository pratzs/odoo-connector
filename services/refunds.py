import shopify
from utils import get_odoo_connection, log_event

def process_refund(data, shop_url):
    """
    Handles Shopify Refund Webhook -> Odoo Credit Note
    """
    from app import app
    
    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo: return

        # 1. Get Shopify Order ID/Name
        # Webhooks usually send 'order_id' (numeric) but we sync using Name (#1001)
        # We need to fetch the order name if not present, or use the parent_id
        order_id = data.get('order_id')
        
        # We need the Order NAME (e.g. #1234) to match your 'ONLINE_#1234' pattern
        # Since the webhook might only give ID, we might need a quick lookup or heuristic
        # Ideally, we fetch the Shopify Order to get the 'name'
        shopify_order_name = ""
        
        try:
            # We assume session is set up by caller or we do it here
            from utils import setup_shopify_session
            if setup_shopify_session(shop_url):
                sp_order = shopify.Order.find(order_id)
                shopify_order_name = sp_order.name
        except:
            # Fallback: sometimes data contains it
            pass
            
        if not shopify_order_name:
            log_event('Refund', 'Warning', f"Could not determine Order Name for Refund {data.get('id')}", shop_url=shop_url)
            return

        client_ref = f"ONLINE_{shopify_order_name}"

        try:
            # 2. Find Odoo Sale Order using CLIENT_ORDER_REF
            # FIX: Do NOT use 'shopify_order_id'
            domain = [['client_order_ref', '=', client_ref]]
            ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'sale.order', 'search', [domain])
            
            if not ids:
                log_event('Refund', 'Warning', f"Original Order {client_ref} not found in Odoo.", shop_url=shop_url)
                return
            
            sale_order_id = ids[0]

            # 3. Log the "To-Do" (Creating actual Credit Notes is complex via API)
            # For now, we log that we found it, so you know the connection works.
            # Automated Credit Note creation requires finding the Invoice, not just the Order.
            log_event('Refund', 'Info', f"Refund detected for {client_ref}. Automated Credit Note creation is pending implementation.", shop_url=shop_url)

        except Exception as e:
            log_event('Refund', 'Error', f"Refund Logic Failed: {e}", shop_url=shop_url)
