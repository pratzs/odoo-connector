from models import db
from utils import get_odoo_connection, log_event
from datetime import datetime, timedelta

def process_refund_data(data, shop_url):
    """
    Handles Shopify Refund Webhooks by creating an Activity in Odoo.
    """
    shopify_order_id = data.get('order_id')
    # Get total amount refunded for the note
    transactions = data.get('transactions', [])
    refund_amount = sum(float(t.get('amount', 0.0)) for t in transactions)
    
    odoo = get_odoo_connection(shop_url)
    if not odoo:
        return False, "Odoo connection failed."

    try:
        # 1. Find the Odoo Order using Shopify ID
        # (Assuming you store shopify_order_id on the sale.order)
        order_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'sale.order', 'search', [[('shopify_order_id', '=', str(shopify_order_id))]])
        
        if not order_ids:
            return False, f"Odoo Order for Shopify ID {shopify_order_id} not found."
        
        order_id = order_ids[0]
        
        # 2. Get the User ID responsible for the order to assign the activity
        order_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
            'sale.order', 'read', [order_id], {'fields': ['user_id']})
        user_id = order_data[0]['user_id'][0] if order_data[0].get('user_id') else odoo.uid

        # 3. Create an Activity (mail.activity)
        activity_note = f"<b>Shopify Refund Detected</b><br/>Amount: ${refund_amount}<br/>Action required: Check inventory returns and adjust Odoo invoice."
        
        activity_vals = {
            'res_id': order_id,
            'res_model_id': odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'ir.model', 'search', [[('model', '=', 'sale.order')]])[0],
            'activity_type_id': 4, # 4 is usually 'To Do' in Odoo
            'summary': 'Shopify Refund - Action Required',
            'note': activity_note,
            'user_id': user_id,
            'date_deadline': datetime.now().strftime('%Y-%m-%d')
        }
        
        odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'mail.activity', 'create', [activity_vals])
        
        return True, f"Created Odoo Activity for Order {shopify_order_id}"

    except Exception as e:
        return False, f"Refund Activity Error: {str(e)}"
