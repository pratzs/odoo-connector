# services/orders.py
from datetime import datetime
from models import db, ProcessedOrder, Shop, CustomerMap
from utils import get_config, set_config, log_event, acquire_distributed_lock

def process_order_data(data, odoo_client, shop_url): 
    """
    Syncs order with Distributed Redis Locking, Smart UOM Switching, and Date Filtering.
    """
    odoo = odoo_client
    shopify_id = str(data.get('id', ''))
    shopify_name = data.get('name')
    
    # 1. DEFINE LOCK KEY
    lock_key = f"lock:order:{shopify_id}"

    # 2. ACQUIRE LOCK
    with acquire_distributed_lock(lock_key, timeout=30) as acquired:
        if not acquired:
            return True, "Skipped: Currently being processed by another worker (Redis Lock)"

        # --- GUARD 0: DATE CHECK ---
        try:
            start_date_str = '2000-01-01' 
            shop_record = Shop.query.filter_by(shop_url=shop_url).first()
            if shop_record and getattr(shop_record, 'sync_start_date', None):
                start_date_str = shop_record.sync_start_date

            order_created_at = data.get('created_at', '')
            if order_created_at:
                order_date_iso = order_created_at.split('T')[0]
                order_date_dt = datetime.strptime(order_date_iso, "%Y-%m-%d")
                start_date_dt = datetime.strptime(start_date_str, "%Y-%m-%d")

                if order_date_dt < start_date_dt:
                    return True, f"Skipped: Order date ({order_date_iso}) is older than start date ({start_date_str})"
        except Exception as e:
            print(f"Date Check Warning for {shopify_name}: {e}")

       # --- GUARD 1: SQL DATABASE CHECK ---
        try:
            exists = db.session.get(ProcessedOrder, (shopify_id, shop_url))
            if exists:
                return True, "Skipped: Found in Local Lock (Already Processed)"
        except: pass

        # --- GUARD 2: Cancelled Checks ---
        if data.get('cancelled_at'): return False, "Skipped: Order is Cancelled."

        # --- LOCK IT PERMANENTLY IN DB ---
        try:
            new_lock = ProcessedOrder(shopify_id=shopify_id, shop_url=shop_url)
            db.session.add(new_lock)
            db.session.commit()
        except Exception as e:
            db.session.rollback()
            return True, "Skipped: Race Condition caught by DB Lock"

        # ==========================================================
        # ACTUAL ODOO SYNC STARTS HERE
        # ==========================================================
        try:
            email = data.get('email') or data.get('contact_email')
            client_ref = f"ONLINE_{shopify_name}"
            company_id = get_config('odoo_company_id', shop_url=shop_url) 
            
            # Double Check Odoo
            try:
                existing_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                    'sale.order', 'search', [[['client_order_ref', '=', client_ref]]])
                if existing_ids: return True, "Skipped: Order exists in Odoo."
            except: pass

            # 1. Resolve Odoo partner — CustomerMap first, email fallback
            # CustomerMap lookup avoids the synthetic/+ email mismatch problem:
            # Shopify may have a + alias but Odoo only knows the real email.
            partner = None
            shopify_customer_id = str(data.get('customer', {}).get('id', ''))
            if shopify_customer_id:
                mapping = CustomerMap.query.filter_by(
                    shopify_customer_id=shopify_customer_id,
                    shop_url=shop_url
                ).first()
                if mapping:
                    results = odoo.models.execute_kw(
                        odoo.db, odoo.uid, odoo.password,
                        'res.partner', 'search_read',
                        [[['id', '=', mapping.odoo_partner_id]]],
                        {'fields': ['id', 'name', 'email'], 'limit': 1}
                    )
                    if results:
                        partner = results[0]
                        log_event('Order', 'Info',
                            f"Partner resolved via CustomerMap: {partner['name']} (Odoo ID {partner['id']})",
                            shop_url=shop_url)

            # Fallback: email search scoped to the configured company so we never
            # accidentally match a same-email partner from a sibling Odoo company.
            if not partner:
                partner = odoo.search_partner_by_email(email, company_id=company_id)
            
            cust_data = data.get('customer', {})
            bill_addr = data.get('billing_address') or data.get('shipping_address') or {}
            ship_addr = data.get('shipping_address') or bill_addr
            
            if not partner:
                # NEW CUSTOMER: Only create if the email does not exist in Odoo at all
                company_name = (bill_addr.get('company') or '').strip()
                b2b_data = data.get('company')
                if b2b_data and isinstance(b2b_data, dict) and b2b_data.get('name'):
                    company_name = b2b_data.get('name').strip()
                
                first = (bill_addr.get('first_name') or '').strip()
                last = (bill_addr.get('last_name') or '').strip()
                person_name = f"{first} {last}".strip()
                
                final_name = company_name if company_name else (person_name or email)
                
                vals = {
                    'name': final_name, 'email': email, 'phone': cust_data.get('phone'),
                    'street': bill_addr.get('address1'), 'city': bill_addr.get('city'),
                    'zip': bill_addr.get('zip'), 'country_code': bill_addr.get('country_code'),
                    'is_company': True, 'company_type': 'company'
                }
                if company_id: vals['company_id'] = int(company_id)
                partner_id = odoo.create_partner(vals)
                partner = {'id': partner_id, 'name': final_name}
                
                if shopify_id and cust_data.get('id'):
                    try:
                        sh_cust_id = str(cust_data['id'])
                        if not CustomerMap.query.filter_by(shopify_customer_id=sh_cust_id, shop_url=shop_url).first():
                            db.session.add(CustomerMap(shop_url=shop_url, shopify_customer_id=sh_cust_id, odoo_partner_id=partner_id, email=email))
                            db.session.commit()
                    except Exception as e: 
                        db.session.rollback()
                        print(f"Customer Map Error: {e}")

            # 2. Assign Addresses to Order
            main_partner_id = partner.get('id') 
            
            # A. Invoice Address: Stick to Odoo's official billing address
            try:
                addr_res = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                    'res.partner', 'address_get', [[main_partner_id], ['invoice']])
                invoice_id = addr_res.get('invoice', main_partner_id)
            except Exception as e:
                print(f"Odoo native invoice address routing error: {e}")
                invoice_id = main_partner_id
                
            # B. Delivery Address: Dynamic Shopify Mapping
            shipping_id = main_partner_id
            if ship_addr:
                try:
                    del_id = odoo.find_or_create_child_address(main_partner_id, ship_addr, type='delivery')
                    if del_id: 
                        shipping_id = del_id
                except Exception as e:
                    print(f"Shipping address routing error: {e}")
            
            sales_rep_id = odoo.get_partner_salesperson(main_partner_id) or odoo.uid

            # 3. SMART UOM LOOKUP
            unit_uom_id = None
            try:
                uom_names = ['Units', 'Unit', 'Piece', 'Pieces', 'PCE', 'ea', 'Each']
                uom_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                    'uom.uom', 'search', [[['name', 'in', uom_names]]])
                if not uom_ids:
                    uom_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                        'uom.uom', 'search', [[['name', 'ilike', 'Unit']]])
                if uom_ids: unit_uom_id = uom_ids[0]
            except Exception as e:
                print(f"UOM Lookup Error: {e}")

            ctx = {'allowed_company_ids': [int(company_id)], 'company_id': int(company_id)} if company_id else {}

            lines = []
            for item in data.get('line_items', []):
                raw_sku = item.get('sku')
                if not raw_sku: continue

                sku = raw_sku
                is_unit_variant = False
                if sku.endswith('-UNIT'):
                    sku = sku.replace('-UNIT', '')
                    is_unit_variant = True

                product_id = None
                p_domain = [['default_code', '=', sku]]
                if company_id:
                    p_domain += [['company_id', '=', int(company_id)]]
                
                try:
                    p_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                        'product.product', 'search', [p_domain], {'limit': 1, 'context': ctx})
                    if p_ids: product_id = p_ids[0]
                except: pass

                if not product_id:
                    if not odoo.check_product_exists_by_sku(sku, company_id):
                        try:
                            new_p = {'name': item['name'], 'default_code': sku, 'list_price': float(item.get('price', 0)), 'type': 'product'}
                            if company_id: new_p['company_id'] = int(company_id)
                            odoo.create_product(new_p)
                            p_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                                'product.product', 'search', [p_domain], {'limit': 1, 'context': ctx})
                            if p_ids: product_id = p_ids[0]
                        except: pass

                # If still not found, try stripping the pack suffix (e.g. OBBA24C-6perpack -> OBBA24C)
                if not product_id and '-' in raw_sku and not is_unit_variant:
                    base_sku = raw_sku.rsplit('-', 1)[0]
                    base_domain = [['default_code', '=', base_sku]]
                    if company_id:
                        base_domain += [['company_id', '=', int(company_id)]]
                    try:
                        p_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                            'product.product', 'search', [base_domain], {'limit': 1, 'context': ctx})
                        if p_ids:
                            product_id = p_ids[0]
                            sku = base_sku
                            is_unit_variant = True
                    except: pass

                if product_id:
                    price = float(item.get('price', 0))
                    qty = int(item.get('quantity', 1))
                    disc = float(item.get('total_discount', 0))
                    pct = (disc / (price * qty)) * 100 if price > 0 else 0.0
                    
                    line_vals = {'product_id': product_id, 'product_uom_qty': qty, 'price_unit': price, 'name': item['name'], 'discount': pct}
                    
                    variant_title = (item.get('variant_title') or '').lower()
                    title_indicates_unit = any(x in variant_title for x in ['unit', 'single', 'each', 'bottle', 'can', 'pce', 'per pack'])
                    
                    if unit_uom_id and (is_unit_variant or title_indicates_unit):
                        line_vals['product_uom'] = unit_uom_id
                        log_event('Order', 'Info', f"[UOM] Switched {raw_sku} to Unit UOM (ID: {unit_uom_id})", shop_url=shop_url)
                    
                    lines.append((0, 0, line_vals))

            # Shipping
            for ship_line in data.get('shipping_lines', []):
                cost = float(ship_line.get('price', 0.0))
                if cost >= 0:
                    s_title = ship_line.get('title', 'Shipping')
                    
                    sp_id = None
                    s_domain = [['name', '=', s_title]]
                    if company_id: s_domain += [['company_id', '=', int(company_id)]]
                    try:
                        s_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'search', [s_domain], {'limit': 1, 'context': ctx})
                        if s_ids: sp_id = s_ids[0]
                    except: pass

                    if not sp_id:
                        s_sku_domain = [['default_code', '=', 'SHIP_FEE']]
                        if company_id: s_sku_domain += [['company_id', '=', int(company_id)]]
                        try:
                            s_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'search', [s_sku_domain], {'limit': 1, 'context': ctx})
                            if s_ids: sp_id = s_ids[0]
                        except: pass

                    if not sp_id:
                        try:
                            sv = {'name': s_title, 'type': 'service', 'list_price': 0.0, 'default_code': 'SHIP_FEE'}
                            if company_id: sv['company_id'] = int(company_id)
                            odoo.create_product(sv)
                            s_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'search', [s_sku_domain], {'limit': 1, 'context': ctx})
                            if s_ids: sp_id = s_ids[0]
                        except: pass

                    if sp_id: lines.append((0, 0, {'product_id': sp_id, 'product_uom_qty': 1, 'price_unit': cost, 'name': s_title, 'discount': 0.0}))

            if not lines:
                try:
                    lock = ProcessedOrder.query.get((shopify_id, shop_url))
                    if lock:
                        db.session.delete(lock)
                        db.session.commit()
                except:
                    pass
                return False, "No valid lines — lock released for retry"

            gateway = data.get('gateway') or (data.get('payment_gateway_names')[0] if data.get('payment_gateway_names') else 'Shopify')
            customer_note = data.get('note') or ""
            note_text = f"Payment Gateway: {gateway}"
            if customer_note: note_text = f"Customer Note: {customer_note}\n\n{note_text}"

            vals = {
                'name': client_ref, 'client_order_ref': client_ref, 'partner_id': main_partner_id, 
                'partner_invoice_id': invoice_id, 'partner_shipping_id': shipping_id, 
                'order_line': lines, 'user_id': sales_rep_id, 'state': 'draft', 'note': note_text
            }
            if company_id: vals['company_id'] = int(company_id)
            
            sync_tax_included = get_config('order_sync_tax', False, shop_url=shop_url)
            odoo.create_sale_order(vals, context={'manual_price': True, 'tax_included': sync_tax_included})
            
            log_event('Order', 'Success', f"Synced {client_ref}", shop_url=shop_url)
            return True, "Synced"

        except Exception as e:
            log_event('Order', 'Error', f"Error {shopify_name}: {e}", shop_url=shop_url)
            try:
                l = ProcessedOrder.query.get((shopify_id, shop_url))
                if l:
                    db.session.delete(l)
                    db.session.commit()
            except: pass
            return False, str(e)
