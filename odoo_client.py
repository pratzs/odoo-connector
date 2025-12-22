import xmlrpc.client
import ssl

class OdooClient:
    def __init__(self, url, db, username, password):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        self.context = ssl._create_unverified_context()
        self.common = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/common', context=self.context, allow_none=True)
        self.uid = self.common.authenticate(self.db, self.username, self.password, {})

    @property
    def models(self):
        return xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/object', context=self.context, allow_none=True)

    def search_partner_by_email(self, email):
        domain = ['|', ['active', '=', True], ['active', '=', False], ['email', '=', email]]
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'search', [domain])
        if ids:
            partners = self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'read', [ids], {'fields': ['id', 'name', 'active', 'parent_id', 'user_id', 'category_id']})
            if not partners[0].get('active'):
                self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'write', [[partners[0]['id']], {'active': True}])
            return partners[0]
        return None

    def get_partner_salesperson(self, partner_id):
        data = self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'read', [[partner_id]], {'fields': ['user_id']})
        return data[0]['user_id'][0] if data and data[0].get('user_id') else None

    def create_partner(self, vals):
        self._resolve_country(vals)
        return self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'create', [vals])

    def find_or_create_child_address(self, parent_id, address_data, type='delivery'):
        domain = [['parent_id', '=', parent_id], ['type', '=', type], ['street', '=', address_data.get('street')], ['active', '=', True]]
        existing = self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'search', [domain])
        if existing: return existing[0]
        
        vals = {
            'parent_id': parent_id, 'type': type, 'name': address_data.get('name') or "Delivery Address",
            'street': address_data.get('street'), 'city': address_data.get('city'), 'zip': address_data.get('zip'),
            'country_code': address_data.get('country_code'), 'phone': address_data.get('phone'), 'email': address_data.get('email')
        }
        self._resolve_country(vals)
        return self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'create', [vals])

    def _resolve_country(self, vals):
        if vals.get('country_code'):
            ids = self.models.execute_kw(self.db, self.uid, self.password, 'res.country', 'search', [[['code', '=', vals['country_code']]]])
            if not ids: ids = self.models.execute_kw(self.db, self.uid, self.password, 'res.country', 'search', [[['name', 'ilike', vals['country_code']]]])
            if ids: vals['country_id'] = ids[0]
            del vals['country_code']

    def search_product_by_sku(self, sku, company_id=None):
        domain = [['default_code', '=', sku], ['active', '=', True]]
        if company_id: domain.extend(['|', ['company_id', '=', int(company_id)], ['company_id', '=', False]])
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])
        return ids[0] if ids else None

    def check_product_exists_by_sku(self, sku, company_id=None):
        domain = [['default_code', '=', sku], '|', ['active', '=', True], ['active', '=', False]]
        if company_id: domain.extend(['|', ['company_id', '=', int(company_id)], ['company_id', '=', False]])
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])
        return ids[0] if ids else None

    def search_product_by_name(self, name, company_id=None):
        domain = [['name', 'ilike', name], ['active', '=', True]]
        if company_id: domain.extend(['|', ['company_id', '=', int(company_id)], ['company_id', '=', False]])
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])
        return ids[0] if ids else None

    def create_service_product(self, name, company_id=None):
        vals = {'name': name, 'type': 'service', 'invoice_policy': 'order', 'list_price': 0.0, 'sale_ok': True, 'purchase_ok': False}
        if company_id: vals['company_id'] = int(company_id)
        return self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'create', [vals])

    def create_product(self, vals):
        if 'type' not in vals: vals['type'] = 'product'
        if 'invoice_policy' not in vals: vals['invoice_policy'] = 'delivery'
        return self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'create', [vals])

    def get_vendor_product_code(self, product_id):
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'product.supplierinfo', 'search', [[['product_tmpl_id', '=', product_id]]])
        if ids:
            data = self.models.execute_kw(self.db, self.uid, self.password, 'product.supplierinfo', 'read', [ids[0]], {'fields': ['product_code']})
            if data and data[0].get('product_code'): return data[0]['product_code']
        return None

    def get_vendor_name(self, product_id):
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'product.supplierinfo', 'search', [[['product_tmpl_id', '=', product_id]]], {'limit': 1})
        if ids:
            data = self.models.execute_kw(self.db, self.uid, self.password, 'product.supplierinfo', 'read', [ids[0]], {'fields': ['partner_id']})
            if data and data[0].get('partner_id'): return data[0]['partner_id'][1]
        return None

    def get_public_category_name(self, category_ids):
        if not category_ids: return None
        data = self.models.execute_kw(self.db, self.uid, self.password, 'product.public.category', 'read', [category_ids[0]], {'fields': ['name']})
        return data[0]['name'] if data else None

    def get_tag_names(self, tag_ids):
        if not tag_ids: return []
        data = self.models.execute_kw(self.db, self.uid, self.password, 'product.tag', 'read', [tag_ids], {'fields': ['name']})
        return [t['name'] for t in data]

    def get_product_image(self, product_id):
        data = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'read', [product_id], {'fields': ['image_1920']})
        return data[0]['image_1920'] if data and data[0].get('image_1920') else None

    def get_changed_products(self, time_limit_str, company_id=None):
        # FIX: Added 'sale_ok' filter to prevent junk from incremental syncs
        domain = [
            ('write_date', '>', time_limit_str), 
            ('sale_ok', '=', True), 
            ('type', 'in', ['product', 'consu']),
            '|', ('active', '=', True), ('active', '=', False)
        ]
        
        if company_id:
            domain.append(('company_id', '=', int(company_id)))
            
        return self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])

    def get_changed_customers(self, time_limit_str, company_id=None):
        domain = [('write_date', '>', time_limit_str), ('is_company', '=', True), ('customer_rank', '>', 0), ('active', '=', True)]
        if company_id: domain = ['&', '&', '&', ('write_date', '>', time_limit_str), ('is_company', '=', True), ('customer_rank', '>', 0), '|', ('company_id', '=', int(company_id)), ('company_id', '=', False)]
        fields = ['id', 'name', 'email', 'phone', 'street', 'city', 'zip', 'country_id', 'vat', 'category_id', 'user_id']
        return self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'search_read', [domain], {'fields': fields})

    def get_product_ids_with_recent_stock_moves(self, time_limit_str, company_id=None):
        domain = [['date', '>', time_limit_str], ['state', '=', 'done']]
        if company_id: domain.append(['company_id', '=', int(company_id)])
        move_ids = self.models.execute_kw(self.db, self.uid, self.password, 'stock.move', 'search', [domain])
        if not move_ids: return []
        moves = self.models.execute_kw(self.db, self.uid, self.password, 'stock.move', 'read', [move_ids], {'fields': ['product_id']})
        product_ids = set()
        for m in moves:
            if m.get('product_id'): product_ids.add(m['product_id'][0])
        return list(product_ids)

    def get_companies(self):
        """Fetches all allowed companies for the user."""
        try:
            # Fetch companies the user has access to
            # We use 'res.company' search with empty domain to get all allowed for user context
            ids = self.models.execute_kw(self.db, self.uid, self.password,
                'res.company', 'search', [[]])
            
            if not ids: return []
            
            companies = self.models.execute_kw(self.db, self.uid, self.password,
                'res.company', 'read', [ids], {'fields': ['id', 'name']})
            return companies
        except Exception as e:
            print(f"Odoo Get Companies Error: {e}")
            return []

    def get_locations(self, company_id=None):
        """Fetches stock locations, forcing the specific company context."""
        try:
            # 1. Base Domain
            # We removed ['usage', '=', 'internal'] to ensure we see ALL locations first.
            # You can add it back later if the list is too messy.
            domain = []
            
            # 2. Context setup (Critical for Multi-Company)
            kwargs = {}
            
            if company_id:
                cid = int(company_id)
                # Filter: Locations for this company OR Shared locations (False)
                domain.append('|')
                domain.append(['company_id', '=', cid])
                domain.append(['company_id', '=', False])
                
                # TELL ODOO TO SWITCH COMPANIES FOR THIS REQUEST
                kwargs['context'] = {'allowed_company_ids': [cid]}
            
            # 3. Execute Search with Context
            ids = self.models.execute_kw(self.db, self.uid, self.password,
                'stock.location', 'search', [domain], kwargs)
            
            if not ids: return []
            
            # 4. Read Names
            locs = self.models.execute_kw(self.db, self.uid, self.password,
                'stock.location', 'read', [ids], {'fields': ['id', 'display_name', 'company_id']}, kwargs)
            
            return [{'id': l['id'], 'name': l['display_name']} for l in locs]
            
        except Exception as e:
            print(f"Odoo Get Locations Error: {e}")
            return []

    def get_total_qty_for_locations(self, product_id, location_ids, field_name='qty_available'):
        total_qty = 0
        for loc_id in location_ids:
            context = {'location': loc_id}
            data = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'read', [product_id], {'fields': [field_name], 'context': context})
            if data: total_qty += data[0].get(field_name, 0)
        return total_qty

    def create_sale_order(self, order_vals, context=None):
        kwargs = {}
        if context: kwargs['context'] = context
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'create', [order_vals], kwargs)

    def update_sale_order(self, order_id, order_vals):
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'write', [[order_id], order_vals])

    def post_message(self, order_id, message):
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'message_post', [order_id], {'body': message})

    def cancel_order(self, order_id):
        try:
            self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'action_cancel', [[order_id]])
            return True
        except Exception as e:
            print(f"Odoo Cancel Error: {e}")
            return False

    def get_recently_cancelled_orders(self, time_limit_str, company_id=None):
        domain = [['write_date', '>', time_limit_str], ['state', '=', 'cancel'], ['client_order_ref', 'like', 'ONLINE_']]
        if company_id: domain.append(['company_id', '=', int(company_id)])
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'search_read', [domain], {'fields': ['id', 'client_order_ref']})

    def get_all_products(self, company_id=None):
        # FIX: 'sale_ok=True' is MANDATORY to stop junk products
        domain = [
            ['sale_ok', '=', True], 
            ['type', 'in', ['product', 'consu']], 
            '|', ['active', '=', True], ['active', '=', False]
        ]
        
        if company_id:
             domain.append(['company_id', '=', int(company_id)])
        
        # ADDED 'qty_per_pack' to fields so we can use it in the repair tool
        fields = ['id', 'name', 'default_code', 'list_price', 'standard_price', 'weight', 
                  'description_sale', 'active', 'product_tmpl_id', 'qty_available', 
                  'public_categ_ids', 'product_tag_ids', 'uom_id', 'sh_is_secondary_unit', 
                  'sh_secondary_uom', 'write_date', 'sale_ok', 'barcode', 'qty_per_pack']
                  
        return self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search_read', [domain], {'fields': fields})

    def get_product_split_info(self, product_id, product_data=None):
        try:
            if product_data:
                is_sec = product_data.get('sh_is_secondary_unit', False)
                uom_id = product_data.get('uom_id', False)
            else:
                p_data = self.models.execute_kw(self.db, self.uid, self.password,
                    'product.product', 'read', [product_id], {'fields': ['uom_id', 'sh_is_secondary_unit']})
                if not p_data: return None
                is_sec = p_data[0].get('sh_is_secondary_unit', False)
                uom_id = p_data[0].get('uom_id', False)

            if not is_sec or not uom_id:
                return None 

            real_uom_id = uom_id[0]
            uom_data = self.models.execute_kw(self.db, self.uid, self.password,
                'uom.uom', 'read', [real_uom_id], {'fields': ['name', 'factor_inv']})
            
            if uom_data:
                ratio = float(uom_data[0].get('factor_inv', 1.0))
                return {'ratio': ratio, 'uom_name': uom_data[0]['name']}
                
        except Exception as e:
            print(f"Split Info Error: {e}")
        return None
