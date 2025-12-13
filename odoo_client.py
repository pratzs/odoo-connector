import xmlrpc.client
import ssl

class OdooClient:
    def __init__(self, url, db, username, password):
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        self.context = ssl._create_unverified_context()
        
        # Enable allow_none to handle empty Shopify fields without crashing
        self.common = xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/common', context=self.context, allow_none=True)
        self.uid = self.common.authenticate(self.db, self.username, self.password, {})
        
        # IMPORTANT: self.models is NOT assigned here anymore because it is a @property below.
        # This prevents the "property 'models' has no setter" error.

    @property
    def models(self):
        """
        Creates a fresh ServerProxy for every call. 
        This prevents 'ResponseNotReady' errors in multi-threaded environments (Gunicorn/Flask).
        """
        return xmlrpc.client.ServerProxy(f'{self.url}/xmlrpc/2/object', context=self.context, allow_none=True)

    def search_partner_by_email(self, email):
        # Strictly Active
        ids = self.models.execute_kw(self.db, self.uid, self.password,
            'res.partner', 'search', [[['email', '=', email], ['active', '=', True]]])
        if ids:
            partners = self.models.execute_kw(self.db, self.uid, self.password,
                'res.partner', 'read', [ids], {'fields': ['id', 'name', 'parent_id', 'user_id', 'category_id']})
            return partners[0]
        return None

    def get_partner_salesperson(self, partner_id):
        data = self.models.execute_kw(self.db, self.uid, self.password,
            'res.partner', 'read', [[partner_id]], {'fields': ['user_id']})
        if data and data[0].get('user_id'):
            return data[0]['user_id'][0] 
        return None

    def create_partner(self, vals):
        self._resolve_country(vals)
        return self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'create', [vals])

    def find_or_create_child_address(self, parent_id, address_data, type='delivery'):
        domain = [
            ['parent_id', '=', parent_id],
            ['type', '=', type],
            ['street', '=', address_data.get('street')],
            ['active', '=', True]
        ]
        existing_ids = self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'search', [domain])

        if existing_ids:
            return existing_ids[0]
        
        vals = {
            'parent_id': parent_id,
            'type': type,
            'name': address_data.get('name') or "Delivery Address",
            'street': address_data.get('street'),
            'city': address_data.get('city'),
            'zip': address_data.get('zip'),
            'country_code': address_data.get('country_code'),
            'phone': address_data.get('phone'),
            'email': address_data.get('email')
        }
        
        self._resolve_country(vals)
        return self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'create', [vals])

    def _resolve_country(self, vals):
        code = vals.get('country_code')
        if code:
            ids = self.models.execute_kw(self.db, self.uid, self.password, 'res.country', 'search', [[['code', '=', code]]])
            if not ids:
                 ids = self.models.execute_kw(self.db, self.uid, self.password, 'res.country', 'search', [[['name', 'ilike', code]]])
            if ids:
                vals['country_id'] = ids[0]
            del vals['country_code']

    def search_product_by_sku(self, sku, company_id=None):
        domain = [['default_code', '=', sku], ['active', '=', True]]
        if company_id:
            domain.append('|')
            domain.append(['company_id', '=', int(company_id)])
            domain.append(['company_id', '=', False])
            
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])
        return ids[0] if ids else None

    def check_product_exists_by_sku(self, sku, company_id=None):
        domain = [['default_code', '=', sku], '|', ['active', '=', True], ['active', '=', False]]
        if company_id:
            domain.append('|')
            domain.append(['company_id', '=', int(company_id)])
            domain.append(['company_id', '=', False])
            
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])
        return ids[0] if ids else None

    def search_product_by_name(self, name, company_id=None):
        domain = [['name', 'ilike', name], ['active', '=', True]]
        if company_id:
            domain.append('|')
            domain.append(['company_id', '=', int(company_id)])
            domain.append(['company_id', '=', False])
            
        ids = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])
        return ids[0] if ids else None

    def create_service_product(self, name, company_id=None):
        vals = {
            'name': name, 'type': 'service', 'invoice_policy': 'order', 
            'list_price': 0.0, 'sale_ok': True, 'purchase_ok': False
        }
        if company_id: vals['company_id'] = int(company_id)
        return self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'create', [vals])

    def create_product(self, vals):
        if 'type' not in vals:
            vals['type'] = 'product'
        if 'invoice_policy' not in vals:
            vals['invoice_policy'] = 'delivery'
        return self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'create', [vals])

    def get_vendor_product_code(self, product_id):
        ids = self.models.execute_kw(self.db, self.uid, self.password, 
            'product.supplierinfo', 'search', [[['product_tmpl_id', '=', product_id]]])
            
        if ids:
            data = self.models.execute_kw(self.db, self.uid, self.password, 
                'product.supplierinfo', 'read', [ids[0]], {'fields': ['product_code']})
            if data and data[0].get('product_code'):
                return data[0]['product_code']
        return None

    def get_vendor_name(self, product_id):
        """Fetches the primary vendor name for a product template."""
        ids = self.models.execute_kw(self.db, self.uid, self.password, 
            'product.supplierinfo', 'search', [[['product_tmpl_id', '=', product_id]]], {'limit': 1})
        if ids:
            data = self.models.execute_kw(self.db, self.uid, self.password, 
                'product.supplierinfo', 'read', [ids[0]], {'fields': ['partner_id']})
            # partner_id is (id, name)
            if data and data[0].get('partner_id'):
                return data[0]['partner_id'][1]
        return None

    def get_public_category_name(self, category_ids):
        """Fetches the name of the first public category (Ecommerce category)."""
        if not category_ids: return None
        # category_ids is a list of IDs. We just take the first one.
        data = self.models.execute_kw(self.db, self.uid, self.password,
            'product.public.category', 'read', [category_ids[0]], {'fields': ['name']})
        if data:
            return data[0]['name']
        return None

    def get_tag_names(self, tag_ids):
        """Fetches the names of product tags."""
        if not tag_ids: return []
        data = self.models.execute_kw(self.db, self.uid, self.password,
            'product.tag', 'read', [tag_ids], {'fields': ['name']})
        return [t['name'] for t in data]

    def get_product_image(self, product_id):
        """Fetches the base64 image_1920 for a specific product."""
        data = self.models.execute_kw(self.db, self.uid, self.password,
            'product.product', 'read', [product_id], {'fields': ['image_1920']})
        if data and data[0].get('image_1920'):
            return data[0]['image_1920']
        return None

    def get_all_products(self, company_id=None):
        domain = [('type', '=', 'product'), ('default_code', '!=', False), '|', ('active', '=', True), ('active', '=', False)]
        if company_id:
            domain = [
                '&', '&', '&',
                ('type', '=', 'product'),
                ('default_code', '!=', False),
                '|', ('active', '=', True), ('active', '=', False),
                '|', ('company_id', '=', int(company_id)), ('company_id', '=', False)
            ]
        
        # Added 'qty_available', 'public_categ_ids', and 'product_tag_ids' to support new mappings
        fields = ['id', 'name', 'default_code', 'list_price', 'standard_price', 'weight', 'description_sale', 'active', 'product_tmpl_id', 'qty_available', 'public_categ_ids', 'product_tag_ids']
        return self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search_read', [domain], {'fields': fields})

    def get_changed_products(self, time_limit_str, company_id=None):
        domain = [('write_date', '>', time_limit_str), ('type', '=', 'product'), '|', ('active', '=', True), ('active', '=', False)]
        if company_id:
            domain = [
                '&', '&', '&',
                ('write_date', '>', time_limit_str), 
                ('type', '=', 'product'),
                '|', ('active', '=', True), ('active', '=', False),
                '|', 
                ('company_id', '=', int(company_id)), 
                ('company_id', '=', False)
            ]
            
        return self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'search', [domain])

    def get_changed_customers(self, time_limit_str, company_id=None):
        domain = [('write_date', '>', time_limit_str), ('is_company', '=', True), ('customer', '=', True), ('active', '=', True)]
        if company_id:
            domain = [
                '&', '&', '&',
                ('write_date', '>', time_limit_str), 
                ('is_company', '=', True),
                ('customer', '=', True),
                '|', 
                ('company_id', '=', int(company_id)), 
                ('company_id', '=', False)
            ]
        
        # ADDED 'user_id' to this list to fetch Salesperson
        fields = ['id', 'name', 'email', 'phone', 'street', 'city', 'zip', 'country_id', 'vat', 'category_id', 'user_id']
        return self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'search_read', [domain], {'fields': fields})



    def get_product_ids_with_recent_stock_moves(self, time_limit_str, company_id=None):
        """
        Finds products that have moved (Sold, Purchased, Adjusted) recently.
        This is MUCH more reliable for Inventory Sync than checking product write_date.
        """
        # Search stock.move for items processed ('done') since the last check
        domain = [['date', '>', time_limit_str], ['state', '=', 'done']]
        if company_id:
            domain.append(['company_id', '=', int(company_id)])
            
        move_ids = self.models.execute_kw(self.db, self.uid, self.password, 'stock.move', 'search', [domain])
        
        if not move_ids: return []
        
        # Read the moves to get the product_ids
        moves = self.models.execute_kw(self.db, self.uid, self.password, 'stock.move', 'read', [move_ids], {'fields': ['product_id']})
        
        # Extract unique IDs (product_id is returned as [id, "Name"])
        product_ids = set()
        for m in moves:
            if m.get('product_id'):
                product_ids.add(m['product_id'][0])
                
        return list(product_ids)


    def get_companies(self):
        return self.models.execute_kw(self.db, self.uid, self.password, 'res.company', 'search_read', [[]], {'fields': ['id', 'name']})

    def get_locations(self, company_id=None):
        if not company_id: return []
        domain = [['usage', '=', 'internal'], ['company_id', '=', int(company_id)]]
        return self.models.execute_kw(self.db, self.uid, self.password, 'stock.location', 'search_read', [domain], {'fields': ['id', 'complete_name', 'company_id']})

    def get_total_qty_for_locations(self, product_id, location_ids, field_name='qty_available'):
        total_qty = 0
        for loc_id in location_ids:
            context = {'location': loc_id}
            data = self.models.execute_kw(self.db, self.uid, self.password,
                'product.product', 'read', [product_id],
                {'fields': [field_name], 'context': context})
            if data: total_qty += data[0].get(field_name, 0)
        return total_qty

    def create_sale_order(self, order_vals, context=None):
        kwargs = {}
        if context:
            kwargs['context'] = context
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'create', [order_vals], kwargs)

    def update_sale_order(self, order_id, order_vals):
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'write', [[order_id], order_vals])

    def post_message(self, order_id, message):
        return self.models.execute_kw(self.db, self.uid, self.password, 'sale.order', 'message_post', [order_id], {'body': message})
