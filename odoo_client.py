import xmlrpc.client
import ssl
import requests
import urllib3
import io
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- SILENCE SSL WARNINGS (ROBUST) ---
# Disable for standard urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# Disable for requests' internal urllib3 (just in case)
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --- OPTIMIZATION: Persistent Transport (Keep-Alive) ---
class RequestsTransport(xmlrpc.client.Transport):
    """
    Custom XML-RPC Transport using 'requests' to enable HTTP Keep-Alive.
    """
    def __init__(self, use_https=True, verify=False):
        # Initialize parent without arguments
        super().__init__()
        
        self._use_https = use_https
        self.verify = verify
        self.verbose = False  # <--- FIX: Initialize verbose to prevent AttributeError
        self.session = requests.Session()
        
        # Retry strategy for network blips
        retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503, 504])
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        
        self.session.headers.update({
            'Content-Type': 'text/xml',
            'User-Agent': 'OdooShopifyConnector/6.0 (Optimized)'
        })

    def request(self, host, handler, request_body, verbose=False):
        scheme = "https" if self._use_https else "http"
        url = f"{scheme}://{host}{handler}"
        try:
            resp = self.session.post(
                url, 
                data=request_body, 
                headers={'Content-Type': 'text/xml'},
                verify=self.verify, 
                timeout=300
            )
            resp.raise_for_status()
            
            # FIX: Wrap bytes in BytesIO so xmlrpc can .read() it
            return self.parse_response(io.BytesIO(resp.content))
            
        except requests.RequestException as e:
            if hasattr(e.response, 'content') and e.response.content:
                # FIX: Wrap error response in BytesIO too
                return self.parse_response(io.BytesIO(e.response.content))
            raise xmlrpc.client.ProtocolError(url, e.response.status_code if e.response else 500, str(e), {})

# ---------------------------------------------------------

class OdooClient:
    def __init__(self, url, db, username, password):
        self.url = url.rstrip('/') # Safety fix for trailing slashes
        self.db = db
        self.username = username
        self.password = password
        
        # Use Custom Transport (disables SSL verify to match your original _create_unverified_context)
        self.transport = RequestsTransport(use_https=self.url.startswith("https"), verify=False)
        
        # 1. Connect to Common (Auth)
        self.common = xmlrpc.client.ServerProxy(
            f'{self.url}/xmlrpc/2/common', 
            transport=self.transport, 
            allow_none=True
        )
        
        # Authenticate
        self.uid = self.common.authenticate(self.db, self.username, self.password, {})
        
        # Check if login actually succeeded
        if not self.uid:
            raise Exception(f"Odoo Login Failed! Check credentials for {self.username}")

        # 2. Connect to Object (Data) - Persisted once
        self.models = xmlrpc.client.ServerProxy(
            f'{self.url}/xmlrpc/2/object', 
            transport=self.transport, 
            allow_none=True
        )

    # --- HELPER TO REDUCE BOILERPLATE ---
    def execute(self, model, method, *args, **kwargs):
        """Wrapper to call execute_kw cleaner."""
        return self.models.execute_kw(self.db, self.uid, self.password, model, method, args, kwargs)

    # --- PARTNER METHODS ---

    def search_partner_by_email(self, email):
        # OPTIMIZATION: Use search_read (1 call) instead of search + read (2 calls)
        domain = ['|', ['active', '=', True], ['active', '=', False], ['email', '=', email]]
        fields = ['id', 'name', 'active', 'parent_id', 'user_id', 'category_id', 'vat', 'phone', 'street', 'city', 'zip', 'country_id']
        
        partners = self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'search_read', [domain], {'fields': fields, 'limit': 1})
        
        if partners:
            p = partners[0]
            if not p.get('active'):
                self.models.execute_kw(self.db, self.uid, self.password, 'res.partner', 'write', [[p['id']], {'active': True}])
            return p
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

    # --- PRODUCT METHODS ---

    def search_product_by_sku(self, sku, company_id=None):
        domain = [
            ['default_code', '=', sku], 
            ['active', '=', True]
        ]
        if company_id:
            # Correctly structure the OR condition for Company
            domain.append('|')
            domain.append(['company_id', '=', int(company_id)])
            domain.append(['company_id', '=', False])
            
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
        # Optimization: search_read
        data = self.models.execute_kw(self.db, self.uid, self.password, 'product.supplierinfo', 'search_read', 
                                      [[['product_tmpl_id', '=', product_id]]], {'fields': ['product_code'], 'limit': 1})
        if data and data[0].get('product_code'): return data[0]['product_code']
        return None

    def get_vendor_name(self, product_id):
        # Optimization: search_read
        data = self.models.execute_kw(self.db, self.uid, self.password, 'product.supplierinfo', 'search_read', 
                                      [[['product_tmpl_id', '=', product_id]]], {'fields': ['partner_id'], 'limit': 1})
        if data and data[0].get('partner_id'): return data[0]['partner_id'][1]
        return None

    def get_public_category_name(self, category_ids):
        if not category_ids: return None
        data = self.models.execute_kw(self.db, self.uid, self.password, 'product.public.category', 'read', [category_ids[0]], {'fields': ['name']})
        return data[0]['name'] if data else None

    def get_tag_names(self, tag_ids, model='res.partner.category'):
    """
    Returns tag names for the given IDs.
    Defaults to res.partner.category (customer tags).
    Pass model='product.tag' for product tags.
    """
    if not tag_ids: return []
    try:
        data = self.models.execute_kw(self.db, self.uid, self.password,
            model, 'read', [tag_ids], {'fields': ['name']})
        return [t['name'] for t in data]
    except Exception as e:
        print(f"Tag fetch error ({model}): {e}")
        return []

    def get_product_image(self, product_id):
        data = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'read', [product_id], {'fields': ['image_1920']})
        return data[0]['image_1920'] if data and data[0].get('image_1920') else None

    def get_changed_products(self, time_limit_str, company_id=None):
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
        domain = [
            ('write_date', '>', time_limit_str), 
            ('active', '=', True),
            ('email', '!=', False)
        ]
        if company_id:
             domain.append('|')
             domain.append(('company_id', '=', int(company_id)))
             domain.append(('company_id', '=', False))
        fields = ['id', 'name', 'email', 'phone', 'street', 'city', 'zip', 'country_id', 'vat', 'category_id', 'user_id', 'is_company', 'parent_id']
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
        try:
            ids = self.models.execute_kw(self.db, self.uid, self.password, 'res.company', 'search', [[]])
            if not ids: return []
            return self.models.execute_kw(self.db, self.uid, self.password, 'res.company', 'read', [ids], {'fields': ['id', 'name']})
        except Exception as e:
            print(f"Odoo Get Companies Error: {e}")
            return []

    def get_locations(self, company_id=None):
        try:
            domain = []
            context_dict = {}
            if company_id:
                cid = int(company_id)
                domain.append('|')
                domain.append(['company_id', '=', cid])
                domain.append(['company_id', '=', False])
                context_dict = {'allowed_company_ids': [cid], 'company_id': cid}

            fields = ['id', 'display_name', 'company_id', 'usage']
            kw_args = {'fields': fields, 'limit': 4000}
            if context_dict: kw_args['context'] = context_dict

            locs = self.models.execute_kw(self.db, self.uid, self.password, 'stock.location', 'search_read', [domain], kw_args)
            return [{'id': l['id'], 'name': f"{l['display_name']} [{l.get('usage', 'unknown')}]"} for l in locs]
        except Exception as e:
            return [{'id': 0, 'name': f"Error: {str(e)}"}]
            

    def get_stock_batch(self, product_ids, location_ids, field_name='qty_available'):
        """
        MASSIVE OPTIMIZATION: 
        1. If asking for 'qty_available' (On Hand), we query 'stock.quant' directly using read_group.
           This is 50x faster as it sums up all locations in 1 API call.
        2. If asking for 'virtual_available' (Forecasted), we must fall back to the slower loop method.
        """
        if not product_ids or not location_ids: return {}

        # FAST PATH: Stock Quants (Physical On Hand)
        if field_name == 'qty_available':
            try:
                # Query stock.quant table directly: "Sum of quantity for these products in these locations"
                domain = [
                    ('product_id', 'in', product_ids),
                    ('location_id', 'in', location_ids)
                ]
                # read_group returns aggregated sums
                groups = self.models.execute_kw(
                    self.db, self.uid, self.password, 
                    'stock.quant', 'read_group',
                    [domain], 
                    ['product_id', 'quantity'], # Fields to read
                    ['product_id']              # Group By
                )
                
                totals = {pid: 0 for pid in product_ids}
                for g in groups:
                    if g.get('product_id'):
                        pid = g['product_id'][0] 
                        totals[pid] = g.get('quantity', 0)
                return totals
            except Exception as e:
                print(f"Fast Stock Path Failed ({e}), falling back to slow path...")

        # SLOW PATH: Forecasted Quantity (or if fast path fails)
        # We must ask the product model to compute it based on context
        totals = {pid: 0 for pid in product_ids}
        for loc_id in location_ids:
            context = {'location': loc_id}
            try:
                data = self.models.execute_kw(
                    self.db, self.uid, self.password, 
                    'product.product', 'read', 
                    [product_ids], 
                    {'fields': [field_name], 'context': context}
                )
                for record in data:
                    pid = record['id']
                    totals[pid] += record.get(field_name, 0)
            except: pass
                
        return totals

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
        domain = [
            ['sale_ok', '=', True], 
            ['type', 'in', ['product', 'consu']], 
            '|', ['active', '=', True], ['active', '=', False]
        ]
        if company_id: domain.append(['company_id', '=', int(company_id)])
        
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
                p_data = self.models.execute_kw(self.db, self.uid, self.password, 'product.product', 'read', [product_id], {'fields': ['uom_id', 'sh_is_secondary_unit']})
                if not p_data: return None
                is_sec = p_data[0].get('sh_is_secondary_unit', False)
                uom_id = p_data[0].get('uom_id', False)

            if not is_sec or not uom_id: return None 

            real_uom_id = uom_id[0]
            uom_data = self.models.execute_kw(self.db, self.uid, self.password, 'uom.uom', 'read', [real_uom_id], {'fields': ['name', 'factor_inv']})
            if uom_data:
                ratio = float(uom_data[0].get('factor_inv', 1.0))
                return {'ratio': ratio, 'uom_name': uom_data[0]['name']}
        except Exception as e:
            print(f"Split Info Error: {e}")
        return None
