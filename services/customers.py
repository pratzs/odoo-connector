import shopify
import json
from datetime import datetime
from models import db, Shop, CustomerMap
from utils import get_config, log_event, set_config

# --- GRAPHQL HELPERS FOR B2B ---
def execute_graphql(query, variables=None, shop_url=None):
    # Note: Session setup must be done by caller
    try:
        client = shopify.GraphQL()
        result = client.execute(query, variables)
        return json.loads(result)
    except Exception as e:
        print(f"GraphQL Error: {e}")
        return None

def ensure_shopify_company(name, shop_url):
    query_find = """
    query($query: String!) {
      companies(first: 1, query: $query) {
        edges { node { id name } }
      }
    }
    """
    res = execute_graphql(query_find, {'query': f"name:'{name}'"}, shop_url)
    edges = res.get('data', {}).get('companies', {}).get('edges', [])
    if edges: return edges[0]['node']['id']

    mutation_create = """
    mutation($input: CompanyInput!) {
      companyCreate(input: $input) {
        company { id }
      }
    }
    """
    res = execute_graphql(mutation_create, {'input': {'name': name}}, shop_url)
    company = res.get('data', {}).get('companyCreate', {}).get('company', {})
    if company: return company['id']
    return None

def ensure_company_location(company_id, location_name, address_data, shop_url):
    query_loc = """
    query($companyId: ID!) {
      company(id: $companyId) {
        locations(first: 50) {
          edges { node { id name } }
        }
      }
    }
    """
    res = execute_graphql(query_loc, {'companyId': company_id}, shop_url)
    locations = res.get('data', {}).get('company', {}).get('locations', {}).get('edges', [])
    for loc in locations:
        if loc['node']['name'] == location_name:
            return loc['node']['id']

    mutation_create = """
    mutation($companyId: ID!, $input: CompanyLocationInput!) {
      companyLocationCreate(companyId: $companyId, input: $input) {
        companyLocation { id }
      }
    }
    """
    input_data = {
        'name': location_name,
        'shippingAddress': {
            'address1': address_data.get('address1', ''),
            'city': address_data.get('city', ''),
            'zip': address_data.get('zip', ''),
            'countryCode': address_data.get('country_code', 'NZ')
        }
    }
    res = execute_graphql(mutation_create, {'companyId': company_id, 'input': input_data}, shop_url)
    loc = res.get('data', {}).get('companyLocationCreate', {}).get('companyLocation', {})
    if loc: return loc['id']
    return None

def assign_customer_to_company(company_id, location_id, customer_id, shop_url):
    if not customer_id.startswith('gid://'):
        customer_id = f"gid://shopify/Customer/{customer_id}"

    mutation_assign = """
    mutation($companyId: ID!, $customerId: ID!) {
      companyContactCreate(
        companyId: $companyId, 
        input: { customer: {id: $customerId} }
      ) {
        companyContact { id }
      }
    }
    """
    res = execute_graphql(mutation_assign, {'companyId': company_id, 'customerId': customer_id}, shop_url)
    contact = res.get('data', {}).get('companyContactCreate', {}).get('companyContact', {})
    
    contact_id = None
    if contact: contact_id = contact['id']
    
    if contact_id:
        mutation_role = """
        mutation($companyContactId: ID!, $locationId: ID!) {
          companyContactRoleAssign(
            companyContactId: $companyContactId,
            rolesToAssign: [{name: "location_admin", companyLocationId: $locationId}]
          ) {
            userErrors { field message }
          }
        }
        """
        execute_graphql(mutation_role, {'companyContactId': contact_id, 'locationId': location_id}, shop_url)


def sync_customers_master(shop_url):
    """
    Odoo -> Shopify Customer Sync (Paginated Master).
    """
    # 1. IMPORT APP FOR CONTEXT
    from app import app, get_odoo_connection, setup_shopify_session

    # 2. WRAP EVERYTHING IN CONTEXT
    with app.app_context():
        odoo = get_odoo_connection(shop_url) 
        if not odoo or not setup_shopify_session(shop_url): 
            log_event('System', 'Error', "Customer Sync Failed: Connection Error", shop_url=shop_url)
            return

        # 3. Configuration
        direction = get_config('cust_direction', 'bidirectional', shop_url=shop_url)
        if direction == 'shopify_to_odoo': return

        company_id = get_config('odoo_company_id', shop_url=shop_url)
        
        w_tag = get_config('cust_whitelist_tags', '', shop_url=shop_url)
        b_tag = get_config('cust_blacklist_tags', '', shop_url=shop_url)
        whitelist = [t.strip() for t in w_tag.split(',') if t.strip()]
        blacklist = [t.strip() for t in b_tag.split(',') if t.strip()]
        use_tags_filter = get_config('cust_sync_tags', False, shop_url=shop_url)
        sync_vat = get_config('cust_sync_vat', True, shop_url=shop_url)
        sync_salesrep = get_config('cust_sync_salesrep', True, shop_url=shop_url)
        raw_groups = get_config('group_companies_list', '', shop_url=shop_url)
        group_whitelist = [g.strip().lower() for g in raw_groups.split(',') if g.strip()]

        # 4. TIMESTAMP & DOMAIN
        last_run = get_config('last_customer_sync_time', '2000-01-01 00:00:00', shop_url=shop_url)
        current_run_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        domain = [
            ('write_date', '>', last_run), 
            ('active', '=', True),
            ('email', '!=', False),  # FIX: Missing comma was causing a TypeError crash
            ('type', '=', 'contact')
        ]
        if company_id:
             domain.append('|')
             domain.append(('company_id', '=', int(company_id)))
             domain.append(('company_id', '=', False))
        
        fields = ['id', 'name', 'email', 'phone', 'street', 'city', 'zip', 'country_id', 'vat', 'category_id', 'user_id', 'is_company', 'parent_id']

        # 5. PAGINATION LOOP
        limit = 1000
        offset = 0
        synced_count = 0
        total_found = 0

        try:
            count_check = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'res.partner', 'search_count', [domain])
            log_event('Customer Sync', 'Info', f"Found {count_check} changed customers. Starting Paginated Sync...", shop_url=shop_url)
        except: pass

        while True:
            try:
                odoo_customers = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                    'res.partner', 'search_read', [domain], 
                    {'fields': fields, 'limit': limit, 'offset': offset}
                )
            except Exception as e:
                log_event('Customer Sync', 'Error', f"Odoo Fetch Failed at offset {offset}: {e}", shop_url=shop_url)
                return

            if not odoo_customers: break 

            for p in odoo_customers:
                parent_info = p.get('parent_id')

                # Skip the group parent company itself (e.g. "CSB Group 3 Ltd")
                is_group_parent = any(g in p.get('name', '').lower() for g in group_whitelist)
                if is_group_parent and not parent_info:
                    continue

                if parent_info:
                    parent_name = parent_info[1]
                    is_whitelisted = any(g in parent_name.lower() for g in group_whitelist)
                    if not is_whitelisted: continue

                # --- 1. NAMES ---
                shopify_first_name = p.get('name')
                shopify_last_name = "" 
                shopify_company = p.get('name')
                
                b2b_company_name = p.get('name')
                # FIX: Append Odoo ID to location name so it's a stable unique key.
                # Without this, any rename in Odoo silently creates a duplicate
                # Shopify Company Location on the next sync run.
                b2b_location_name = f"{p.get('name')} [OID:{p['id']}]"
                staff_note = f"Independent | Odoo ID: {p['id']}"
                context_tags = ["Independent"]

                if parent_info:
                    parent_name = parent_info[1]
                    shopify_first_name = p.get('name') 
                    shopify_company = parent_name        
                    b2b_company_name = parent_name
                    b2b_location_name = f"{p.get('name')} [OID:{p['id']}]"
                    staff_note = f"Franchise Site | Odoo ID: {p['id']}"
                    context_tags = ["Franchise", "Site"]

                raw_email = p.get('email')
                if raw_email and "@" in raw_email: email = raw_email
                else: email = f"no-email-{p['id']}@pos.local"

                odoo_tags = odoo.get_tag_names(p.get('category_id', []))
                if use_tags_filter:
                    if blacklist and any(t in odoo_tags for t in blacklist): continue
                    if whitelist and not any(t in odoo_tags for t in whitelist): continue

                try:
                    shopify_cust = shopify.Customer.search(query=f"email:{email}")
                    if shopify_cust: c = shopify_cust[0]
                    else: c = shopify.Customer(); c.email = email

                    c.tax_exempt = False 
                    c.first_name = shopify_first_name
                    c.last_name = shopify_last_name
                    c.note = staff_note
                    c.phone = (p.get('phone') or p.get('mobile') or '').strip()
                    c.verified_email = True
                    
                    address_data = {
                        'address1': p.get('street') or '', 'city': p.get('city') or '', 'zip': p.get('zip') or '',
                        'country_code': p.get('country_id')[1] if p.get('country_id') else 'NZ', 
                        'company': shopify_company, 'phone': c.phone,
                        'first_name': c.first_name, 'last_name': c.last_name, 'default': True
                    }
                    c.addresses = [shopify.Address(address_data)]
                    
                    tags_str = getattr(c, 'tags', '')
                    current_shopify_tags = [t.strip() for t in tags_str.split(',')] if tags_str else []
                    final_tag_list = list(set(current_shopify_tags + odoo_tags + context_tags))
                    c.tags = ",".join(final_tag_list)

                    metafields_to_save = []
                    vat = p.get('vat')
                    if vat and sync_vat: 
                        c.note = f"{c.note}\nVAT: {vat}"
                        metafields_to_save.append(shopify.Metafield({'key': 'vat_number', 'value': vat, 'type': 'single_line_text_field', 'namespace': 'custom'}))
                    
                    salesperson_field = p.get('user_id')
                    if salesperson_field and sync_salesrep: 
                        metafields_to_save.append(shopify.Metafield({'key': 'salesrep', 'value': salesperson_field[1], 'type': 'single_line_text_field', 'namespace': 'custom'}))

                    if metafields_to_save: c.metafields = metafields_to_save
                    c.save()
                    
                    if not CustomerMap.query.filter_by(shopify_customer_id=str(c.id), shop_url=shop_url).first():
                        db.session.add(CustomerMap(shop_url=shop_url, shopify_customer_id=str(c.id), odoo_partner_id=p['id'], email=email))
                        db.session.commit()
                    
                    # B2B Links
                    try:
                        b2b_cid = ensure_shopify_company(b2b_company_name, shop_url)
                        if b2b_cid:
                            b2b_lid = ensure_company_location(b2b_cid, b2b_location_name, address_data, shop_url)
                            if b2b_lid: assign_customer_to_company(b2b_cid, b2b_lid, str(c.id), shop_url)
                    except Exception as b2b_error:
                        log_event('Customer Sync', 'Warning', f"B2B Link Error for {email}: {b2b_error}", shop_url=shop_url)

                    synced_count += 1

                except Exception as e:
                    log_event('Customer Sync', 'Error', f"Failed {email}: {e}", shop_url=shop_url)

            total_found += len(odoo_customers)
            offset += limit
            log_event('Customer Sync', 'Info', f"Processed batch {offset}...", shop_url=shop_url)

        set_config('last_customer_sync_time', current_run_time, shop_url=shop_url)
        log_event('Customer Sync', 'Success', f"Sync Complete. Processed {synced_count}/{total_found} customers.", shop_url=shop_url)
