import shopify
import json
import re
from datetime import datetime, timedelta
from models import db, Shop, CustomerMap
from utils import get_config, log_event, set_config, get_odoo_connection, setup_shopify_session


# --- GRAPHQL HELPERS FOR B2B ---
def execute_graphql(query, variables=None, shop_url=None):
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


# =====================================================
# SHARED HELPER — builds Shopify customer fields
# =====================================================
def _build_customer_fields(p, group_whitelist, sync_vat, sync_salesrep, odoo):
    """
    Returns a dict of fields to apply to a Shopify customer object,
    or None if this customer should be skipped.
    """
    parent_info = p.get('parent_id')

    # RULE 1: Skip the group parent company record itself (exact match only)
    # e.g. "CSB Groups 3 Ltd" should not become a Shopify customer
    if group_whitelist and not parent_info:
        is_group_parent = any(
            p.get('name', '').strip().lower() == g.strip().lower()
            for g in group_whitelist
        )
        if is_group_parent:
            return None, f"group_parent:{p.get('name')}"

    # RULE 2: Child contacts are only synced if their parent is whitelisted
    if parent_info and group_whitelist:
        parent_name = parent_info[1]
        is_whitelisted = any(g.strip().lower() in parent_name.lower() for g in group_whitelist)
        if not is_whitelisted:
            return None, f"non_whitelisted_child:{p.get('name')}"

    # RULE 3: Independent businesses (no parent, not a group parent) → always sync

    # --- Names & B2B context ---
    shopify_first_name = p.get('name')
    shopify_last_name = ""
    shopify_company = p.get('name')
    b2b_company_name = p.get('name')
    b2b_location_name = f"{p.get('name')} [OID:{p['id']}]"
    staff_note = f"Independent | Odoo ID: {p['id']}"
    context_tags = ["Independent"]

    if parent_info:
        parent_name = parent_info[1]
        shopify_company = parent_name
        b2b_company_name = parent_name
        staff_note = f"Franchise Site | Odoo ID: {p['id']}"
        context_tags = ["Franchise", "Site"]

    raw_email = p.get('email') or ''
    # Extract email from "Name <email>" or ": Name <email>" formats stored in Odoo
    email_match = re.search(r'[\w._%+\-]+@[\w.\-]+\.[a-zA-Z]{2,}', raw_email)
    if email_match:
        email = email_match.group(0).lower()
    elif raw_email and '@' in raw_email:
        email = raw_email.strip().lower()
    else:
        email = f"no-email-{p['id']}@pos.local"

    # Metafields
    metafields = []
    vat = p.get('vat')
    if vat and sync_vat:
        staff_note = f"{staff_note}\nVAT: {vat}"
        metafields.append(shopify.Metafield({
            'key': 'vat_number', 'value': vat,
            'type': 'single_line_text_field', 'namespace': 'custom'
        }))

    salesperson_field = p.get('user_id')
    if salesperson_field and sync_salesrep:
        metafields.append(shopify.Metafield({
            'key': 'salesrep', 'value': salesperson_field[1],
            'type': 'single_line_text_field', 'namespace': 'custom'
        }))

    odoo_tags = odoo.get_tag_names(p.get('category_id', []))

    return {
        'email': email,
        'first_name': shopify_first_name,
        'last_name': shopify_last_name,
        'note': staff_note,
        'phone': (p.get('phone') or p.get('mobile') or '').strip(),
        'shopify_company': shopify_company,
        'b2b_company_name': b2b_company_name,
        'b2b_location_name': b2b_location_name,
        'context_tags': context_tags,
        'odoo_tags': odoo_tags,
        'metafields': metafields,
        'address_country': p.get('country_id')[1] if p.get('country_id') else 'NZ',
        'street': p.get('street') or '',
        'city': p.get('city') or '',
        'zip': p.get('zip') or '',
    }, None


def _sync_single_customer(p, fields, shop_url, odoo):
    """
    Saves one customer to Shopify and creates/updates the CustomerMap record.
    Returns True on success, False on failure.
    """
    email = fields['email']
    try:
        shopify_cust = shopify.Customer.search(query=f"email:{email}")
        c = shopify_cust[0] if shopify_cust else shopify.Customer()
        if not shopify_cust:
            c.email = email

        c.tax_exempt = False
        c.first_name = fields['first_name']
        c.last_name = fields['last_name']
        c.note = fields['note']
        # Use extracted clean email (handles "Name <email>" Odoo format)
        c.email = fields['email']
        # Sanitise phone — remove spaces, keep only +, digits, hyphens
        c.phone = re.sub(r'[^\d+\-]', '', fields['phone']) if fields['phone'] else ''
        c.verified_email = True

        # Only attach address if we have at least a street — empty addresses cause Shopify to reject new customers
        if fields['street']:
            sanitized_phone = re.sub(r'[^\d+\-]', '', fields['phone']) if fields['phone'] else ''
            address_data = {
                'address1': fields['street'],
                'city': fields['city'],
                'zip': fields['zip'],
                'country': fields['address_country'],
                'company': fields['shopify_company'],
                'phone': sanitized_phone,
                'first_name': fields['first_name'],
                'last_name': fields['last_name'],
                'default': True
            }
            c.addresses = [shopify.Address(address_data)]
        else:
            address_data = {}  # No address — avoids Shopify rejecting new customers with empty fields

        tags_str = getattr(c, 'tags', '')
        current_tags = [t.strip() for t in tags_str.split(',')] if tags_str else []
        c.tags = ",".join(list(set(current_tags + fields['odoo_tags'] + fields['context_tags'])))

        if fields['metafields']:
            c.metafields = fields['metafields']

        c.save()

        # Save mapping
        if not CustomerMap.query.filter_by(shopify_customer_id=str(c.id), shop_url=shop_url).first():
            db.session.add(CustomerMap(
                shop_url=shop_url,
                shopify_customer_id=str(c.id),
                odoo_partner_id=p['id'],
                email=email
            ))
            db.session.commit()

        # B2B company link
        try:
            b2b_cid = ensure_shopify_company(fields['b2b_company_name'], shop_url)
            if b2b_cid:
                b2b_lid = ensure_company_location(b2b_cid, fields['b2b_location_name'], address_data, shop_url)
                if b2b_lid:
                    assign_customer_to_company(b2b_cid, b2b_lid, str(c.id), shop_url)
        except Exception as b2b_error:
            log_event('Customer Sync', 'Warning', f"B2B Link Error for {email}: {b2b_error}", shop_url=shop_url)

        return True

    except Exception as e:
        log_event('Customer Sync', 'Error', f"Failed {email}: {e}", shop_url=shop_url)
        return False


# =====================================================
# QUICK SYNC — delta only (last 24h buffer)
# =====================================================
def sync_customers_master(shop_url):
    """
    Odoo -> Shopify Customer Sync (Paginated, delta based on write_date).
    Runs every 12 hours via scheduler. Uses a 24h lookback buffer to avoid
    missing customers created just before the last sync completed.
    """
    from app import app

    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url):
            log_event('Customer Sync', 'Error', "Connection Error — aborting.", shop_url=shop_url)
            return

        direction = get_config('cust_direction', 'bidirectional', shop_url=shop_url)
        if direction == 'shopify_to_odoo':
            return

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

        # 24h lookback buffer — catches customers created just before last sync
        last_run_raw = get_config('last_customer_sync_time', '2000-01-01 00:00:00', shop_url=shop_url)
        current_run_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        try:
            last_run = (datetime.strptime(last_run_raw, '%Y-%m-%d %H:%M:%S') - timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            last_run = '2000-01-01 00:00:00'

        # CRITICAL: Save timestamp NOW before processing starts.
        # If the worker is killed halfway, next run still moves forward
        # instead of reprocessing thousands of customers again.
        set_config('last_customer_sync_time', current_run_time, shop_url=shop_url)

        domain = [
            ('write_date', '>', last_run),
            ('active', '=', True),
            ('email', '!=', False),
            ('type', '=', 'contact')
        ]
        if company_id:
            domain.append('|')
            domain.append(('company_id', '=', int(company_id)))
            domain.append(('company_id', '=', False))

        fields = ['id', 'name', 'email', 'phone', 'street', 'city', 'zip',
                  'country_id', 'vat', 'category_id', 'user_id', 'is_company', 'parent_id']

        limit = 1000
        offset = 0
        synced_count = 0
        total_found = 0

        try:
            count_check = odoo.models.execute_kw(
                odoo.db, odoo.uid, odoo.password, 'res.partner', 'search_count', [domain])
            log_event('Customer Sync', 'Info',
                f"Found {count_check} changed customers. Starting Paginated Sync...", shop_url=shop_url)
        except Exception:
            pass

        while True:
            try:
                odoo_customers = odoo.models.execute_kw(
                    odoo.db, odoo.uid, odoo.password,
                    'res.partner', 'search_read', [domain],
                    {'fields': fields, 'limit': limit, 'offset': offset}
                )
            except Exception as e:
                log_event('Customer Sync', 'Error', f"Odoo Fetch Failed at offset {offset}: {e}", shop_url=shop_url)
                return

            if not odoo_customers:
                break

            for p in odoo_customers:
                cust_fields, skip_reason = _build_customer_fields(p, group_whitelist, sync_vat, sync_salesrep, odoo)

                if cust_fields is None:
                    if skip_reason and skip_reason.startswith('group_parent:'):
                        log_event('Customer Sync', 'Info',
                            f"Skipping group parent: {p.get('name')}", shop_url=shop_url)
                    continue

                # Tag filter
                if use_tags_filter:
                    odoo_tags = cust_fields['odoo_tags']
                    if blacklist and any(t in odoo_tags for t in blacklist): continue
                    if whitelist and not any(t in odoo_tags for t in whitelist): continue

                if _sync_single_customer(p, cust_fields, shop_url, odoo):
                    synced_count += 1
                    log_event('Customer Sync', 'Success',
                        f"Synced: {p.get('name')} ({cust_fields['email']})", shop_url=shop_url)

            total_found += len(odoo_customers)
            offset += limit
            log_event('Customer Sync', 'Info', f"Processed batch {offset}...", shop_url=shop_url)

        log_event('Customer Sync', 'Success',
            f"Sync Complete. Processed {synced_count}/{total_found} customers.", shop_url=shop_url)

# =====================================================
# HEAVY SYNC — full catalog, no time filter
# =====================================================
def sync_all_customers_absolute_master(shop_url):
    """
    Odoo -> Shopify Customer Sync (Full resync, bypasses time filter).
    Use this to recover missed customers or for the monthly full sync.
    """
    from app import app

    with app.app_context():
        odoo = get_odoo_connection(shop_url)
        if not odoo or not setup_shopify_session(shop_url):
            log_event('Customer Sync', 'Error', "Heavy Sync Connection Error — aborting.", shop_url=shop_url)
            return

        direction = get_config('cust_direction', 'bidirectional', shop_url=shop_url)
        if direction == 'shopify_to_odoo':
            return

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

        log_event('Customer Sync', 'Warning',
            "INITIATING FULL CUSTOMER RESYNC (HEAVY). Bypassing time limits.", shop_url=shop_url)

        domain = [
            ('active', '=', True),
            ('email', '!=', False),
            ('type', '=', 'contact')
        ]
        if company_id:
            domain.append('|')
            domain.append(('company_id', '=', int(company_id)))
            domain.append(('company_id', '=', False))

        fields = ['id', 'name', 'email', 'phone', 'street', 'city', 'zip',
                  'country_id', 'vat', 'category_id', 'user_id', 'is_company', 'parent_id']

        limit = 1000
        offset = 0
        synced_count = 0
        total_found = 0

        while True:
            try:
                odoo_customers = odoo.models.execute_kw(
                    odoo.db, odoo.uid, odoo.password,
                    'res.partner', 'search_read', [domain],
                    {'fields': fields, 'limit': limit, 'offset': offset}
                )
            except Exception as e:
                log_event('Customer Sync', 'Error',
                    f"Heavy Odoo Fetch Failed at offset {offset}: {e}", shop_url=shop_url)
                return

            if not odoo_customers:
                break

            for p in odoo_customers:
                cust_fields, skip_reason = _build_customer_fields(p, group_whitelist, sync_vat, sync_salesrep, odoo)

                if cust_fields is None:
                    if skip_reason and skip_reason.startswith('group_parent:'):
                        log_event('Customer Sync', 'Info',
                            f"Skipping group parent: {p.get('name')}", shop_url=shop_url)
                    continue

                # Tag filter
                if use_tags_filter:
                    odoo_tags = cust_fields['odoo_tags']
                    if blacklist and any(t in odoo_tags for t in blacklist): continue
                    if whitelist and not any(t in odoo_tags for t in whitelist): continue

                if _sync_single_customer(p, cust_fields, shop_url, odoo):
                    synced_count += 1
                    log_event('Customer Sync', 'Success',
                        f"Synced: {p.get('name')} ({cust_fields['email']})", shop_url=shop_url)

            total_found += len(odoo_customers)
            offset += limit
            log_event('Customer Sync', 'Info', f"Heavy Processed batch {offset}...", shop_url=shop_url)

        log_event('Customer Sync', 'Success',
            f"HEAVY Sync Complete. Processed {synced_count}/{total_found} customers.", shop_url=shop_url)
