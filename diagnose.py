import argparse
import sys
import hashlib
import json
import shopify
# NOTE: We do NOT import app at the top to avoid circular import crashes.
from utils import get_odoo_connection, setup_shopify_session
from models import ProductMap, CustomerMap, SyncLog

# ==========================================
# 🎯 DIAGNOSE TOOL (MULTI-TENANT & DEEP SCAN)
# ==========================================

def run_diagnosis(shop_url, target_sku=None, target_email=None):
    print(f"\n🕵️‍♂️ --- STARTING DIAGNOSIS FOR TENANT: {shop_url} ---")
    
    from app import app
    
    with app.app_context():
        # 1. SETUP CONNECTIONS
        odoo = get_odoo_connection(shop_url)
        if not odoo:
            print(f"❌ FATAL: Could not connect to Odoo for {shop_url}.")
            return

        if not setup_shopify_session(shop_url):
            print(f"❌ FATAL: Could not connect to Shopify for {shop_url}.")
            return

        # =====================================================
        # 📦 PART A: PRODUCT DIAGNOSIS (SKU)
        # =====================================================
        if target_sku:
            print(f"\n📦 --- DIAGNOSING PRODUCT: {target_sku} ---")
            
            # 1. CHECK DATABASE MAPPING
            pm = ProductMap.query.filter_by(sku=target_sku, shop_url=shop_url).first()
            # Shopify IDs in DB are just numbers, but GraphQL returns "gid://shopify/ProductVariant/..."
            mapped_shopify_id_numeric = str(pm.shopify_variant_id) if pm else None
            mapped_odoo_id = pm.odoo_product_id if pm else None
            
            if pm:
                print(f"   ✅ DB MAPPING: Found! (Shopify Variant ID: {pm.shopify_variant_id} | Odoo ID: {pm.odoo_product_id})")
                print(f"      - Last Synced: {pm.last_synced_at}")
            else:
                print("   ❌ DB MAPPING: NOT FOUND. (This product is unlinked)")

            # 2. CHECK ODOO DATA (Active Only)
            print(f"\n   👉 ODOO DATA (Active Only):")
            domain = [['default_code', '=', target_sku], ['active', '=', True]]
            fields = ['name', 'active', 'sale_ok', 'barcode', 'list_price', 
                      'uom_id', 'sh_is_secondary_unit', 'qty_per_pack', 'image_1920']
            
            try:
                odoo_results = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                    'product.product', 'search_read', [domain], {'fields': fields})
            except Exception as e:
                print(f"      ⚠️ Odoo Search Error: {e}")
                odoo_results = []
            
            odoo_prod = None
            odoo_stock = 0

            if not odoo_results:
                print("      ❌ NOT FOUND IN ODOO (No Active Product with this SKU).")
            else:
                if len(odoo_results) > 1:
                    print(f"      ⚠️ WARNING: {len(odoo_results)} ACTIVE PRODUCTS FOUND IN ODOO!")
                    odoo_prod = next((p for p in odoo_results if p['id'] == mapped_odoo_id), odoo_results[0])
                else:
                    odoo_prod = odoo_results[0]
                    print(f"      ✅ Found 1 Active Product (ID: {odoo_prod['id']})")

                print(f"      - Name: {odoo_prod['name']}")
                print(f"      - Price: {odoo_prod['list_price']}")
                
                # Live Inventory Check
                try:
                    stock_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                        'stock.quant', 'read_group', 
                        [[('product_id', '=', odoo_prod['id']), ('location_id.usage', '=', 'internal')]], 
                        ['quantity'], ['product_id'])
                    odoo_stock = stock_data[0]['quantity'] if stock_data else 0
                    print(f"      📊 LIVE STOCK (All Internal Locations): {odoo_stock}")
                except Exception as e:
                    print(f"      ⚠️ Stock Fetch Error: {e}")

            # 3. CHECK SHOPIFY DATA (Using GraphQL for Precision)
            print(f"\n   👉 SHOPIFY DATA (GraphQL Search):")
            target_shopify_variant = None
            
            try:
                # Query matches SKU exactly and pulls first 50 EXACT matches
                gql_query = """
                {
                  productVariants(first: 50, query: "sku:%s") {
                    edges {
                      node {
                        id
                        sku
                        price
                        inventoryQuantity
                        barcode
                        product {
                          title
                        }
                      }
                    }
                  }
                }
                """ % target_sku
                
                client = shopify.GraphQL()
                result = client.execute(gql_query)
                data = json.loads(result)
                
                edges = data.get('data', {}).get('productVariants', {}).get('edges', [])
                
                if not edges:
                     print("      ❌ NOT FOUND IN SHOPIFY.")
                else:
                    if len(edges) > 1:
                        print(f"      ⚠️ DUPLICATE SKU DETECTED: Found {len(edges)} variants in Shopify with SKU '{target_sku}':")
                    
                    for edge in edges:
                        node = edge['node']
                        # GraphQL ID looks like "gid://shopify/ProductVariant/123456"
                        # We extract just the number to compare
                        v_id_numeric = node['id'].split('/')[-1]
                        
                        is_mapped = (v_id_numeric == mapped_shopify_id_numeric)
                        marker = "👈 (CORRECT / MAPPED)" if is_mapped else "❌ (DUPLICATE / WRONG)"
                        
                        print(f"      🔹 Product: {node['product']['title']}")
                        print(f"         Variant ID: {v_id_numeric} {marker}")
                        print(f"         Price: {node['price']} | Stock: {node['inventoryQuantity']}")
                        
                        if is_mapped:
                            target_shopify_variant = node
                    
                    # If mapped variant not found in list (weird), try just taking the first one if only 1 exists
                    if not target_shopify_variant and len(edges) == 1:
                        target_shopify_variant = edges[0]['node']

            except Exception as e:
                print(f"      ⚠️ Shopify GraphQL Error: {e}")

            # 4. FINAL COMPARISON (Only if we found the RIGHT one)
            if odoo_prod and target_shopify_variant:
                print(f"\n   ⚖️ COMPARISON (Odoo vs Correct Shopify Variant):")
                v = target_shopify_variant
                
                # Price Check
                if float(v['price']) == float(odoo_prod['list_price']):
                    print(f"      - Price: ✅ Match")
                else:
                    print(f"      - Price: ❌ MISMATCH ({odoo_prod['list_price']} vs {v['price']})")
                
                # Stock Check
                if int(v['inventoryQuantity']) == int(odoo_stock):
                    print(f"      - Stock: ✅ Match")
                else:
                    print(f"      - Stock: ❌ MISMATCH ({odoo_stock} vs {v['inventoryQuantity']})")
                
                # Barcode Check
                # v['barcode'] can be None in GraphQL
                sp_barcode = v.get('barcode') or ''
                od_barcode = odoo_prod.get('barcode') or ''
                
                if str(sp_barcode) == str(od_barcode):
                    print(f"      - Barcode: ✅ Match")
                else:
                    print(f"      - Barcode: ❌ MISMATCH ({od_barcode} vs {sp_barcode})")

            else:
                print(f"\n   ⚖️ COMPARISON: Skipping (Could not match Odoo Product to specific Shopify Variant)")

            # 5. RECENT LOGS
            print(f"\n   📜 RECENT LOGS (Last 5 for SKU):")
            logs = SyncLog.query.filter(SyncLog.shop_url == shop_url)\
                .filter(SyncLog.message.contains(target_sku))\
                .order_by(SyncLog.timestamp.desc()).limit(5).all()
            
            if logs:
                for log in logs:
                    print(f"      [{log.timestamp.strftime('%Y-%m-%d %H:%M')}] {log.status}: {log.message}")
            else:
                print("      (No recent logs found for this SKU)")

    # PART B (Customer) - Keep as is
    if target_email:
        print(f"\n\n👥 --- DIAGNOSING CUSTOMER: {target_email} ---")
        
        cm = CustomerMap.query.filter_by(email=target_email, shop_url=shop_url).first()
        if cm:
            print(f"   ✅ DB MAPPING: Found! (Shopify ID: {cm.shopify_customer_id} | Odoo ID: {cm.odoo_partner_id})")
        else:
            print("   ❌ DB MAPPING: NOT FOUND.")

        try:
            partners = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                'res.partner', 'search_read', [[['email', '=', target_email]]], {'fields': ['name', 'id', 'parent_id']})
            if partners:
                p = partners[0]
                print(f"   ✅ FOUND IN ODOO: {p['name']} (ID: {p['id']})")
            else:
                print("   ❌ NOT FOUND IN ODOO.")
        except Exception as e:
            print(f"   ⚠️ Odoo Error: {e}")

        try:
            customers = shopify.Customer.search(query=f"email:{target_email}")
            if customers:
                c = customers[0]
                print(f"   ✅ FOUND IN SHOPIFY: {c.first_name} {c.last_name} (ID: {c.id})")
            else:
                print("   ❌ NOT FOUND IN SHOPIFY.")
        except Exception as e:
            print(f"   ⚠️ Shopify Error: {e}")

    print("\n🕵️‍♂️ --- DIAGNOSIS COMPLETE ---\n")

if __name__ == "__main__":
    from app import app
    parser = argparse.ArgumentParser()
    parser.add_argument('--shop', required=True)
    parser.add_argument('--sku')
    parser.add_argument('--email')
    args = parser.parse_args()

    if not args.sku and not args.email:
        sys.exit(1)

    run_diagnosis(args.shop, args.sku, args.email)
