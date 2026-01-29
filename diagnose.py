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
# Usage Examples: 
#   python diagnose.py --shop=client.myshopify.com --sku=A123
#   python diagnose.py --shop=client.myshopify.com --email=customer@example.com
# ==========================================

def run_diagnosis(shop_url, target_sku=None, target_email=None):
    print(f"\n🕵️‍♂️ --- STARTING DIAGNOSIS FOR TENANT: {shop_url} ---")
    
    # We import app inside the function or rely on the caller to provide context
    # This prevents the circular import error when app.py calls this script.
    from app import app
    
    with app.app_context():
        # 1. SETUP CONNECTIONS
        odoo = get_odoo_connection(shop_url)
        if not odoo:
            print(f"❌ FATAL: Could not connect to Odoo for {shop_url}.")
            print("   (Check if the shop is installed and credentials are correct in the DB)")
            return

        if not setup_shopify_session(shop_url):
            print(f"❌ FATAL: Could not connect to Shopify for {shop_url}.")
            print("   (Check if the Access Token is valid)")
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
                print(f"      - Image Hash: {pm.image_hash}")
            else:
                print("   ❌ DB MAPPING: NOT FOUND. (This product is unlinked)")

            # 2. CHECK ODOO DATA (Active Only)
            print(f"\n   👉 ODOO DATA (Active Only):")
            
            # --- STRICTLY ACTIVE PRODUCTS ONLY ---
            domain = [['default_code', '=', target_sku], ['active', '=', True]]
            
            # Fields for Pack Logic, Validation, and Stock
            fields = ['name', 'active', 'sale_ok', 'barcode', 'list_price', 
                      'uom_id', 'sh_is_secondary_unit', 'qty_per_pack', 'image_1920', 'qty_available']
            
            try:
                odoo_results = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                    'product.product', 'search_read', [domain], {'fields': fields})
            except Exception as e:
                print(f"      ⚠️ Odoo Search Error: {e}")
                odoo_results = []
            
            odoo_prod = None
            odoo_stock = 0

            if not odoo_results:
                print("      ❌ NOT FOUND IN ODOO (No Active Product with this Internal Reference).")
            else:
                # --- DUPLICATE CHECK ---
                if len(odoo_results) > 1:
                    print(f"      ⚠️ CRITICAL WARNING: {len(odoo_results)} ACTIVE PRODUCTS FOUND WITH SKU '{target_sku}'!")
                    for p in odoo_results:
                        marker = "👈 (Mapped in DB)" if p['id'] == mapped_odoo_id else ""
                        print(f"         - [ID: {p['id']}] {p['name']} {marker}")
                    
                    # Try to pick the "correct" one (matches DB map)
                    odoo_prod = next((p for p in odoo_results if p['id'] == mapped_odoo_id), odoo_results[0])
                    print(f"      ℹ️  Using ID {odoo_prod['id']} for comparison below.")
                else:
                    odoo_prod = odoo_results[0]
                    print(f"      ✅ Found 1 Active Product (ID: {odoo_prod['id']})")

                # Print Details
                print(f"      - Name: {odoo_prod['name']}")
                print(f"      - Active: {odoo_prod['active']}")
                print(f"      - Price: {odoo_prod['list_price']}")
                print(f"      - Barcode: {odoo_prod['barcode'] or 'None'}")
                
                # --- STOCK FETCH (Simple 'qty_available') ---
                odoo_stock = odoo_prod.get('qty_available', 0)
                print(f"      - Stock (On Hand): {odoo_stock}")

                # --- PACK VS UNIT CHECK ---
                is_pack = odoo_prod.get('sh_is_secondary_unit')
                qty = odoo_prod.get('qty_per_pack')
                print(f"      🔍 PACK LOGIC CHECK:")
                print(f"         - Is Secondary Unit: {is_pack}")
                print(f"         - Qty Per Pack: {qty}")
                if is_pack and qty > 1:
                    print(f"         => CONCLUSION: This SHOULD sync as a 'Pack' ({qty}x).")
                else:
                    print(f"         => CONCLUSION: This should sync as a 'Single Unit'.")

                # --- IMAGE HASH CHECK ---
                img_data = odoo_prod.get('image_1920')
                if img_data:
                    calc_hash = hashlib.md5(img_data.encode('utf-8')).hexdigest()
                    db_hash = pm.image_hash if pm else "None"
                    match = "✅ Match" if calc_hash == db_hash else "❌ Mismatch (Will Trigger Sync)"
                    print(f"      🖼️ IMAGE HASH:")
                    print(f"         - Odoo Hash: {calc_hash}")
                    print(f"         - DB Hash:   {db_hash} -> {match}")
                else:
                    print(f"      🖼️ IMAGE: No image in Odoo.")

            # 3. CHECK SHOPIFY DATA (GraphQL Search)
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
                
                # Price Check (With Rounding)
                odoo_price = round(float(odoo_prod['list_price']), 2)
                shopify_price = float(v['price'])
                
                if odoo_price == shopify_price:
                    print(f"      - Price: ✅ Match ({shopify_price})")
                else:
                    print(f"      - Price: ❌ MISMATCH (Odoo: {odoo_price} vs Shopify: {shopify_price})")
                
                # Stock Check
                shopify_stock = int(v['inventoryQuantity'])
                # Using simple 'qty_available' fetched earlier
                if int(odoo_stock) == shopify_stock:
                    print(f"      - Stock: ✅ Match ({shopify_stock})")
                else:
                    print(f"      - Stock: ❌ MISMATCH (Odoo: {odoo_stock} vs Shopify: {shopify_stock})")
                
                # Barcode Check
                sp_barcode = v.get('barcode') or ''
                od_barcode = odoo_prod.get('barcode') or ''
                
                if str(sp_barcode) == str(od_barcode):
                    print(f"      - Barcode: ✅ Match")
                else:
                    print(f"      - Barcode: ❌ MISMATCH (Odoo: {od_barcode} vs Shopify: {sp_barcode})")

            else:
                print(f"\n   ⚖️ COMPARISON: Skipping (Could not match Odoo Product to specific Shopify Variant)")

            # 5. RECENT LOGS
            print(f"\n   📜 RECENT LOGS (Last 5 for SKU):")
            # Filter logs strictly by this tenant
            logs = SyncLog.query.filter(SyncLog.shop_url == shop_url)\
                .filter(SyncLog.message.contains(target_sku))\
                .order_by(SyncLog.timestamp.desc()).limit(5).all()
            
            if logs:
                for log in logs:
                    print(f"      [{log.timestamp.strftime('%Y-%m-%d %H:%M')}] {log.status}: {log.message}")
            else:
                print("      (No recent logs found for this SKU)")


        # =====================================================
        # 👥 PART B: CUSTOMER DIAGNOSIS (EMAIL)
        # =====================================================
        if target_email:
            print(f"\n\n👥 --- DIAGNOSING CUSTOMER: {target_email} ---")
            
            # 1. CHECK DB MAP
            cm = CustomerMap.query.filter_by(email=target_email, shop_url=shop_url).first()
            if cm:
                print(f"   ✅ DB MAPPING: Found! (Shopify ID: {cm.shopify_customer_id} | Odoo ID: {cm.odoo_partner_id})")
            else:
                print("   ❌ DB MAPPING: NOT FOUND.")

            # 2. CHECK ODOO
            try:
                partners = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password,
                    'res.partner', 'search_read', [[['email', '=', target_email]]], {'fields': ['name', 'id', 'parent_id']})
                
                if partners:
                    p = partners[0]
                    print(f"   ✅ FOUND IN ODOO: {p['name']} (ID: {p['id']})")
                    if p.get('parent_id'):
                        print(f"      - Parent Company: {p['parent_id'][1]}")
                else:
                    print("   ❌ NOT FOUND IN ODOO.")
            except Exception as e:
                print(f"   ⚠️ Odoo Customer Search Error: {e}")

            # 3. CHECK SHOPIFY
            try:
                customers = shopify.Customer.search(query=f"email:{target_email}")
                if customers:
                    c = customers[0]
                    print(f"   ✅ FOUND IN SHOPIFY: {c.first_name} {c.last_name} (ID: {c.id})")
                    print(f"      - Orders Count: {c.orders_count}")
                    print(f"      - State: {c.state}")
                else:
                    print("   ❌ NOT FOUND IN SHOPIFY.")
            except Exception as e:
                print(f"   ⚠️ Shopify Customer Search Error: {e}")

    print("\n🕵️‍♂️ --- DIAGNOSIS COMPLETE ---\n")

if __name__ == "__main__":
    from app import app
    parser = argparse.ArgumentParser(description="Multi-Tenant Odoo Connector Diagnostic Tool")
    
    # Required Argument: Shop URL
    parser.add_argument('--shop', required=True, help="The Shopify domain (e.g., client.myshopify.com)")
    
    # Optional Arguments: What to check
    parser.add_argument('--sku', help="Product SKU to diagnose")
    parser.add_argument('--email', help="Customer Email to diagnose")

    args = parser.parse_args()
    
    # Ensure at least one target is provided
    if not args.sku and not args.email:
        print("❌ Error: You must provide at least one target to diagnose (--sku or --email).")
        sys.exit(1)

    # We don't need with app.app_context() here because run_diagnosis handles it internally now
    run_diagnosis(args.shop, args.sku, args.email)
