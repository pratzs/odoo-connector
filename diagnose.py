import argparse
import sys
import hashlib
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
            if pm:
                print(f"   ✅ DB MAPPING: Found! (Shopify Variant ID: {pm.shopify_variant_id} | Odoo ID: {pm.odoo_product_id})")
                print(f"      - Last Synced: {pm.last_synced_at}")
                print(f"      - Image Hash: {pm.image_hash}")
            else:
                print("   ❌ DB MAPPING: NOT FOUND. (This product is unlinked)")

            # 2. CHECK ODOO DATA (Pack Logic + Barcode)
            print(f"\n   👉 ODOO DATA:")
            domain = [['default_code', '=', target_sku], '|', ['active', '=', True], ['active', '=', False]]
            
            # Fields for Pack Logic and Validation
            fields = ['name', 'active', 'sale_ok', 'barcode', 'list_price', 
                      'uom_id', 'sh_is_secondary_unit', 'qty_per_pack', 'image_1920']
            
            try:
                odoo_ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                    'product.product', 'search_read', [domain], {'fields': fields})
            except Exception as e:
                print(f"      ⚠️ Odoo Search Error: {e}")
                odoo_ids = []
            
            odoo_prod = None
            odoo_stock = 0
            
            if odoo_ids:
                odoo_prod = odoo_ids[0]
                print(f"      ✅ Found in Odoo (ID: {odoo_prod['id']})")
                print(f"      - Name: {odoo_prod['name']}")
                print(f"      - Active: {odoo_prod['active']}")
                print(f"      - Price: {odoo_prod['list_price']}")
                print(f"      - Barcode: {odoo_prod['barcode'] or 'None'}")
                
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

                # --- LIVE INVENTORY CHECK ---
                # Using the fast read_group method
                try:
                    stock_data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 
                        'stock.quant', 'read_group', 
                        [[('product_id', '=', odoo_prod['id']), ('location_id.usage', '=', 'internal')]], 
                        ['quantity'], ['product_id'])
                    odoo_stock = stock_data[0]['quantity'] if stock_data else 0
                    print(f"      📊 LIVE STOCK (All Internal Locations): {odoo_stock}")
                except Exception as e:
                    print(f"      ⚠️ Stock Fetch Error: {e}")

            else:
                print("      ❌ NOT FOUND IN ODOO.")

            # 3. CHECK SHOPIFY DATA
            print(f"\n   👉 SHOPIFY DATA:")
            try:
                # Search by SKU
                variants = shopify.Variant.find(limit=1, params={'sku': target_sku})
                if variants:
                    v = variants[0]
                    p = shopify.Product.find(v.product_id)
                    print(f"      ✅ Found in Shopify (Variant ID: {v.id})")
                    print(f"      - Product: {p.title}")
                    print(f"      - Price: {v.price}")
                    print(f"      - Stock: {v.inventory_quantity}")
                    print(f"      - Barcode: {v.barcode}")

                    # 4. FINAL COMPARISON
                    print(f"\n   ⚖️ COMPARISON (Odoo vs Shopify):")
                    
                    if odoo_prod:
                        # Price
                        p_match = float(v.price) == float(odoo_prod['list_price'])
                        print(f"      - Price: {'✅ Match' if p_match else f'❌ MISMATCH ({odoo_prod['list_price']} vs {v.price})'}")
                        
                        # Stock
                        s_match = int(v.inventory_quantity) == int(odoo_stock)
                        print(f"      - Stock: {'✅ Match' if s_match else f'❌ MISMATCH ({odoo_stock} vs {v.inventory_quantity})'}")
                        
                        # Barcode
                        b_match = str(v.barcode or '') == str(odoo_prod['barcode'] or '')
                        print(f"      - Barcode: {'✅ Match' if b_match else f'❌ MISMATCH ({odoo_prod['barcode']} vs {v.barcode})'}")
                else:
                    print("      ❌ NOT FOUND IN SHOPIFY.")
            except Exception as e:
                print(f"      ⚠️ Shopify Error: {e}")

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
    # If running from command line, we can import app here to get context
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
