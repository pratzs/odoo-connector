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
            mapped_shopify_id = str(pm.shopify_variant_id) if pm else None
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
                    # Try to match by ID if mapped, otherwise pick first
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

            # 3. CHECK SHOPIFY DATA (Handle Duplicates)
            print(f"\n   👉 SHOPIFY DATA:")
            target_shopify_variant = None
            
            try:
                # Find ALL variants with this SKU
                variants = shopify.Variant.find(params={'sku': target_sku})
                
                if not variants:
                     print("      ❌ NOT FOUND IN SHOPIFY.")
                else:
                    if len(variants) > 1:
                        print(f"      ⚠️ DUPLICATE SKU DETECTED: Found {len(variants)} products in Shopify with SKU '{target_sku}':")
                    
                    for v in variants:
                        p = shopify.Product.find(v.product_id)
                        is_mapped = (str(v.id) == mapped_shopify_id)
                        marker = "👈 (CORRECT / MAPPED)" if is_mapped else "❌ (DUPLICATE / WRONG)"
                        
                        print(f"      🔹 Product: {p.title}")
                        print(f"         Variant ID: {v.id} {marker}")
                        print(f"         Price: {v.price} | Stock: {v.inventory_quantity}")
                        
                        if is_mapped:
                            target_shopify_variant = v
                    
                    # If we didn't find the mapped one in the list, but list exists, default to first (or none)
                    if not target_shopify_variant and len(variants) == 1:
                        target_shopify_variant = variants[0]

            except Exception as e:
                print(f"      ⚠️ Shopify Error: {e}")

            # 4. FINAL COMPARISON (Only if we found the RIGHT one)
            if odoo_prod and target_shopify_variant:
                print(f"\n   ⚖️ COMPARISON (Odoo vs Correct Shopify Variant):")
                v = target_shopify_variant
                
                # Price Check
                if float(v.price) == float(odoo_prod['list_price']):
                    print(f"      - Price: ✅ Match")
                else:
                    print(f"      - Price: ❌ MISMATCH ({odoo_prod['list_price']} vs {v.price})")
                
                # Stock Check
                if int(v.inventory_quantity) == int(odoo_stock):
                    print(f"      - Stock: ✅ Match")
                else:
                    print(f"      - Stock: ❌ MISMATCH ({odoo_stock} vs {v.inventory_quantity})")
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

    # PART B (Customer) remains the same...
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
    parser = argparse.ArgumentParser()
    parser.add_argument('--shop', required=True)
    parser.add_argument('--sku')
    parser.add_argument('--email')
    args = parser.parse_args()

    if not args.sku and not args.email:
        sys.exit(1)

    run_diagnosis(args.shop, args.sku, args.email)
