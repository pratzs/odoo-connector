from app import app
from utils import get_odoo_connection
from models import ProductMap
import shopify

# CONFIG
TARGET_SKU = "R0001"  # <--- Ensure this matches exactly
SHOP_URL = "vjtrading.myshopify.com" # Change if needed

with app.app_context():
    print(f"\n🔍 --- DIAGNOSING SKU: {TARGET_SKU} ---")
    
    # 1. CHECK DATABASE MAP
    pm = ProductMap.query.filter_by(sku=TARGET_SKU, shop_url=SHOP_URL).first()
    if pm:
        print(f"✅ FOUND IN DB MAP: Shopify Variant ID {pm.shopify_variant_id}")
    else:
        print("❌ NOT IN DB MAP")

    # 2. CHECK ODOO
    odoo = get_odoo_connection(SHOP_URL)
    if odoo:
        ids = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'search', [[['default_code', '=', TARGET_SKU]]])
        if ids:
            data = odoo.models.execute_kw(odoo.db, odoo.uid, odoo.password, 'product.product', 'read', [ids], {'fields': ['name', 'active', 'sale_ok']})
            print(f"✅ FOUND IN ODOO: {data[0]['name']} (Active: {data[0]['active']}, Sold: {data[0]['sale_ok']})")
        else:
            print("❌ NOT FOUND IN ODOO (Check 'Internal Reference' field exact match)")

    # 3. CHECK SHOPIFY
    from utils import setup_shopify_session
    if setup_shopify_session(SHOP_URL):
        # Search by SKU directly
        variants = shopify.Variant.find(params={'sku': TARGET_SKU})
        if variants:
            v = variants[0]
            p = shopify.Product.find(v.product_id)
            print(f"✅ FOUND IN SHOPIFY!")
            print(f"   - Product Title: {p.title}")
            print(f"   - Product ID: {p.id}")
            print(f"   - Status: {p.status}")
            print(f"   - Variant Title: {v.title}")
            print(f"   👉 URL: https://admin.shopify.com/store/{SHOP_URL.split('.')[0]}/products/{p.id}")
        else:
            print("❌ NOT FOUND IN SHOPIFY (API Search returned nothing)")
    
    print("\n------------------------------------------------")
