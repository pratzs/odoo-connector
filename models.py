from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

db = SQLAlchemy()

# --- MOVED FROM APP.PY (Centralized here) ---
class Shop(db.Model):
    __tablename__ = 'shop'
    id = db.Column(db.Integer, primary_key=True)
    shop_url = db.Column(db.String(255), unique=True, nullable=False)
    access_token = db.Column(db.String(255), nullable=False)
    odoo_url = db.Column(db.String(255))
    odoo_db = db.Column(db.String(255))
    odoo_username = db.Column(db.String(255))
    odoo_password = db.Column(db.String(255))
    odoo_company_id = db.Column(db.Integer, default=1)
    is_active = db.Column(db.Boolean, default=True)
    install_date = db.Column(db.DateTime, default=datetime.utcnow)

class ProductMap(db.Model):
    __tablename__ = 'product_map'
    # Composite PK: A Variant ID is unique, but adding shop_url makes queries faster/safer
    id = db.Column(db.Integer, primary_key=True)
    shop_url = db.Column(db.String(255), index=True, nullable=False) # SECURITY
    shopify_variant_id = db.Column(db.String(50), nullable=False)
    odoo_product_id = db.Column(db.Integer, nullable=False)
    sku = db.Column(db.String(50), index=True)
    last_synced_at = db.Column(db.DateTime, default=datetime.utcnow)
    image_hash = db.Column(db.String(32), nullable=True)

class CustomerMap(db.Model):
    __tablename__ = 'customer_map'
    id = db.Column(db.Integer, primary_key=True)
    shop_url = db.Column(db.String(255), index=True, nullable=False) # SECURITY
    shopify_customer_id = db.Column(db.String(50), nullable=False)
    odoo_partner_id = db.Column(db.Integer, nullable=False)
    email = db.Column(db.String(100), index=True)

class SyncLog(db.Model):
    __tablename__ = 'sync_logs'
    id = db.Column(db.Integer, primary_key=True)
    shop_url = db.Column(db.String(255), index=True) # SECURITY (Logs isolate by shop)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    entity = db.Column(db.String(50)) 
    status = db.Column(db.String(20)) 
    message = db.Column(db.Text)

class AppSetting(db.Model):
    __tablename__ = 'app_settings'
    # COMPOSITE PRIMARY KEY: Settings are unique PER SHOP
    shop_url = db.Column(db.String(255), primary_key=True) 
    key = db.Column(db.String(50), primary_key=True)
    value = db.Column(db.Text)

class ProcessedOrder(db.Model):
    __tablename__ = 'processed_orders'
    # Shopify IDs are globally unique, so this is safe as-is
    shopify_id = db.Column(db.String(50), primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
