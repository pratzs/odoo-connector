from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from security_utils import encrypt_val, decrypt_val # Import helpers

db = SQLAlchemy()

class Shop(db.Model):
    __tablename__ = 'shop'
    id = db.Column(db.Integer, primary_key=True)
    shop_url = db.Column(db.String(255), unique=True, nullable=False)
    access_token = db.Column(db.String(255), nullable=False)
    odoo_url = db.Column(db.String(255))
    odoo_db = db.Column(db.String(255))
    odoo_username = db.Column(db.String(255))
    last_inventory_sync_success = db.Column(db.DateTime)
    last_order_sync_success = db.Column(db.DateTime)
    last_return_sync_success = db.Column(db.DateTime)
     # --- ADD THESE BILLING COLUMNS ---
    charge_id = db.Column(db.String(255), nullable=True)
    plan_name = db.Column(db.String(100), nullable=True)
    # ---------------------------------
    
    # STORE ENCRYPTED PASSWORD
    odoo_password_enc = db.Column(db.String(500)) 

    sync_start_date = db.Column(db.String(20), default='2000-01-01')

    # Helper property to handle encryption automatically
    @property
    def odoo_password(self):
        return decrypt_val(self.odoo_password_enc)

    @odoo_password.setter
    def odoo_password(self, value):
        self.odoo_password_enc = encrypt_val(value)

    odoo_company_id = db.Column(db.Integer, default=1)
    is_active = db.Column(db.Boolean, default=True)
    install_date = db.Column(db.DateTime, default=datetime.utcnow)

class ProductMap(db.Model):
    __tablename__ = 'product_map'
    __table_args__ = (
        db.UniqueConstraint('shop_url', 'sku', name='uq_product_map_shop_sku'),
    )
    id = db.Column(db.Integer, primary_key=True)
    shop_url = db.Column(db.String(255), index=True, nullable=False)
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

    # --- MULTIPASS LOGIN ---
    # Stores a bcrypt/werkzeug hash of the customer's self-set password.
    # Never stores plaintext. Null = password not yet set.
    password_hash = db.Column(db.String(255), nullable=True)
    # One-time token emailed to the customer for password set/reset
    reset_token = db.Column(db.String(100), nullable=True, index=True)
    reset_token_expires = db.Column(db.DateTime, nullable=True)

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
    # Composite primary key — scoped per tenant for multi-tenant safety
    shopify_id = db.Column(db.String(50), primary_key=True)
    shop_url   = db.Column(db.String(255), primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class SyncHealth(db.Model):
    __tablename__ = 'sync_health'
    __table_args__ = (
        db.UniqueConstraint('shop_url', 'entity', name='uq_sync_health'),
    )
    id                  = db.Column(db.Integer, primary_key=True)
    shop_url            = db.Column(db.String(255), index=True, nullable=False)
    entity              = db.Column(db.String(50), nullable=False)
    last_success_at     = db.Column(db.DateTime, nullable=True)
    last_attempt_at     = db.Column(db.DateTime, nullable=True)
    consecutive_failures = db.Column(db.Integer, default=0)
    last_error          = db.Column(db.Text, nullable=True)

class FailedSyncOrder(db.Model):
    __tablename__ = 'failed_sync_orders'
    __table_args__ = (
        db.UniqueConstraint('shop_url', 'shopify_id', name='uq_failed_sync_order'),
    )
    id              = db.Column(db.Integer, primary_key=True)
    shop_url        = db.Column(db.String(255), index=True, nullable=False)
    shopify_id      = db.Column(db.String(50), nullable=False)
    order_data      = db.Column(db.Text, nullable=False)
    last_attempt_at = db.Column(db.DateTime, default=datetime.utcnow)
    attempt_count   = db.Column(db.Integer, default=0)
    last_error      = db.Column(db.Text)
