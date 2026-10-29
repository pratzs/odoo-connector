import pytest

import services.orders as orders
from services.orders import excluded_order_reason, get_order_exclusions, process_order_data

SHOP = 'test.myshopify.com'


@pytest.fixture
def cfg(monkeypatch):
    """Stand in for get_config: settings maps key -> stored value, missing keys fall back to the default."""
    settings = {}
    monkeypatch.setattr(orders, 'get_config', lambda key, default=None, shop_url=None: settings.get(key, default))
    return settings


def test_expo_customer_is_excluded_by_default(cfg):
    assert excluded_order_reason({'email': 'expo@glutenfree.com'}, SHOP)


def test_match_is_case_insensitive_and_checks_every_email_field(cfg):
    assert excluded_order_reason({'customer': {'email': 'Expo@GlutenFree.com'}}, SHOP)
    assert excluded_order_reason({'contact_email': ' expo@glutenfree.com '}, SHOP)


def test_other_customers_are_not_excluded(cfg):
    assert excluded_order_reason({'email': 'buyer@example.com', 'customer': {'email': 'buyer@example.com'}}, SHOP) is None
    assert excluded_order_reason({}, SHOP) is None


def test_settings_override_the_defaults(cfg):
    cfg['order_exclude_emails'] = ['other@example.com']
    cfg['order_exclude_sku_suffixes'] = ['-XYZ']
    assert excluded_order_reason({'email': 'expo@glutenfree.com'}, SHOP) is None
    assert excluded_order_reason({'email': 'other@example.com'}, SHOP)
    assert get_order_exclusions(SHOP)[1] == ['-XYZ']


def test_junk_settings_fall_back_to_defaults(cfg):
    cfg['order_exclude_emails'] = 'garbage'
    cfg['order_exclude_sku_suffixes'] = None
    emails, suffixes = get_order_exclusions(SHOP)
    assert emails == {'expo@glutenfree.com'}
    assert suffixes == ['-GF26OCT']


def test_excluded_order_is_skipped_before_touching_odoo(cfg, app_ctx, monkeypatch):
    class Odoo:  # any attribute access would mean the order reached Odoo
        def __getattr__(self, name):
            raise AssertionError(f'Odoo touched: {name}')

    monkeypatch.setattr(orders, 'acquire_distributed_lock', _always_acquired)
    ok, msg = process_order_data(
        {'id': 1, 'name': '#1001', 'email': 'expo@glutenfree.com',
         'line_items': [{'sku': 'C0381 - GF26OCT', 'name': 'x', 'price': '3.91', 'quantity': 1}]},
        Odoo(), SHOP)
    assert ok and msg.startswith('Skipped: customer expo@glutenfree.com')
    assert orders.ProcessedOrder.query.count() == 0  # not locked either


class _always_acquired:
    def __init__(self, *a, **k): pass
    def __enter__(self): return True
    def __exit__(self, *a): return False
