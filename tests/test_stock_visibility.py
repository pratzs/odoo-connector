from services.stock_visibility import (
    NOT_RECEIVED, SOLD_OUT, decide_reason, hidden_product_ids, normalise_product, plan_changes,
    product_sellable, variant_sellable, CFG_SOLD_OUT, HIDDEN_KEY, get_settings,
)
from utils import set_config

BOTH = {'sold_out': True, 'not_received': True}
SOLD_ONLY = {'sold_out': True, 'not_received': False}
RECV_ONLY = {'sold_out': False, 'not_received': True}
OFF = {'sold_out': False, 'not_received': False}


def prod(pid, qty=0, published=True, status='ACTIVE', sku=None, policy='DENY', tracked=True, title=None):
    node = {
        'legacyResourceId': pid, 'title': title or f'Product {pid}', 'status': status,
        'publishedAt': '2026-01-01T00:00:00Z' if published else None,
        'variants': {'edges': [{'node': {'sku': sku or f'SKU{pid}', 'inventoryQuantity': qty,
                                         'inventoryPolicy': policy, 'inventoryItem': {'tracked': tracked}}}]},
    }
    return normalise_product(node)


def entry(reason=SOLD_OUT, **kw):
    return dict({'sku': 'x', 'title': 'x', 'reason': reason, 'since': '2026-11-20 00:00'}, **kw)


# --- sellability ---

def test_variant_sellable_rules():
    assert variant_sellable({'inventoryQuantity': 3, 'tracked': True, 'inventoryPolicy': 'DENY'})
    assert not variant_sellable({'inventoryQuantity': 0, 'tracked': True, 'inventoryPolicy': 'DENY'})
    assert not variant_sellable({'inventoryQuantity': -2, 'tracked': True, 'inventoryPolicy': 'DENY'})
    assert variant_sellable({'inventoryQuantity': 0, 'tracked': False, 'inventoryPolicy': 'DENY'})
    assert variant_sellable({'inventoryQuantity': 0, 'tracked': True, 'inventoryPolicy': 'CONTINUE'})


def test_pack_product_sellable_if_any_variant_is():
    assert product_sellable([{'inventoryQuantity': 0, 'tracked': True}, {'inventoryQuantity': 5, 'tracked': True}])
    assert not product_sellable([{'inventoryQuantity': 0, 'tracked': True}, {'inventoryQuantity': 0, 'tracked': True}])


def test_decide_reason():
    assert decide_reason(True, True, BOTH) is None
    assert decide_reason(False, True, BOTH) == NOT_RECEIVED
    assert decide_reason(False, False, BOTH) == SOLD_OUT
    assert decide_reason(False, True, SOLD_ONLY) == SOLD_OUT
    assert decide_reason(False, False, RECV_ONLY) is None
    assert decide_reason(False, True, OFF) is None


# --- hiding ---

def test_sold_out_published_product_is_hidden():
    plan = plan_changes([prod(1, qty=0), prod(2, qty=4)], {}, set(), SOLD_ONLY)
    assert [p['id'] for p in plan['hide']] == ['1']
    assert plan['hide'][0]['reason'] == SOLD_OUT


def test_both_off_hides_nothing():
    plan = plan_changes([prod(1, qty=0)], {}, {'1'}, OFF)
    assert plan['hide'] == [] and plan['show'] == []


def test_not_received_rule_only_hides_never_received():
    plan = plan_changes([prod(1), prod(2)], {}, {'1'}, RECV_ONLY)
    assert [(p['id'], p['reason']) for p in plan['hide']] == [('1', NOT_RECEIVED)]


def test_staff_unpublished_product_is_never_touched():
    # Not published and not on our list: staff hid it. Never hidden, never shown.
    plan = plan_changes([prod(1, qty=0, published=False), prod(2, qty=9, published=False)], {}, set(), BOTH)
    assert plan['hide'] == [] and plan['show'] == [] and plan['drop'] == []


def test_draft_and_archived_are_left_alone():
    plan = plan_changes([prod(1, status='DRAFT'), prod(2, status='ARCHIVED')], {}, set(), BOTH)
    assert plan['hide'] == []


def test_clearance_mirrors_are_skipped():
    plan = plan_changes([prod(1, sku='ABC-CLR')], {}, set(), BOTH, clearance_suffix='-CLR')
    assert plan['hide'] == []


def test_cap_limits_hides_per_run_and_reports_the_rest():
    products = [prod(i) for i in range(1, 8)]
    plan = plan_changes(products, {}, set(), SOLD_ONLY, cap=3)
    assert len(plan['hide']) == 3 and plan['capped'] == 4


def test_cap_hides_not_received_first():
    products = [prod(1, title='A'), prod(2, title='B')]
    plan = plan_changes(products, {}, {'2'}, BOTH, cap=1)
    assert [p['id'] for p in plan['hide']] == ['2']


def test_sanity_refuses_when_nearly_everything_reads_unsellable():
    products = [prod(i) for i in range(1, 30)] + [prod(99, qty=5)]
    plan = plan_changes(products, {}, set(), SOLD_ONLY)
    assert plan['hide'] == [] and plan['refused']


def test_sanity_does_not_block_a_normal_share():
    products = [prod(i, qty=5) for i in range(1, 30)] + [prod(99)]
    plan = plan_changes(products, {}, set(), SOLD_ONLY)
    assert [p['id'] for p in plan['hide']] == ['99'] and not plan['refused']


# --- showing again ---

def test_restocked_product_we_hid_is_shown():
    plan = plan_changes([prod(1, qty=3, published=False)], {'1': entry()}, set(), SOLD_ONLY)
    assert [p['id'] for p in plan['show']] == ['1']


def test_still_sold_out_stays_hidden():
    plan = plan_changes([prod(1, qty=0, published=False)], {'1': entry()}, set(), SOLD_ONLY)
    assert plan['show'] == [] and plan['hide'] == [] and plan['drop'] == []


def test_switching_off_shows_our_products_again():
    plan = plan_changes([prod(1, qty=0, published=False)], {'1': entry()}, set(), OFF)
    assert [p['id'] for p in plan['show']] == ['1']


def test_switching_off_one_rule_releases_only_its_products():
    hidden = {'1': entry(SOLD_OUT), '2': entry(NOT_RECEIVED)}
    products = [prod(1, published=False), prod(2, published=False)]
    plan = plan_changes(products, hidden, {'2'}, RECV_ONLY)
    assert [p['id'] for p in plan['show']] == ['1']


def test_not_received_becomes_sold_out_after_receipt_and_sell_through():
    hidden = {'1': entry(NOT_RECEIVED)}
    plan = plan_changes([prod(1, published=False)], hidden, set(), BOTH)
    assert plan['update']['1']['reason'] == SOLD_OUT and plan['show'] == []


def test_hidden_product_drafted_by_clearance_is_left_alone():
    # Clearance drafted the base while it was on our list: keep the entry, do nothing.
    plan = plan_changes([prod(1, status='DRAFT', published=False, qty=0)], {'1': entry()}, set(), BOTH)
    assert plan['show'] == [] and plan['drop'] == [] and plan['update'] == {}


def test_exclusion_restore_then_restock_shows_again():
    # Exclusion archived then released it: back ACTIVE but unpublished, still on our list.
    plan = plan_changes([prod(1, qty=6, published=False)], {'1': entry()}, set(), BOTH)
    assert [p['id'] for p in plan['show']] == ['1']


def test_staff_republished_while_sold_out_is_left_visible():
    plan = plan_changes([prod(1, qty=0, published=True)], {'1': entry()}, set(), SOLD_ONLY)
    assert plan['hide'] == [] and plan['update']['1']['staff_override'] is True
    # next run: still sold out, still left alone
    plan2 = plan_changes([prod(1, qty=0, published=True)], {'1': plan['update']['1']}, set(), SOLD_ONLY)
    assert plan2['hide'] == [] and plan2['drop'] == []
    # once it restocks the override is forgotten, so a later sell-out hides it again
    plan3 = plan_changes([prod(1, qty=4, published=True)], {'1': plan['update']['1']}, set(), SOLD_ONLY)
    assert plan3['drop'] == ['1']


def test_republished_by_someone_else_and_sellable_is_dropped():
    # e.g. clearance restore re-published it with stock: just forget it.
    plan = plan_changes([prod(1, qty=4, published=True)], {'1': entry()}, set(), SOLD_ONLY)
    assert plan['drop'] == ['1'] and plan['show'] == []


def test_deleted_products_are_dropped():
    plan = plan_changes([], {'7': entry()}, set(), BOTH, missing_ids=['7'])
    assert plan['drop'] == ['7']


# --- settings storage ---

def test_settings_default_off_and_hidden_ids_skip_overrides(app_ctx):
    shop = 'test.myshopify.com'
    assert get_settings(shop) == {'sold_out': False, 'not_received': False}
    set_config(CFG_SOLD_OUT, True, shop_url=shop)
    assert get_settings(shop)['sold_out'] is True
    set_config(HIDDEN_KEY, {'1': entry(), '2': entry(staff_override=True)}, shop_url=shop)
    assert hidden_product_ids(shop) == {'1'}
