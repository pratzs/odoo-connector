import pytest

from services.products import (
    _clean_exclusion_rules, build_exclusion_domain, build_exclusion_match_domain, get_exclusion_match,
)

RULES = [
    {'field': 'category', 'op': 'contains', 'value': 'General Merchandise'},
    {'field': 'price', 'op': 'lte', 'value': '0'},
    {'field': 'tag', 'op': 'eq', 'value': 'seasonal'},
]


def prod(price=5, categ='PRODUCT BU / Food', tags=()):
    return {'list_price': price, 'categ_id': [1, categ] if categ else False}, list(tags)


def match(p, rules=RULES):
    product, tags = p
    return get_exclusion_match(product, _clean_exclusion_rules(rules), tags)


def test_normal_product_is_not_excluded():
    assert match(prod()) is None


def test_general_merchandise_category_is_excluded():
    assert match(prod(categ='PRODUCT BU / General Merchandise'))


def test_zero_price_is_excluded_but_positive_price_syncs():
    assert match(prod(price=0))
    assert match(prod(price=0.01)) is None  # price rises above 0 -> syncs


def test_tag_rule_is_case_insensitive():
    assert match(prod(tags=['Seasonal']))
    assert match(prod(tags=['Core'])) is None


def test_missing_category_does_not_crash():
    assert match(prod(categ=None)) is None


def test_no_rules_excludes_nothing():
    assert match(prod(price=0, categ='PRODUCT BU / General Merchandise'), rules=[]) is None


def test_invalid_rules_are_dropped():
    junk = [
        {'field': 'price', 'op': 'lte', 'value': 'abc'},
        {'field': 'price', 'op': 'nope', 'value': '1'},
        {'field': 'colour', 'op': 'eq', 'value': 'red'},
        {'field': 'tag', 'op': 'eq', 'value': '  '},
        'not a dict',
    ]
    assert _clean_exclusion_rules(junk) == []
    assert _clean_exclusion_rules('garbage') == []
    assert _clean_exclusion_rules(None) == []


def test_domain_is_empty_without_rules():
    assert build_exclusion_domain([]) == []


def test_domain_shape():
    assert build_exclusion_domain(RULES) == [
        '!', ['categ_id.complete_name', 'ilike', 'General Merchandise'],
        '!', ['list_price', '<=', 0.0],
        '!', ['product_tag_ids.name', '=ilike', 'seasonal'],
    ]


def test_like_wildcards_are_escaped_in_exact_match():
    d = build_exclusion_domain([{'field': 'tag', 'op': 'eq', 'value': '50%_off'}])
    assert d[1][2] == '50\\%\\_off'


# --- parity: the Odoo-side domain and the per-product guard must agree ---
def _eval_domain(domain, product, tags):
    """Tiny evaluator for the leaves build_exclusion_domain emits ('!' + one leaf, ANDed)."""
    ops = {'<': lambda a, b: a < b, '<=': lambda a, b: a <= b, '=': lambda a, b: a == b,
           '>': lambda a, b: a > b, '>=': lambda a, b: a >= b}

    def leaf(l):
        f, op, v = l
        if f == 'list_price':
            return ops[op](float(product['list_price']), v)
        if f == 'categ_id.complete_name':
            name = product['categ_id'][1].lower() if product['categ_id'] else ''
            return v.lower() in name if op == 'ilike' else name == v.lower()
        names = [t.lower() for t in tags]
        return any(v.lower() in n for n in names) if op == 'ilike' else v.lower() in names

    it = iter(domain)
    keep = True
    for tok in it:
        assert tok == '!'
        matched = leaf(next(it))  # always consume the leaf, even once keep is already False
        keep = keep and not matched
    return keep


@pytest.mark.parametrize('rules', [
    RULES,
    [{'field': 'category', 'op': 'eq', 'value': 'product bu / general merchandise'}],
    [{'field': 'price', 'op': 'lt', 'value': '2'}, {'field': 'tag', 'op': 'contains', 'value': 'sea'}],
    [{'field': 'price', 'op': 'gte', 'value': '100'}],
])
@pytest.mark.parametrize('price', [0, 0.01, 1.99, 2, 5, 100])
@pytest.mark.parametrize('categ', ['PRODUCT BU / General Merchandise', 'PRODUCT BU / Food', None])
@pytest.mark.parametrize('tags', [[], ['Seasonal'], ['Core', 'seasonal sale']])
def test_domain_and_guard_agree(rules, price, categ, tags):
    product, tag_names = prod(price, categ, tags)
    guard_excludes = get_exclusion_match(product, _clean_exclusion_rules(rules), tag_names) is not None
    domain_keeps = _eval_domain(build_exclusion_domain(rules), product, tag_names)
    assert guard_excludes == (not domain_keeps)


# --- archiving products that are already in Shopify ---
from types import SimpleNamespace

import services.products as sp_mod
from models import ProductMap


def test_match_domain_is_an_or_of_the_rules():
    assert build_exclusion_match_domain([]) == []
    one = build_exclusion_match_domain([RULES[1]])
    assert one == [['list_price', '<=', 0.0]]
    three = build_exclusion_match_domain(RULES)
    assert three[:2] == ['|', '|'] and len(three) == 5


class FakeShopify:
    """Stands in for the shopify module: variant id -> product id, products keep a status."""
    def __init__(self, statuses):
        self.statuses = statuses  # product_id -> status
        self.saved = []
        outer = self

        class Variant:
            @staticmethod
            def find(vid):
                return SimpleNamespace(product_id=int(vid) + 1000)

        class Product:
            @staticmethod
            def find(pid):
                obj = SimpleNamespace(id=pid, status=outer.statuses[pid])
                obj.save = lambda: (outer.statuses.__setitem__(pid, obj.status), outer.saved.append(pid))
                return obj

        self.Variant, self.Product = Variant, Product


class FakeOdoo:
    db, uid, password = 'db', 1, 'pw'

    def __init__(self, matching_ids):
        self.matching_ids = matching_ids
        self.last_domain = None
        outer = self

        class Models:
            @staticmethod
            def execute_kw(db, uid, pw, model, method, args, kw=None):
                outer.last_domain = args[0]
                allowed = args[0][0][2]
                return [{'id': i, 'default_code': f'SKU{i}'} for i in outer.matching_ids if i in allowed]

        self.models = Models()


@pytest.fixture
def archive_env(app_ctx, monkeypatch):
    from models import db
    logs = []
    monkeypatch.setattr(sp_mod, 'log_event', lambda *a, **k: logs.append(a))
    monkeypatch.setattr(sp_mod, 'setup_shopify_session', lambda shop: True)
    cfg = {'prod_exclusion_archive': True}
    monkeypatch.setattr(sp_mod, 'get_config', lambda key, default=None, shop_url=None: cfg.get(key, default))
    for odoo_id in (1, 2, 3):
        db.session.add(ProductMap(shop_url='s.myshopify.com', odoo_product_id=odoo_id, sku=f'SKU{odoo_id}',
                                  shopify_variant_id=str(odoo_id)))
    db.session.commit()
    return SimpleNamespace(logs=logs, cfg=cfg, monkeypatch=monkeypatch)


def run_archive(env, statuses, matching):
    fake = FakeShopify(statuses)
    env.monkeypatch.setattr(sp_mod, 'shopify', fake)
    odoo = FakeOdoo(matching)
    n = sp_mod._archive_excluded_products_in_shopify('s.myshopify.com', odoo, RULES)
    return n, fake, odoo


def test_matching_products_in_shopify_get_archived(archive_env):
    n, fake, odoo = run_archive(archive_env, {1001: 'active', 1002: 'active', 1003: 'active'}, matching=[1, 3])
    assert n == 2
    assert fake.statuses == {1001: 'archived', 1002: 'active', 1003: 'archived'}
    # search is limited to products already mapped to Shopify
    assert sorted(odoo.last_domain[0][2]) == [1, 2, 3]


def test_already_archived_products_are_left_alone(archive_env):
    n, fake, _ = run_archive(archive_env, {1001: 'archived', 1002: 'active', 1003: 'active'}, matching=[1])
    assert n == 0 and fake.saved == []


def test_switch_off_archives_nothing(archive_env):
    archive_env.cfg['prod_exclusion_archive'] = False
    n, fake, _ = run_archive(archive_env, {1001: 'active', 1002: 'active', 1003: 'active'}, matching=[1, 2, 3])
    assert n == 0 and fake.saved == []


def test_no_rules_archives_nothing(archive_env):
    fake = FakeShopify({1001: 'active'})
    archive_env.monkeypatch.setattr(sp_mod, 'shopify', fake)
    assert sp_mod._archive_excluded_products_in_shopify('s.myshopify.com', FakeOdoo([1]), []) == 0
    assert fake.saved == []


def test_safety_cap_refuses_a_runaway_rule(archive_env):
    archive_env.monkeypatch.setattr(sp_mod, 'EXCLUSION_ARCHIVE_MAX', 1)
    n, fake, _ = run_archive(archive_env, {1001: 'active', 1002: 'active', 1003: 'active'}, matching=[1, 2])
    assert n == 0 and fake.saved == []
    assert any('safety limit' in str(l) for l in archive_env.logs)
