from unittest.mock import patch

from services import products as P


class FakeModels:
    """Odoo stand-in that cuts off any read over `limit` records, like a reply too big to send."""
    def __init__(self, limit, poison=()):
        self.limit, self.poison, self.calls = limit, set(poison), []

    def execute_kw(self, db, uid, pw, model, method, args, kw):
        ids = args[0]
        self.calls.append(len(ids))
        if len(ids) > self.limit or self.poison & set(ids):
            raise Exception('ProtocolError: 500 Response ended prematurely')
        return [{'id': i} for i in ids]


class FakeOdoo:
    db, uid, password = 'db', 1, 'pw'

    def __init__(self, models):
        self.models = models


def run(ids, limit, poison=(), chunk=10):
    odoo = FakeOdoo(FakeModels(limit, poison))
    with patch.object(P, 'log_event') as log:
        recs, failed = P.bisecting_read(odoo, 'product.product', ids, ['name'], 'shop', 'Batch 1/1',
                                        chunk_size=chunk, entity='Product Sync')
    return recs, failed, log


def test_oversized_chunk_is_split_not_dropped():
    recs, failed, log = run(list(range(50)), limit=3)
    assert sorted(r['id'] for r in recs) == list(range(50))
    assert failed == []
    log.assert_not_called()


def test_only_the_unreadable_product_is_skipped_and_logged():
    recs, failed, log = run(list(range(50)), limit=50, poison=[17])
    assert failed == [17]
    assert len(recs) == 49
    assert log.call_args[0][0] == 'Product Sync'
    assert '17' in log.call_args[0][2]


def test_catch_up_picks_only_products_never_on_shopify():
    cands = [
        {'id': 1, 'default_code': 'C0630'},           # never synced -> pick
        {'id': 2, 'default_code': 'A0001'},           # already linked by id
        {'id': 3, 'default_code': 'C116'},            # SKU already on Shopify via another record
        {'id': 4, 'default_code': False},             # no SKU
        {'id': 5, 'default_code': ' C0631 '},         # whitespace still counts as new
    ]
    ids, found = P.pick_unsynced(cands, mapped_odoo_ids={2, 99}, mapped_skus={'A0001', 'C116'})
    assert ids == [1, 5] and found == 2


def test_catch_up_is_capped_per_run():
    cands = [{'id': i, 'default_code': f'X{i}'} for i in range(10)]
    ids, found = P.pick_unsynced(cands, set(), set(), limit=4)
    assert ids == [0, 1, 2, 3] and found == 10
