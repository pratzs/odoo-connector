"""The paid/not-paid rule for marking Shopify orders paid from Odoo."""
from services.payments import odoo_order_is_paid, shopify_name_from_ref, _dates_close, _so_shopify_name

SO = {'state': 'done', 'delivery_status': 'full'}
DONE = [{'qty_to_invoice': 0.0, 'display_type': False}]


def inv(state='posted', pay='paid', amt=100.0, kind='out_invoice'):
    return {'move_type': kind, 'state': state, 'payment_state': pay, 'amount_total': amt}


def test_fully_invoiced_and_paid():
    ok, _, total = odoo_order_is_paid(SO, [inv()], DONE)
    assert ok and total == 100.0


def test_in_payment_counts_as_paid():
    assert odoo_order_is_paid(SO, [inv(pay='in_payment')], DONE)[0]


def test_unpaid_or_partial_invoice_blocks():
    assert not odoo_order_is_paid(SO, [inv(), inv(pay='not_paid')], DONE)[0]
    assert not odoo_order_is_paid(SO, [inv(pay='partial')], DONE)[0]


def test_still_to_invoice_blocks():
    # Delivered, but a line not yet invoiced.
    lines = DONE + [{'qty_to_invoice': 2.0, 'display_type': False}]
    assert not odoo_order_is_paid(SO, [inv()], lines)[0]


def test_open_backorder_blocks():
    # First half shipped and paid, backorder still to ship.
    assert not odoo_order_is_paid({'state': 'done', 'delivery_status': 'partial'}, [inv()], DONE)[0]


def test_locked_order_with_invoice_status_no_is_judged_on_lines():
    so = {'state': 'done', 'delivery_status': 'full', 'invoice_status': 'no'}
    assert odoo_order_is_paid(so, [inv()], DONE)[0]


def test_section_lines_ignored():
    lines = DONE + [{'qty_to_invoice': 5.0, 'display_type': 'line_section'}]
    assert odoo_order_is_paid(SO, [inv()], lines)[0]


def test_no_invoice_or_draft_only_blocks():
    assert not odoo_order_is_paid(SO, [], DONE)[0]
    assert not odoo_order_is_paid(SO, [inv(state='draft')], DONE)[0]


def test_cancelled_order_blocks():
    assert not odoo_order_is_paid({'state': 'cancel', 'delivery_status': 'full'}, [inv()], DONE)[0]


def test_reversed_invoice_ignored_when_reissued_and_paid():
    moves = [inv(pay='reversed', amt=100), inv(kind='out_refund', pay='paid', amt=100), inv(amt=90)]
    ok, _, total = odoo_order_is_paid(SO, moves, DONE)
    assert ok and total == 90.0


def test_credit_note_reduces_paid_total():
    ok, _, total = odoo_order_is_paid(SO, [inv(amt=100), inv(kind='out_refund', amt=15)], DONE)
    assert ok and total == 85.0


def test_shopify_name_from_ref_shapes():
    assert shopify_name_from_ref('ONLINE_#5572') == '#5572'
    assert shopify_name_from_ref('#2968') == '#2968'
    assert shopify_name_from_ref('ONLINE_#5771 Emma') == '#5771'
    assert shopify_name_from_ref('Missy Email 29th May') is None
    assert shopify_name_from_ref('PO 2968') is None
    assert shopify_name_from_ref(False) is None


def test_dates_close():
    assert _dates_close('2025-12-07 10:00:00', '2025-12-07T01:00:00Z')
    assert not _dates_close('2024-01-01 00:00:00', '2025-12-07T01:00:00Z')


def test_so_name_wins_over_overwritten_ref():
    assert _so_shopify_name({'name': 'ONLINE_#4296', 'client_order_ref': 'Missy Email 29th May'}) == '#4296'
    assert _so_shopify_name({'name': 'S01234', 'client_order_ref': '#2968'}) == '#2968'
