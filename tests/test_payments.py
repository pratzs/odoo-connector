"""The paid/not-paid rule for marking Shopify orders paid from Odoo."""
from services.payments import odoo_order_is_paid

SO = {'state': 'sale', 'invoice_status': 'invoiced'}


def inv(state='posted', pay='paid', amt=100.0, kind='out_invoice'):
    return {'move_type': kind, 'state': state, 'payment_state': pay, 'amount_total': amt}


def test_fully_invoiced_and_paid():
    ok, _, total = odoo_order_is_paid(SO, [inv()])
    assert ok and total == 100.0


def test_in_payment_counts_as_paid():
    assert odoo_order_is_paid(SO, [inv(pay='in_payment')])[0]


def test_unpaid_or_partial_invoice_blocks():
    assert not odoo_order_is_paid(SO, [inv(), inv(pay='not_paid')])[0]
    assert not odoo_order_is_paid(SO, [inv(pay='partial')])[0]


def test_still_to_invoice_blocks():
    # First half delivered and paid, second half not yet invoiced.
    assert not odoo_order_is_paid({'state': 'sale', 'invoice_status': 'to invoice'}, [inv()])[0]


def test_no_invoice_or_draft_only_blocks():
    assert not odoo_order_is_paid(SO, [])[0]
    assert not odoo_order_is_paid(SO, [inv(state='draft')])[0]


def test_cancelled_order_blocks():
    assert not odoo_order_is_paid({'state': 'cancel', 'invoice_status': 'invoiced'}, [inv()])[0]


def test_reversed_invoice_ignored_when_reissued_and_paid():
    moves = [inv(pay='reversed', amt=100), inv(kind='out_refund', pay='paid', amt=100), inv(amt=90)]
    ok, _, total = odoo_order_is_paid(SO, moves)
    assert ok and total == 90.0


def test_credit_note_reduces_paid_total():
    ok, _, total = odoo_order_is_paid(SO, [inv(amt=100), inv(kind='out_refund', amt=15)])
    assert ok and total == 85.0
