from services.self_heal import _update_drift_streaks


def d(sku, site, odoo):
    return {sku: {'sku': sku, 'on_site': site, 'expected': odoo, 'diff': site - odoo}}


def test_same_figures_grow_the_streak():
    s = _update_drift_streaks({}, d('C423', 403, 401))
    s = _update_drift_streaks(s, d('C423', 403, 401))
    s = _update_drift_streaks(s, d('C423', 403, 401))
    assert s['C423']['n'] == 3


def test_moving_figures_restart_the_streak():
    s = _update_drift_streaks({}, d('C423', 403, 401))
    s = _update_drift_streaks(s, d('C423', 403, 401))
    s = _update_drift_streaks(s, d('C423', 400, 398))  # sync corrected, sales moved it
    assert s['C423']['n'] == 1


def test_converged_sku_drops_out():
    s = _update_drift_streaks({}, d('C423', 403, 401))
    assert _update_drift_streaks(s, {}) == {}


def test_legacy_int_state_restarts_at_one():
    assert _update_drift_streaks({'C423': 2}, d('C423', 403, 401))['C423']['n'] == 1
