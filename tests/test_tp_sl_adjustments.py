import unittest


class TestTPStopAdjustments(unittest.TestCase):
    def test_adaptive_tp_buffer_pct_penny(self):
        from core.executor import _adaptive_tp_buffer_pct

        self.assertAlmostEqual(_adaptive_tp_buffer_pct(9.99), 0.01, places=8)
        self.assertAlmostEqual(_adaptive_tp_buffer_pct(1.0), 0.01, places=8)

    def test_adaptive_tp_buffer_pct_regular(self):
        from core.executor import _adaptive_tp_buffer_pct

        self.assertAlmostEqual(_adaptive_tp_buffer_pct(10.0), 0.005, places=8)
        self.assertAlmostEqual(_adaptive_tp_buffer_pct(250.0), 0.005, places=8)

    def test_adjust_tp_buy_regular_triggers_when_too_close(self):
        from core.executor import _adjust_tp_for_broker_limits

        entry = 100.0
        tp_close = 100.20  # 0.2% < 0.3% trigger
        tp_new, adjusted, req = _adjust_tp_for_broker_limits(
            action="BUY",
            entry_price=entry,
            tp_level=tp_close,
            symbol="AAPL",
            min_dist=None,
            atr_value=None,
        )
        self.assertTrue(adjusted)
        self.assertGreaterEqual(abs(tp_new - entry), req)
        self.assertAlmostEqual(tp_new, entry * (1 + 0.005), places=8)

    def test_adjust_tp_sell_regular_triggers_when_too_close(self):
        from core.executor import _adjust_tp_for_broker_limits

        entry = 100.0
        tp_close = 99.80  # 0.2% < 0.3% trigger
        tp_new, adjusted, req = _adjust_tp_for_broker_limits(
            action="SELL",
            entry_price=entry,
            tp_level=tp_close,
            symbol="AAPL",
            min_dist=None,
            atr_value=None,
        )
        self.assertTrue(adjusted)
        self.assertGreaterEqual(abs(tp_new - entry), req)
        self.assertAlmostEqual(tp_new, entry * (1 - 0.005), places=8)

    def test_adjust_tp_penny_uses_1pct_floor(self):
        from core.executor import _adjust_tp_for_broker_limits

        entry = 5.0
        tp_close = 5.01  # 0.2% close
        tp_new, adjusted, req = _adjust_tp_for_broker_limits(
            action="BUY",
            entry_price=entry,
            tp_level=tp_close,
            symbol="PTON",
            min_dist=None,
            atr_value=None,
        )
        self.assertTrue(adjusted)
        self.assertAlmostEqual(req, entry * 0.01, places=8)
        self.assertAlmostEqual(tp_new, entry * (1 + 0.01), places=8)

    def test_adjust_tp_respects_broker_min_dist_when_larger(self):
        from core.executor import _adjust_tp_for_broker_limits

        entry = 100.0
        tp_close = 100.10  # too close
        tp_new, adjusted, req = _adjust_tp_for_broker_limits(
            action="BUY",
            entry_price=entry,
            tp_level=tp_close,
            symbol="AAPL",
            min_dist=1.2,  # larger than 0.5% = 0.5
            atr_value=None,
        )
        self.assertTrue(adjusted)
        self.assertAlmostEqual(req, 1.2, places=8)
        self.assertAlmostEqual(tp_new, entry + 1.2, places=8)

    def test_adjust_tp_respects_atr_floor_when_larger(self):
        from core.executor import _adjust_tp_for_broker_limits

        entry = 100.0
        tp_close = 100.10
        tp_new, adjusted, req = _adjust_tp_for_broker_limits(
            action="BUY",
            entry_price=entry,
            tp_level=tp_close,
            symbol="AAPL",
            min_dist=None,
            atr_value=50.0,  # atr_floor=2.5 dominates
        )
        self.assertTrue(adjusted)
        self.assertAlmostEqual(req, 2.5, places=8)
        self.assertAlmostEqual(tp_new, entry + 2.5, places=8)

    def test_adjust_tp_no_change_when_far_enough(self):
        from core.executor import _adjust_tp_for_broker_limits

        entry = 100.0
        tp_ok = 101.0  # 1% away
        tp_new, adjusted, _ = _adjust_tp_for_broker_limits(
            action="BUY",
            entry_price=entry,
            tp_level=tp_ok,
            symbol="AAPL",
            min_dist=None,
            atr_value=None,
        )
        self.assertFalse(adjusted)
        self.assertAlmostEqual(tp_new, tp_ok, places=8)

    def test_rebalance_sl_buy_preserves_rr(self):
        from core.executor import _rebalance_sl_for_target_rr

        entry = 100.0
        tp = 101.0  # dist=1.0
        sl_old = 99.0  # dist=1.0 -> rr=1
        sl_new, adjusted = _rebalance_sl_for_target_rr(
            action="BUY",
            entry_price=entry,
            stop_level=sl_old,
            tp_level=tp,
            target_rr=2.0,
            min_dist=None,
            max_dist=None,
        )
        self.assertTrue(adjusted)
        self.assertAlmostEqual(abs(entry - sl_new), 0.5, places=8)
        self.assertAlmostEqual(sl_new, 99.5, places=8)

    def test_rebalance_sl_sell_preserves_rr(self):
        from core.executor import _rebalance_sl_for_target_rr

        entry = 100.0
        tp = 99.0  # dist=1.0
        sl_old = 101.0  # dist=1.0 -> rr=1
        sl_new, adjusted = _rebalance_sl_for_target_rr(
            action="SELL",
            entry_price=entry,
            stop_level=sl_old,
            tp_level=tp,
            target_rr=2.0,
            min_dist=None,
            max_dist=None,
        )
        self.assertTrue(adjusted)
        self.assertAlmostEqual(abs(entry - sl_new), 0.5, places=8)
        self.assertAlmostEqual(sl_new, 100.5, places=8)

    def test_rebalance_sl_respects_min_dist(self):
        from core.executor import _rebalance_sl_for_target_rr

        entry = 100.0
        tp = 101.0  # dist=1.0 -> desired SL dist=0.5, but min_dist=0.8
        sl_old = 99.0
        sl_new, adjusted = _rebalance_sl_for_target_rr(
            action="BUY",
            entry_price=entry,
            stop_level=sl_old,
            tp_level=tp,
            target_rr=2.0,
            min_dist=0.8,
            max_dist=None,
        )
        self.assertTrue(adjusted)
        self.assertAlmostEqual(abs(entry - sl_new), 0.8, places=8)
        self.assertAlmostEqual(sl_new, 99.2, places=8)

    def test_rebalance_sl_respects_max_dist(self):
        from core.executor import _rebalance_sl_for_target_rr

        entry = 100.0
        tp = 120.0  # dist=20 -> desired SL dist=10, but max_dist=3
        sl_old = 80.0
        sl_new, adjusted = _rebalance_sl_for_target_rr(
            action="BUY",
            entry_price=entry,
            stop_level=sl_old,
            tp_level=tp,
            target_rr=2.0,
            min_dist=None,
            max_dist=3.0,
        )
        self.assertTrue(adjusted)
        self.assertAlmostEqual(abs(entry - sl_new), 3.0, places=8)
        self.assertAlmostEqual(sl_new, 97.0, places=8)

    def test_validate_broker_limits_buy_ok(self):
        from core.executor import validate_broker_limits

        entry = 100.0
        sl = 99.0
        tp = 101.0
        ok, details = validate_broker_limits(entry, sl, tp, "AAPL")
        self.assertTrue(ok)
        self.assertIn("min_legal_distance", details)

    def test_validate_broker_limits_rejects_too_close(self):
        from core.executor import validate_broker_limits

        entry = 100.0
        sl = 99.99
        tp = 100.01
        ok, details = validate_broker_limits(entry, sl, tp, "AAPL")
        self.assertFalse(ok)
        self.assertGreater(details["min_legal_distance"], 0.0)


if __name__ == "__main__":
    unittest.main()

