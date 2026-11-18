"""
target_dates.py のテスト: cron が何時間遅れても、狙った開催日が対象になること。

  python -m unittest discover -s tests -v
"""

import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scraper"))

import target_dates as td  # noqa: E402


def jst(y, m, d, hh, mm=0):
    return datetime(y, m, d, hh, mm, tzinfo=td.JST)


# 2026-11-13(金) / 14(土) / 15(日) / 16(月)
class ResultDatesTest(unittest.TestCase):
    def test_on_time_saturday_evening_targets_saturday(self):
        self.assertIn("20261114", td.result_dates(jst(2026, 11, 14, 18, 0)))

    def test_actual_delayed_run_targets_saturday_not_sunday(self):
        # 実測: 土曜18:00予定のジョブが日曜00:22に起動し、日曜を見に行っていた
        self.assertEqual(td.result_dates(jst(2026, 11, 15, 0, 22)), ["20261114"])

    def test_sunday_job_delayed_into_monday_targets_sunday(self):
        self.assertEqual(td.result_dates(jst(2026, 11, 16, 1, 30)), ["20261115"])

    def test_on_time_sunday_run_also_retries_saturday(self):
        self.assertEqual(td.result_dates(jst(2026, 11, 15, 18, 40)),
                         ["20261114", "20261115"])

    def test_any_delay_up_to_22h_keeps_the_race_day(self):
        # 土曜18:00予定 → 日曜16:59までのどの時刻に走っても土曜ぶんを取り、
        # レースが終わっていない日(当日17時前)は対象にしない
        scheduled = jst(2026, 11, 14, 18, 0)
        for minutes in range(0, 23 * 60, 7):
            now = scheduled + timedelta(minutes=minutes)
            dates = td.result_dates(now)
            self.assertIn("20261114", dates, now.isoformat())
            if now.hour < td.RESULTS_READY_HOUR:
                self.assertNotIn(now.strftime("%Y%m%d"), dates, now.isoformat())

    def test_uses_jst_even_when_given_utc(self):
        # 土曜 15:22 UTC = 日曜 00:22 JST
        now = datetime(2026, 11, 14, 15, 22, tzinfo=td.timezone.utc)
        self.assertEqual(td.result_dates(now), ["20261114"])


class ForwardDateTest(unittest.TestCase):
    def test_on_time_friday_evening_targets_saturday(self):
        self.assertEqual(td.forward_date(jst(2026, 11, 13, 18, 0)), "20261114")

    def test_actual_delayed_friday_run_targets_saturday_not_sunday(self):
        # 実測: 金曜18:00予定のジョブが土曜01:54に起動し、日曜を取りに行っていた
        self.assertEqual(td.forward_date(jst(2026, 11, 14, 1, 54)), "20261114")

    def test_saturday_evening_job_targets_sunday_on_time_or_late(self):
        self.assertEqual(td.forward_date(jst(2026, 11, 14, 18, 0)), "20261115")
        self.assertEqual(td.forward_date(jst(2026, 11, 15, 0, 22)), "20261115")

    def test_any_delay_up_to_15h_keeps_next_race_day(self):
        scheduled = jst(2026, 11, 13, 18, 0)
        for minutes in range(0, 15 * 60, 7):  # 〜土曜08:59
            now = scheduled + timedelta(minutes=minutes)
            self.assertEqual(td.forward_date(now), "20261114", now.isoformat())

    def test_morning_refresh_targets_today(self):
        self.assertEqual(td.forward_date(jst(2026, 11, 14, 8, 0)), "20261114")


class PreviewDatesTest(unittest.TestCase):
    """X前日ポスト: (投稿対象のレース日, バイアス分析の参照日)"""

    def test_on_time_runs_match_previous_behaviour(self):
        # 金曜20時 → 土曜のレース / 先週日曜(旧: 実行日の5日前)
        self.assertEqual(td.preview_dates(jst(2026, 11, 13, 20, 0)),
                         ("20261114", "20261108"))
        # 土曜20時 → 日曜のレース / 当日(土曜)
        self.assertEqual(td.preview_dates(jst(2026, 11, 14, 20, 0)),
                         ("20261115", "20261114"))

    def test_actual_delayed_friday_post_targets_saturday(self):
        # 実測: 金曜20:00予定の投稿ジョブが土曜03:26に起動。旧実装は日曜を
        # 対象にし、参照日も「当日(土曜)」になっていた
        self.assertEqual(td.preview_dates(jst(2026, 11, 14, 3, 26)),
                         ("20261114", "20261108"))

    def test_delayed_saturday_post_targets_sunday(self):
        # 実測: 土曜20:00予定 → 日曜01:53起動。旧実装は月曜を対象にしていた
        self.assertEqual(td.preview_dates(jst(2026, 11, 15, 1, 53)),
                         ("20261115", "20261114"))

    def test_any_delay_until_first_race_keeps_target(self):
        scheduled = jst(2026, 11, 13, 20, 0)
        for minutes in range(0, 13 * 60, 7):  # 〜土曜08:59
            now = scheduled + timedelta(minutes=minutes)
            self.assertEqual(td.preview_dates(now), ("20261114", "20261108"),
                             now.isoformat())


if __name__ == "__main__":
    unittest.main()
