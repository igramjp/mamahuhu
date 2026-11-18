"""
backfill.py のテスト(ネットワークには出ない)。

  python -m unittest discover -s tests -v
"""

import contextlib
import io
import re
import sys
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scraper"))

import backfill  # noqa: E402
import db  # noqa: E402
from scrape import decode_html  # noqa: E402


# ---------- フィクスチャ(db.netkeiba の実ページ構造を最小限に再現) ----------
def race_html(name="2歳未勝利", n=3):
    rows = "".join(
        f"<tr><td>{i}</td><td>{i}</td><td>{i}</td><td>馬{i}</td><td>牡2</td>"
        f"<td>55</td><td>騎手{i}</td><td>1:09.{i}</td><td></td><td>1-1</td>"
        f"<td>35.{i}</td><td>{i}.5</td><td>{i}</td><td>470(0)</td></tr>"
        for i in range(1, n + 1))
    return f"""<html><head><title>{name}｜2026年11月14日 | 競馬データベース - netkeiba</title></head>
<body><div class="data_intro"><h1>{name}</h1>
<p><diary_snap_cut><span>ダ右1700m / 天候 : 晴 / ダート : 良 / 発走 : 09:40</span></diary_snap_cut></p></div>
<table class="race_table_01"><tr><th>着順</th><th>枠番</th><th>馬番</th><th>馬名</th>
<th>性齢</th><th>斤量</th><th>騎手</th><th>タイム</th><th>着差</th><th>通過</th>
<th>上り</th><th>単勝</th><th>人気</th><th>馬体重</th></tr>{rows}</table>
<table class="pay_table_01"><tr><th>単勝</th><td>1</td><td>150</td><td>1</td></tr></table>
</body></html>"""


# 存在しないrace_idに対して db.netkeiba が HTTP 200 で返す雛形ページ
MISSING_HTML = """<html><head><title>｜1970年1月01日 | 競馬データベース - netkeiba</title></head>
<body><div class="data_intro"><h1></h1><p> / 天候 :  / </p></div>netkeibaについて</body></html>"""

BLOCK_HTML = "<html><body><h1>Too many requests</h1></body></html>"


def list_html(ids):
    links = "".join(f'<a href="/race/{i}/">R</a>' for i in ids)
    # 地方(場コード45)のリンクは無視されること
    return (f"<html><head><title>レース情報 - netkeiba</title></head><body>{links}"
            f'<a href="/race/202645111301/">地方</a></body></html>')


def response(status=200, text="", encoding="utf-8"):
    r = requests.Response()
    r.status_code = status
    r._content = text.encode(encoding)
    r.url = "https://db.netkeiba.com/x"
    return r


def ids_for(day_no, n=12):
    return [f"2026030303{r:02d}" if day_no == 3 else f"2026030304{r:02d}"
            for r in range(1, n + 1)]


# ---------- 3. 日付範囲(JST) ----------
class DateRangeTest(unittest.TestCase):
    def test_jst_today_differs_from_utc_date_in_the_morning(self):
        # 火曜 00:30 JST = 月曜 15:30 UTC。UTCの日付(16日)ではなくJSTの17日
        now = datetime(2026, 11, 16, 15, 30, tzinfo=timezone.utc)
        self.assertEqual(backfill.jst_today(now), date(2026, 11, 17))
        self.assertEqual(backfill.default_to(now), "20261117")

    def test_does_not_depend_on_process_timezone(self):
        now = datetime(2026, 11, 17, 9, 0, tzinfo=timezone.utc)  # 火曜18:00 JST
        for offset in (-8, 0, 9):
            local = now.astimezone(timezone(timedelta(hours=offset)))
            self.assertEqual(backfill.jst_today(local), date(2026, 11, 17))

    def test_tuesday_job_range_contains_last_weekend(self):
        # 火曜の定期実行(09:00 UTC = 18:00 JST)。旧実装は --to が金曜止まりだった
        now = datetime(2026, 11, 17, 9, 0, tzinfo=timezone.utc)
        d_from, d_to = backfill.recent_range(8, now)
        self.assertEqual((d_from, d_to), ("20261109", "20261117"))
        self.assertLessEqual(d_from, "20261114")  # 土
        self.assertGreaterEqual(d_to, "20261115")  # 日

    def test_range_always_contains_most_recent_saturday_and_sunday(self):
        # 4週間ぶんの毎時(cronの遅延・日付境界を含む)で、直近の土曜と日曜が
        # (当日が土日ならその日も含め)必ず [from, to] に入る
        t = datetime(2026, 11, 1, 0, 0, tzinfo=timezone.utc)
        end = t + timedelta(days=28)
        while t < end:
            today = backfill.jst_today(t)
            d_from, d_to = backfill.recent_range(8, t)
            for weekday in (5, 6):  # 土, 日
                recent = today - timedelta(days=(today.weekday() - weekday) % 7)
                ymd = recent.strftime("%Y%m%d")
                self.assertTrue(d_from <= ymd <= d_to,
                                f"now={t.isoformat()} {ymd} not in {d_from}〜{d_to}")
            t += timedelta(hours=1)


# ---------- 文字コード ----------
class DecodeTest(unittest.TestCase):
    def test_utf8_and_eucjp_both_decode(self):
        self.assertIn("馬番", decode_html("着順 枠番 馬番".encode("utf-8")))
        self.assertIn("馬番", decode_html("着順 枠番 馬番".encode("euc_jp")))

    def test_parse_real_structure(self):
        race, results = backfill.parse_race_page(race_html(n=3), "202603030301", "20261114")
        self.assertEqual((race["place"], race["surface"], race["distance"]),
                         ("福島", "ダート", 1700))
        self.assertEqual(race["track_condition"], "良")
        self.assertEqual(len(results), 3)
        self.assertEqual(results[0]["horse"], "馬1")

    def test_mojibake_is_a_parse_failure_not_missing(self):
        # 2026-11 の障害の再現: UTF-8のページをEUC-JPとして読んだ状態
        garbled = race_html().encode("utf-8").decode("euc_jp", errors="replace")
        with self.assertRaises(ValueError) as cm:
            backfill.parse_race_page(garbled, "202603030301", "20261114")
        self.assertIn("列名が想定外", str(cm.exception))


# ---------- 2. 不存在 / ブロックの区別 ----------
class ClassifyTest(unittest.TestCase):
    def fetch_with(self, *responses):
        with mock.patch.object(backfill.requests, "get", side_effect=list(responses)) as g, \
                mock.patch.object(backfill.time, "sleep"):
            try:
                return backfill.fetch("https://db.netkeiba.com/race/x/"), g.call_count
            except Exception as e:
                e.calls = g.call_count
                raise

    def test_403_and_429_are_blocked_without_retry(self):
        for status in (403, 429):
            with self.assertRaises(backfill.BlockedError) as cm:
                self.fetch_with(response(status))
            self.assertEqual(cm.exception.calls, 1)

    def test_200_non_netkeiba_body_is_blocked(self):
        with self.assertRaises(backfill.BlockedError):
            self.fetch_with(response(200, BLOCK_HTML))

    def test_404_is_not_found(self):
        with self.assertRaises(backfill.RaceNotFoundError) as cm:
            self.fetch_with(response(404))
        self.assertEqual(cm.exception.calls, 1)

    def test_5xx_is_retried_then_succeeds(self):
        html, calls = self.fetch_with(response(503), response(200, race_html()))
        self.assertEqual(calls, 2)
        self.assertIn("馬番", html)

    def test_eucjp_response_still_readable(self):
        html, _ = self.fetch_with(response(200, race_html(), encoding="euc_jp"))
        self.assertIn("馬番", html)

    def test_placeholder_page_is_not_found(self):
        with self.assertRaises(backfill.RaceNotFoundError):
            backfill.parse_race_page(MISSING_HTML, "209912310101", "20991231")

    def test_list_page_enumerates_only_real_jra_ids(self):
        ids = ids_for(3, 2)
        with mock.patch.object(backfill, "fetch", return_value=list_html(ids)):
            self.assertEqual(backfill.get_all_race_ids("20261114"), ids)


# ---------- run_backfill 全体(1/2/4) ----------
class RunBackfillTest(unittest.TestCase):
    NOW = datetime(2026, 11, 17, 9, 0, tzinfo=timezone.utc)  # 火曜18:00 JST

    def run_with(self, pages, date_from="20261113", date_to="20261115", lists=None):
        """pages: {race_id: html | Exception}。未指定のレースは正常ページ。
        lists: {yyyymmdd: [race_id]}。戻り値 (stats, 出力, exit_code, conn, 取得URL)。"""
        lists = lists if lists is not None else {
            "20261114": ids_for(3), "20261115": ids_for(4)}
        fetched = []

        def fake_fetch(url, retries=3):
            fetched.append(url)
            m = re.search(r"/race/list/(\d{8})/", url)
            if m:
                return list_html(lists.get(m.group(1), []))
            rid = re.search(r"/race/(\d{12})/", url).group(1)
            page = pages.get(rid, race_html())
            if isinstance(page, Exception):
                raise page
            return page

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = str(Path(tmp.name) / "t.db")
        out = io.StringIO()
        stats, code = None, 0
        with mock.patch.object(backfill, "fetch", side_effect=fake_fetch), \
                mock.patch.object(backfill.time, "sleep"), \
                contextlib.redirect_stdout(out):
            try:
                stats = backfill.run_backfill(date_from, date_to, path, now=self.NOW)
            except SystemExit as e:
                code = e.code
        conn = db.connect(path)
        self.addCleanup(conn.close)
        return stats, out.getvalue(), code, conn, fetched

    @staticmethod
    def summary_counts(out):
        m = re.search(r"今回: ok=(\d+) fail=(\d+) 不存在=(\d+) blocked=(\d+) skip=(\d+)", out)
        return tuple(int(x) for x in m.groups())

    def test_empty_list_day_is_skipped_normally(self):
        stats, out, code, conn, fetched = self.run_with({})
        self.assertEqual(code, 0)
        self.assertEqual((stats.ok, stats.fail, stats.missing, stats.blocked), (24, 0, 0, 0))
        # 11/13(金)は一覧が空 → レースページへは1件もアクセスしない
        self.assertEqual(sum("/race/list/" in u for u in fetched), 3)
        self.assertEqual(len(fetched), 3 + 24)
        # 直近の「開催なし」は確定させない(db未反映の可能性)
        self.assertNotIn("20261113", db.checked_dates(conn))
        self.assertEqual(db.checked_dates(conn)["20261114"], 12)

    def test_old_empty_day_is_cached(self):
        _, _, _, conn, _ = self.run_with({}, "20261001", "20261001", lists={})
        self.assertEqual(db.checked_dates(conn)["20261001"], 0)

    def test_missing_races_are_skipped_and_never_stop(self):
        # 土曜12Rすべてが不存在でも停止せず、日曜を取り切る
        pages = {rid: MISSING_HTML for rid in ids_for(3)}
        stats, out, code, conn, _ = self.run_with(pages)
        self.assertEqual(code, 0)
        self.assertEqual((stats.ok, stats.fail, stats.missing), (12, 0, 12))
        self.assertEqual(out.count("  不存在 "), 12)
        self.assertNotIn("FAIL", out)
        # 不存在は failures に記録しない
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM failures").fetchone()[0], 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM races").fetchone()[0], 12)

    def test_block_stops_after_consecutive_blocks(self):
        sat = ids_for(3)
        pages = {rid: backfill.BlockedError("HTTP 429") for rid in sat[2:]}
        stats, out, code, conn, fetched = self.run_with(pages)
        self.assertEqual(code, 2)
        self.assertIn("レート制限のため停止", out)
        n = backfill.MAX_CONSECUTIVE_BLOCKS
        self.assertEqual(out.count("  BLOCKED "), n)
        # 停止後は一切アクセスしない(一覧1(金)+1(土) + 成功2 + ブロックn)
        self.assertEqual(len(fetched), 2 + 2 + n)
        # 途中停止でも当日ぶんがサマリに載る(旧実装は ok=0 fail=0 になっていた)
        self.assertEqual(self.summary_counts(out), (2, 0, 0, n, 0))
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM failures").fetchone()[0], 0)

    def test_missing_between_blocks_does_not_reset_or_count(self):
        sat = ids_for(3)
        pages = {rid: backfill.BlockedError("HTTP 403") for rid in sat}
        pages[sat[1]] = MISSING_HTML
        _, out, code, _, _ = self.run_with(pages)
        self.assertEqual(code, 2)
        self.assertEqual(out.count("  BLOCKED "), backfill.MAX_CONSECUTIVE_BLOCKS)
        self.assertEqual(out.count("  不存在 "), 1)

    def test_summary_fail_count_matches_fail_log_lines(self):
        sat, sun = ids_for(3), ids_for(4)
        bad = "<html><head><title>壊れたページ - netkeiba</title></head><body></body></html>"
        pages = {sat[0]: bad, sat[5]: bad, sun[3]: bad, sun[7]: MISSING_HTML}
        stats, out, code, conn, _ = self.run_with(pages)
        self.assertEqual(code, 0)
        n_fail_lines = len(re.findall(r"^\s*(?:\d{8}: )?FAIL ", out, flags=re.M))
        self.assertEqual(n_fail_lines, 3)
        self.assertEqual(self.summary_counts(out), (20, 3, 1, 0, 0))
        self.assertEqual(stats.fail, n_fail_lines)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM failures").fetchone()[0], 3)

    def test_summary_matches_log_when_aborted_midway(self):
        # 2026-11-17 の障害の再現: 全ページがパース不能 → 連続FAILで停止(exit 3)。
        # ブロック扱いにはせず、サマリの fail は FAIL ログ行数と一致する
        garbled = race_html().encode("utf-8").decode("euc_jp", errors="replace")
        pages = {rid: garbled for rid in ids_for(3) + ids_for(4)}
        _, out, code, _, _ = self.run_with(pages)
        self.assertEqual(code, 3)
        self.assertNotIn("レート制限", out)
        n_fail_lines = out.count("  FAIL ")
        self.assertEqual(n_fail_lines, backfill.MAX_CONSECUTIVE_FAILS)
        self.assertEqual(self.summary_counts(out), (0, n_fail_lines, 0, 0, 0))

    def test_rerun_skips_done_and_upgrades_realtime_rows(self):
        _, _, _, conn, _ = self.run_with({})
        path = conn.execute("PRAGMA database_list").fetchone()[2]
        # 土曜ぶんを週末realtime取り込みの暫定行に戻す → 取り直し対象になること
        with conn:
            conn.execute("UPDATE races SET source='realtime' WHERE date='20261114'")
        fetched = []

        def fake_fetch(url, retries=3):
            fetched.append(url)
            m = re.search(r"/race/list/(\d{8})/", url)
            if m:
                return list_html({"20261114": ids_for(3), "20261115": ids_for(4)}
                                 .get(m.group(1), []))
            return race_html()

        with mock.patch.object(backfill, "fetch", side_effect=fake_fetch), \
                mock.patch.object(backfill.time, "sleep"), \
                contextlib.redirect_stdout(io.StringIO()):
            stats = backfill.run_backfill("20261113", "20261115", path, now=self.NOW)
        self.assertEqual((stats.ok, stats.skip), (12, 12))
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) FROM races WHERE source='db'").fetchone()[0], 24)


if __name__ == "__main__":
    unittest.main()
