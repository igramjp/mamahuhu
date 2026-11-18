"""
過去レースの一括バックフィル。

db.netkeiba.com から日付範囲のレース結果・確定単勝オッズ・通過順・馬場状態を
取得し、data/keiba.db にUPSERTする。長期ベースライン(bias_analysis_spec.md)の
データソース。

- 冪等: 取得済みrace_idはスキップ。何度実行しても安全
- 再開可能: 中断しても再実行すれば続きから
- レースIDは日付ごとの開催一覧ページ(race/list/YYYYMMDD/)から実在するものだけ
  列挙する(回次・日次からの機械生成はしない)。一覧が空の日は「開催なし」
- 開催なし日は checked_dates にキャッシュし、再実行時のリクエストを省く
- 失敗は3種類に区別する:
    不存在   … 一覧にはあるがレースページが無い。スキップして続行(停止しない)
    ブロック … HTTP 403/429 または netkeiba 以外の応答。連続したら停止(exit 2)
    FAIL     … ページはあるがパース/取得できない。failures テーブルに記録して続行
               (--retry-failures で再試行)。連続したら形式変更を疑い停止(exit 3)
- 日付はすべてJST基準(GHAランナーはUTCなので date.today() は使わない)

使い方:
  python scraper/backfill.py --from 20230101 --to 20260630
  python scraper/backfill.py --days 8      # JSTの今日から8日前〜今日(火曜の定期取り込み)
  python scraper/backfill.py --retry-failures
  python scraper/backfill.py --from 20240106 --to 20240108 --limit 5   # お試し
"""

import argparse
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

import db
import payouts as payouts_mod
from scrape import COURSE, HEADERS, _extract_race_table, decode_html

CODE_TO_PLACE = {v: k for k, v in COURSE.items()}

SLEEP_RACE = 2.0   # レースページ間隔(秒)。1.0sで約2時間後にレート制限を
SLEEP_LIST = 1.5   # 受けた実績(2026-07-04)があるため控えめに設定
MAX_CONSECUTIVE_BLOCKS = 5   # 連続ブロックで停止(ハンマリング防止)
# 連続FAIL(ページは返るがパース不能/通信エラー)での停止。ブロックではなく
# 「ページ形式が変わった」ケース用(2026-11 の EUC-JP→UTF-8 切替がこれ)。
# 無いと全レースに無駄打ちし続けるため、ブロックとは別枠で残している。
MAX_CONSECUTIVE_FAILS = 10
REST_EVERY = 800   # このレース数ごとに長休止(約3500req連続でレート制限を受けたため)
REST_SECONDS = 300
BLOCK_STATUSES = {403, 429}
# 開催なし(0件)をキャッシュしない直近日数。db側への反映前に一覧が空だった日を
# 「開催なし」と確定させてしまうと、以後ずっとスキップされるため。
RECENT_DAYS = 7

JST = timezone(timedelta(hours=9))  # 日本は夏時間なし


class BlockedError(Exception):
    """レート制限/ブロック(HTTP 403/429、または netkeiba 以外の応答)。"""


class RaceNotFoundError(Exception):
    """レースページが存在しない(HTTP 404、または中身が空の雛形ページ)。"""


class BackfillAborted(Exception):
    """全体を停止するための例外。exit_code: 2=ブロック, 3=連続FAIL。"""

    def __init__(self, message, exit_code):
        super().__init__(message)
        self.exit_code = exit_code


# ---------- 日付(JST) ----------
def jst_today(now=None):
    """JSTでの今日。now はテスト用(tz付きdatetime)。
    GHAランナーはUTCで、date.today() だと日本時間0〜9時に前日を返す。"""
    now = now or datetime.now(timezone.utc)
    return now.astimezone(JST).date()


def default_to(now=None):
    """--to の既定値 = JSTの今日。
    旧既定の「4日前」は火曜実行だと金曜止まりで、取り込みたい直前の土日が
    範囲から外れていた。未反映の日は一覧が空=開催なし扱い(直近はキャッシュ
    しない)になるだけなので、今日まで含めて問題ない。"""
    return jst_today(now).strftime("%Y%m%d")


def recent_range(days, now=None):
    """JSTの今日から days 日前〜今日の (from, to)。days>=7 なら直近の土日を必ず含む。"""
    today = jst_today(now)
    return (today - timedelta(days=days)).strftime("%Y%m%d"), today.strftime("%Y%m%d")


# ---------- 取得 ----------
def block_reason(html):
    """HTTP 200 でもブロック画面が返ることがあるので応答内容でも判定する。
    正常なページは(存在しないレースの雛形ページも含め)必ず netkeiba の
    ヘッダ/フッタを含む。ブロックなら理由文字列、正常なら None。"""
    if "netkeiba" not in html:
        return "netkeibaのページではない応答"
    return None


def fetch(url, retries=3):
    """GETして文字列を返す。
    - 403/429・ブロック画面 → BlockedError(リトライしない)
    - 404 → RaceNotFoundError(リトライしない)
    - 5xx・通信エラー → 指数バックオフでリトライ、尽きたら raise
    """
    for attempt in range(retries):
        last = attempt == retries - 1
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
        except requests.RequestException:
            if last:
                raise
            time.sleep(5 * 2 ** attempt)
            continue
        if r.status_code in BLOCK_STATUSES:
            raise BlockedError(f"HTTP {r.status_code}")
        if r.status_code == 404:
            raise RaceNotFoundError("HTTP 404")
        if r.status_code >= 500 and not last:
            time.sleep(5 * 2 ** attempt)
            continue
        r.raise_for_status()
        html = decode_html(r.content)
        reason = block_reason(html)
        if reason:
            raise BlockedError(reason)
        return html


def get_all_race_ids(yyyymmdd):
    """その日のJRA全場の実在するrace_idを開催一覧ページから列挙する
    (開催なしなら空)。IDを回次・日次から組み立てることはしない。"""
    html = fetch(f"https://db.netkeiba.com/race/list/{yyyymmdd}/")
    soup = BeautifulSoup(html, "html.parser")
    ids = set()
    for a in soup.select("a[href*='/race/']"):
        m = re.search(r"/race/(\d{12})", a.get("href", ""))
        if m and m.group(1)[4:6] in CODE_TO_PLACE:
            ids.add(m.group(1))
    return sorted(ids)


# ---------- パース ----------
def _parse_info(soup):
    """レース情報欄(例: '芝右 外1600m / 天候 : 晴 / 芝 : 良 / 発走 : 15:35')を
    パースして (surface, distance, turn, course_note, weather, condition) を返す。"""
    info_text = ""
    for sel in ["diary_snap_cut", "p.diary_snap_cut", "div.data_intro"]:
        tag = soup.select_one(sel)
        if tag:
            t = tag.get_text(" ", strip=True)
            if "m" in t:
                info_text = t
                break

    if "障" in info_text:
        surface = "障害"
    elif "芝" in info_text:
        surface = "芝"
    elif "ダ" in info_text:
        surface = "ダート"
    else:
        surface = None

    dist_m = re.search(r"(\d{3,4})m", info_text)
    distance = int(dist_m.group(1)) if dist_m else None

    turn = next((t for t in ["右", "左", "直線"] if t in info_text.split("/")[0]), None)
    # 外回り/内回りの付記(コース部分のみから拾う)
    course_part = info_text.split("/")[0]
    m_note = re.search(r"(外→内|内→外|外|内)\s*\d", course_part)
    course_note = m_note.group(1) if m_note else None

    m_weather = re.search(r"天候\s*[:：]\s*([^\s/]+)", info_text)
    weather = m_weather.group(1) if m_weather else None

    # 馬場状態はトラック種別に対応する欄を優先(障害は芝側を採用)
    condition = None
    label = {"芝": "芝", "障害": "芝", "ダート": "ダート"}.get(surface)
    if label:
        m_cond = re.search(rf"{label}\s*[:：]\s*(不良|稍重|重|良)", info_text)
        condition = m_cond.group(1) if m_cond else None
    if condition is None:
        condition = next((c for c in ["不良", "稍重", "重", "良"] if c in info_text), None)

    return surface, distance, turn, course_note, weather, condition


def is_missing_race_page(soup):
    """存在しないrace_idのページか。db.netkeiba は不存在でも HTTP 200 で
    雛形(結果テーブルなし・レース名空・日付1970年1月01日)を返す。
    結果テーブルが無いだけでは不存在としない(レイアウト変更をFAILで検知するため)。"""
    if soup.select_one("table.race_table_01"):
        return False
    title = soup.title.get_text(strip=True) if soup.title else ""
    return title.startswith("｜") or "1970年" in title


def _to_int(v):
    try:
        return int(str(v).strip())
    except (ValueError, TypeError):
        return None


def _to_float(v):
    try:
        return float(str(v).strip().replace(",", ""))
    except (ValueError, TypeError):
        return None


def parse_race_page(html, race_id, yyyymmdd):
    """db.netkeiba のレースページから (race dict, results list) を作る。
    存在しないレースの雛形ページなら RaceNotFoundError。"""
    soup = BeautifulSoup(html, "html.parser")
    if is_missing_race_page(soup):
        raise RaceNotFoundError("レースページなし(雛形ページ)")
    df = _extract_race_table(html)
    if "馬番" not in df.columns:
        raise ValueError("結果テーブルの列名が想定外(文字コード/レイアウト変更の疑い): "
                         f"{list(df.columns)[:5]}")

    surface, distance, turn, course_note, weather, condition = _parse_info(soup)

    name_tag = (soup.select_one("div.data_intro h1")
                or soup.select_one("dl.racedata h1")
                or soup.select_one("h1.race_name"))
    race_name = name_tag.get_text(strip=True) if name_tag else None

    def cell(row, name):
        v = row.get(name)
        s = str(v).strip() if v is not None else ""
        return s if s and s.lower() != "nan" else None

    results = []
    n_starters = 0
    for _, row in df.iterrows():
        umaban = _to_int(cell(row, "馬番"))
        if umaban is None:
            continue
        finish_raw = cell(row, "着順")
        finish = _to_int(finish_raw)
        # 取消・除外は出走していない。中止・失格は出走扱い。
        excluded = finish_raw is not None and any(x in finish_raw for x in ("取", "除"))
        if not excluded:
            n_starters += 1
        results.append({
            "race_id": race_id,
            "umaban": umaban,
            "wakuban": _to_int(cell(row, "枠番")),
            "finish": finish,
            "finish_raw": finish_raw,
            "horse": cell(row, "馬名"),
            "sex_age": cell(row, "性齢"),
            "weight_carried": _to_float(cell(row, "斤量")),
            "jockey": cell(row, "騎手"),
            "time": cell(row, "タイム"),
            "margin": cell(row, "着差"),
            "passing": cell(row, "通過"),
            "last3f": _to_float(cell(row, "上り")),
            "win_odds": _to_float(cell(row, "単勝")),
            "popularity": _to_int(cell(row, "人気")),
            "horse_weight": cell(row, "馬体重"),
        })

    if not results:
        raise ValueError("結果行が1行もパースできません")
    if surface is None or distance is None:
        raise ValueError(f"レース情報欄がパースできません (surface={surface}, distance={distance})")

    race = {
        "race_id": race_id,
        "date": yyyymmdd,
        "place": CODE_TO_PLACE.get(race_id[4:6]),
        "kai": int(race_id[6:8]),
        "nichi": int(race_id[8:10]),
        "race_no": int(race_id[10:12]),
        "race_name": race_name,
        "surface": surface,
        "distance": distance,
        "turn": turn,
        "course_note": course_note,
        "weather": weather,
        "track_condition": condition,
        "n_starters": n_starters,
        "source": "db",
        "scraped_at": datetime.now().isoformat(timespec="seconds"),
    }
    return race, results


# ---------- 実行 ----------
@dataclass
class Stats:
    """今回実行ぶんのカウンタ。ログ1行につき必ず1つ加算する
    (サマリとログの件数を一致させるため、日単位の途中集計は持たない)。"""
    ok: int = 0
    fail: int = 0      # "FAIL" ログ行と同数
    missing: int = 0   # "不存在" ログ行と同数
    blocked: int = 0   # "BLOCKED" ログ行と同数
    skip: int = 0      # 取得済みでリクエストしなかったレース


def scrape_one(conn, race_id, yyyymmdd):
    """1レース取得して保存。"ok" / "fail" / "missing" を返す。
    ブロックは BlockedError をそのまま上げる(レース固有の失敗ではないので
    failures には記録しない)。"""
    try:
        html = fetch(f"https://db.netkeiba.com/race/{race_id}/")
        race, results = parse_race_page(html, race_id, yyyymmdd)
        db.upsert_race(conn, race, results)
    except BlockedError:
        raise
    except RaceNotFoundError as e:
        print(f"  不存在 {race_id}: {e}(スキップ)", flush=True)
        return "missing"
    except Exception as e:
        db.record_failure(conn, race_id, yyyymmdd, e)
        print(f"  FAIL {race_id}: {str(e)[:120]}", flush=True)
        return "fail"
    # 同じHTMLから払戻も抽出(追加リクエストなし)。失敗しても本体は成功扱い
    try:
        pays = payouts_mod.parse_db_payouts(html)
        if pays:
            db.upsert_payouts(conn, race_id, pays)
    except Exception as e:
        print(f"  払戻パース失敗 {race_id}(継続): {str(e)[:80]}", flush=True)
    return "ok"


def iter_dates(d_from, d_to):
    d = d_from
    while d <= d_to:
        yield d.strftime("%Y%m%d")
        d += timedelta(days=1)


class _Streaks:
    """連続ブロック/連続FAILの監視。不存在はどちらにも数えない。"""

    def __init__(self):
        self.blocks = 0
        self.fails = 0

    def ok(self):
        self.blocks = self.fails = 0

    def blocked(self, context):
        self.blocks += 1
        if self.blocks >= MAX_CONSECUTIVE_BLOCKS:
            raise BackfillAborted(
                f"連続{self.blocks}回ブロック({context})。レート制限のため停止。"
                f"時間をおいて再実行してください(再開は自動)。", 2)

    def failed(self, context):
        self.blocks = 0  # ページ自体は返っている/別種の障害
        self.fails += 1
        if self.fails >= MAX_CONSECUTIVE_FAILS:
            raise BackfillAborted(
                f"連続{self.fails}回FAIL({context})。ブロックではなくページ形式変更"
                f"または通信障害の疑いのため停止。FAILログの内容を確認してください。", 3)


def run_backfill(date_from, date_to, db_path=None, limit=None, now=None):
    conn = db.connect(db_path)
    done_ids = db.existing_race_ids(conn)
    checked = db.checked_dates(conn)
    # db品質で保存済みのレース数。realtime暫定行は「取得済み」に数えない
    # (数えると、暫定行が揃っている日は取り直されずdb品質に上がらない)
    saved_by_date = dict(conn.execute(
        "SELECT date, COUNT(*) FROM races WHERE source = 'db' GROUP BY date"))

    d_from = datetime.strptime(date_from, "%Y%m%d").date()
    d_to = datetime.strptime(date_to, "%Y%m%d").date()
    recent_from = (jst_today(now) - timedelta(days=RECENT_DAYS)).strftime("%Y%m%d")
    stats = Stats()
    streaks = _Streaks()
    fetched = 0

    try:
        for ymd in iter_dates(d_from, d_to):
            # チェック済みかつ全レース保存済みの日付は一覧取得すら省く
            if ymd in checked and saved_by_date.get(ymd, 0) >= checked[ymd]:
                stats.skip += checked[ymd]
                continue

            try:
                ids = get_all_race_ids(ymd)
            except BlockedError as e:
                stats.blocked += 1
                print(f"{ymd}: BLOCKED 一覧ページ: {e}", flush=True)
                streaks.blocked("一覧ページ")
                time.sleep(SLEEP_LIST)
                continue
            except Exception as e:
                stats.fail += 1
                print(f"{ymd}: FAIL 一覧ページ: {str(e)[:120]}", flush=True)
                streaks.failed("一覧ページ")
                time.sleep(SLEEP_LIST)
                continue
            time.sleep(SLEEP_LIST)
            if not ids:
                # 開催なし = 正常スキップ。直近はdb未反映の可能性があるので確定させない
                if ymd < recent_from:
                    db.mark_date_checked(conn, ymd, 0)
                continue
            db.mark_date_checked(conn, ymd, len(ids))

            day = Stats()
            for rid in ids:
                if rid in done_ids:
                    day.skip += 1
                    stats.skip += 1
                    continue
                if limit is not None and fetched >= limit:
                    print(f"--limit {limit} に到達、終了", flush=True)
                    print_summary(conn, stats)
                    return stats
                fetched += 1
                try:
                    status = scrape_one(conn, rid, ymd)
                except BlockedError as e:
                    day.blocked += 1
                    stats.blocked += 1
                    print(f"  BLOCKED {rid}: {e}", flush=True)
                    streaks.blocked("レースページ")
                else:
                    if status == "ok":
                        day.ok += 1
                        stats.ok += 1
                        streaks.ok()
                    elif status == "missing":
                        day.missing += 1
                        stats.missing += 1
                    else:
                        day.fail += 1
                        stats.fail += 1
                        streaks.failed("レースページ")
                time.sleep(SLEEP_RACE)
                if fetched % REST_EVERY == 0:
                    print(f"  ({fetched}レース取得、{REST_SECONDS}秒休止)", flush=True)
                    time.sleep(REST_SECONDS)

            places = sorted({CODE_TO_PLACE.get(r[4:6], "?") for r in ids})
            print(f"{ymd} [{'/'.join(places)}] {len(ids)}R: ok={day.ok} skip={day.skip}"
                  f" fail={day.fail} 不存在={day.missing} blocked={day.blocked}"
                  f" (累計 ok={stats.ok} fail={stats.fail})", flush=True)
    except BackfillAborted as e:
        print(f"\n!! {e}", flush=True)
        print_summary(conn, stats)
        raise SystemExit(e.exit_code)

    print_summary(conn, stats)
    return stats


def backfill_payouts(date_from, date_to, db_path=None, limit=None):
    """取得済みレース(source='db')のうち払戻が未収集のものだけ、
    レースページを再取得して払戻を埋める。通常のバックフィルと同じ
    レート制限対策(SLEEP_RACE・連続エラー停止・定期休止)。"""
    conn = db.connect(db_path)
    rows = conn.execute(
        "SELECT race_id, date FROM races"
        " WHERE source = 'db' AND date BETWEEN ? AND ?"
        "   AND race_id NOT IN (SELECT DISTINCT race_id FROM payouts)"
        " ORDER BY date", (date_from, date_to)).fetchall()
    if not rows:
        print("払戻未収集のレースなし")
        return
    print(f"払戻バックフィル対象: {len(rows)}R", flush=True)

    ok = fail = missing = blocked = 0
    consecutive_blocks = 0
    for i, (rid, ymd) in enumerate(rows):
        if limit is not None and i >= limit:
            print(f"--limit {limit} に到達、終了", flush=True)
            break
        try:
            html = fetch(f"https://db.netkeiba.com/race/{rid}/")
            pays = payouts_mod.parse_db_payouts(html)
            if pays:
                db.upsert_payouts(conn, rid, pays)
                ok += 1
            else:
                fail += 1
                print(f"  FAIL {rid}: 払戻なし ({ymd})", flush=True)
            consecutive_blocks = 0
        except BlockedError as e:
            blocked += 1
            consecutive_blocks += 1
            print(f"  BLOCKED {rid}: {e}", flush=True)
            if consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
                print(f"!! 連続{consecutive_blocks}回ブロック。レート制限のため停止"
                      f"(再実行で続きから)", flush=True)
                break
        except RaceNotFoundError as e:
            missing += 1
            print(f"  不存在 {rid}: {e}(スキップ)", flush=True)
        except Exception as e:
            fail += 1
            print(f"  FAIL {rid}: {str(e)[:120]}", flush=True)
        time.sleep(SLEEP_RACE)
        if (i + 1) % REST_EVERY == 0:
            print(f"  ({i + 1}レース処理、{REST_SECONDS}秒休止)", flush=True)
            time.sleep(REST_SECONDS)

    n_pay = conn.execute(
        "SELECT COUNT(DISTINCT race_id) FROM payouts").fetchone()[0]
    print(f"\n払戻バックフィル完了: ok={ok} fail={fail} 不存在={missing} blocked={blocked}"
          f" / DB累計 {n_pay}Rぶんの払戻", flush=True)


def retry_failures(db_path=None):
    conn = db.connect(db_path)
    rows = list(conn.execute("SELECT race_id, date FROM failures ORDER BY date"))
    if not rows:
        print("failures なし")
        return
    print(f"failures {len(rows)}件を再試行", flush=True)
    stats = Stats()
    consecutive_blocks = 0
    for rid, ymd in rows:
        try:
            status = scrape_one(conn, rid, ymd)
        except BlockedError as e:
            stats.blocked += 1
            consecutive_blocks += 1
            print(f"  BLOCKED {rid}: {e}", flush=True)
            if consecutive_blocks >= MAX_CONSECUTIVE_BLOCKS:
                print(f"!! 連続{consecutive_blocks}回ブロック。レート制限のため停止",
                      flush=True)
                print_summary(conn, stats)
                raise SystemExit(2)
        else:
            consecutive_blocks = 0
            if status == "ok":
                stats.ok += 1
            elif status == "missing":
                stats.missing += 1
                # 実在しないレースは再試行しても無駄なので failures から外す
                with conn:
                    conn.execute("DELETE FROM failures WHERE race_id = ?", (rid,))
            else:
                stats.fail += 1
        time.sleep(SLEEP_RACE)
    print(f"再試行完了: {stats.ok}/{len(rows)} 成功", flush=True)
    print_summary(conn, stats)


def print_summary(conn, stats):
    n_races = conn.execute("SELECT COUNT(*) FROM races").fetchone()[0]
    n_results = conn.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    n_fail = conn.execute("SELECT COUNT(*) FROM failures").fetchone()[0]
    span = conn.execute("SELECT MIN(date), MAX(date) FROM races").fetchone()
    print(f"\n=== サマリ ===\n"
          f"今回: ok={stats.ok} fail={stats.fail} 不存在={stats.missing}"
          f" blocked={stats.blocked} skip={stats.skip}\n"
          f"DB累計: races={n_races} results={n_results} failures={n_fail} "
          f"期間={span[0]}〜{span[1]}", flush=True)


def main():
    ap = argparse.ArgumentParser(description="netkeiba過去レースのバックフィル")
    ap.add_argument("--from", dest="date_from", default=None,
                    help="開始日 YYYYMMDD(既定: 20230101)")
    ap.add_argument("--to", dest="date_to", default=None,
                    help="終了日 YYYYMMDD(既定: JSTの今日)")
    ap.add_argument("--days", type=int, default=None,
                    help="JSTの今日からN日前を開始日にする(--from の代わり)")
    ap.add_argument("--db", dest="db_path", default=None)
    ap.add_argument("--limit", type=int, default=None, help="今回取得する最大レース数(テスト用)")
    ap.add_argument("--retry-failures", action="store_true")
    ap.add_argument("--payouts-only", action="store_true",
                    help="取得済みレースの払戻だけを埋める(過去分の払戻収集用)")
    args = ap.parse_args()
    if args.days is not None and args.date_from:
        ap.error("--days と --from は同時に指定できません")
    if args.days is not None:
        args.date_from = recent_range(args.days)[0]
    args.date_from = args.date_from or "20230101"
    args.date_to = args.date_to or default_to()

    if args.retry_failures:
        retry_failures(args.db_path)
    elif args.payouts_only:
        print(f"払戻バックフィル: {args.date_from} 〜 {args.date_to}", flush=True)
        backfill_payouts(args.date_from, args.date_to, args.db_path, args.limit)
    else:
        print(f"バックフィル: {args.date_from} 〜 {args.date_to}", flush=True)
        run_backfill(args.date_from, args.date_to, args.db_path, args.limit)


if __name__ == "__main__":
    main()
