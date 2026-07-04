"""
過去レースの一括バックフィル。

db.netkeiba.com から日付範囲のレース結果・確定単勝オッズ・通過順・馬場状態を
取得し、data/keiba.db にUPSERTする。長期ベースライン(bias_analysis_spec.md)の
データソース。

- 冪等: 取得済みrace_idはスキップ。何度実行しても安全
- 再開可能: 中断しても再実行すれば続きから
- 開催なし日は checked_dates にキャッシュし、再実行時のリクエストを省く
- パース失敗は failures テーブルに記録して続行(--retry-failures で再試行)

使い方:
  python scraper/backfill.py --from 20230101 --to 20260630
  python scraper/backfill.py --retry-failures
  python scraper/backfill.py --from 20240106 --to 20240108 --limit 5   # お試し
"""

import argparse
import re
import time
from datetime import date, datetime, timedelta

import requests
from bs4 import BeautifulSoup

import db
import payouts as payouts_mod
from scrape import COURSE, HEADERS, _extract_race_table

CODE_TO_PLACE = {v: k for k, v in COURSE.items()}

SLEEP_RACE = 2.0   # レースページ間隔(秒)。1.0sで約2時間後にレート制限を
SLEEP_LIST = 1.5   # 受けた実績(2026-07-04)があるため控えめに設定
MAX_CONSECUTIVE_ERRORS = 5  # 連続エラーでレート制限とみなし停止(ハンマリング防止)
REST_EVERY = 800   # このレース数ごとに長休止(約3500req連続でレート制限を受けたため)
REST_SECONDS = 300


class RateLimitedError(Exception):
    """連続エラー = レート制限の疑い。即座に全体を停止するための例外。"""


# ---------- 取得 ----------
def fetch(url, retries=3):
    """指数バックオフ付きGET。4xx(レート制限含む)はリトライせず即raise。"""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            r.raise_for_status()
            r.encoding = "EUC-JP"
            return r.text
        except requests.HTTPError as e:
            if e.response is not None and 400 <= e.response.status_code < 500:
                raise  # レート制限/ブロックはリトライしても無駄
            if attempt == retries - 1:
                raise
            time.sleep(5 * 2 ** attempt)
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(5 * 2 ** attempt)


def get_all_race_ids(yyyymmdd):
    """その日のJRA全場のrace_idを返す(開催なしなら空)。"""
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
    """db.netkeiba のレースページから (race dict, results list) を作る。"""
    df = _extract_race_table(html)
    soup = BeautifulSoup(html, "html.parser")

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
def scrape_one(conn, race_id, yyyymmdd):
    try:
        html = fetch(f"https://db.netkeiba.com/race/{race_id}/")
        race, results = parse_race_page(html, race_id, yyyymmdd)
        db.upsert_race(conn, race, results)
        # 同じHTMLから払戻も抽出(追加リクエストなし)。失敗しても本体は成功扱い
        try:
            pays = payouts_mod.parse_db_payouts(html)
            if pays:
                db.upsert_payouts(conn, race_id, pays)
        except Exception as e:
            print(f"  払戻パース失敗 {race_id}(継続): {str(e)[:80]}", flush=True)
        return True
    except Exception as e:
        db.record_failure(conn, race_id, yyyymmdd, e)
        print(f"  FAIL {race_id}: {str(e)[:120]}", flush=True)
        return False


def iter_dates(d_from, d_to):
    d = d_from
    while d <= d_to:
        yield d.strftime("%Y%m%d")
        d += timedelta(days=1)


def run_backfill(date_from, date_to, db_path=None, limit=None):
    conn = db.connect(db_path)
    done_ids = db.existing_race_ids(conn)
    checked = db.checked_dates(conn)
    saved_by_date = db.races_count_by_date(conn)

    d_from = datetime.strptime(date_from, "%Y%m%d").date()
    d_to = datetime.strptime(date_to, "%Y%m%d").date()
    total_ok = total_fail = total_skip = 0
    fetched = 0
    consecutive_errors = 0

    def bump_errors(context):
        nonlocal consecutive_errors
        consecutive_errors += 1
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            raise RateLimitedError(
                f"連続{consecutive_errors}回エラー({context})。レート制限の疑いのため停止。"
                f"時間をおいて再実行してください(再開は自動)。")

    try:
        for ymd in iter_dates(d_from, d_to):
            # チェック済みかつ全レース保存済みの日付は一覧取得すら省く
            if ymd in checked and saved_by_date.get(ymd, 0) >= checked[ymd]:
                total_skip += checked[ymd]
                continue

            try:
                ids = get_all_race_ids(ymd)
                consecutive_errors = 0
            except Exception as e:
                print(f"{ymd}: 一覧取得エラー: {str(e)[:120]}", flush=True)
                bump_errors("一覧ページ")
                time.sleep(SLEEP_LIST)
                continue
            time.sleep(SLEEP_LIST)
            db.mark_date_checked(conn, ymd, len(ids))
            if not ids:
                continue

            ok = fail = skip = 0
            for rid in ids:
                if rid in done_ids:
                    skip += 1
                    continue
                if limit is not None and fetched >= limit:
                    print(f"--limit {limit} に到達、終了", flush=True)
                    print_summary(conn, total_ok + ok, total_fail + fail, total_skip + skip)
                    return
                if scrape_one(conn, rid, ymd):
                    ok += 1
                    consecutive_errors = 0
                else:
                    fail += 1
                    bump_errors("レースページ")
                fetched += 1
                time.sleep(SLEEP_RACE)
                if fetched % REST_EVERY == 0:
                    print(f"  ({fetched}レース取得、{REST_SECONDS}秒休止)", flush=True)
                    time.sleep(REST_SECONDS)

            total_ok += ok
            total_fail += fail
            total_skip += skip
            places = sorted({CODE_TO_PLACE.get(r[4:6], "?") for r in ids})
            print(f"{ymd} [{'/'.join(places)}] {len(ids)}R: ok={ok} skip={skip} fail={fail}"
                  f" (累計 ok={total_ok} fail={total_fail})", flush=True)
    except RateLimitedError as e:
        print(f"\n!! {e}", flush=True)
        print_summary(conn, total_ok, total_fail, total_skip)
        raise SystemExit(2)

    print_summary(conn, total_ok, total_fail, total_skip)


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

    ok = fail = 0
    consecutive_errors = 0
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
                print(f"  払戻なし {rid} ({ymd})", flush=True)
            consecutive_errors = 0
        except Exception as e:
            fail += 1
            consecutive_errors += 1
            print(f"  FAIL {rid}: {str(e)[:120]}", flush=True)
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                print(f"!! 連続{consecutive_errors}回エラー。レート制限の疑いのため停止"
                      f"(再実行で続きから)", flush=True)
                break
        time.sleep(SLEEP_RACE)
        if (i + 1) % REST_EVERY == 0:
            print(f"  ({i + 1}レース処理、{REST_SECONDS}秒休止)", flush=True)
            time.sleep(REST_SECONDS)

    n_pay = conn.execute(
        "SELECT COUNT(DISTINCT race_id) FROM payouts").fetchone()[0]
    print(f"\n払戻バックフィル完了: ok={ok} fail={fail}"
          f" / DB累計 {n_pay}Rぶんの払戻", flush=True)


def retry_failures(db_path=None):
    conn = db.connect(db_path)
    rows = list(conn.execute("SELECT race_id, date FROM failures ORDER BY date"))
    if not rows:
        print("failures なし")
        return
    print(f"failures {len(rows)}件を再試行", flush=True)
    ok = 0
    for rid, ymd in rows:
        if scrape_one(conn, rid, ymd):
            ok += 1
        time.sleep(SLEEP_RACE)
    print(f"再試行完了: {ok}/{len(rows)} 成功", flush=True)
    print_summary(conn, ok, len(rows) - ok, 0)


def print_summary(conn, ok, fail, skip):
    n_races = conn.execute("SELECT COUNT(*) FROM races").fetchone()[0]
    n_results = conn.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    n_fail = conn.execute("SELECT COUNT(*) FROM failures").fetchone()[0]
    span = conn.execute("SELECT MIN(date), MAX(date) FROM races").fetchone()
    print(f"\n=== サマリ ===\n"
          f"今回: ok={ok} fail={fail} skip={skip}\n"
          f"DB累計: races={n_races} results={n_results} failures={n_fail} "
          f"期間={span[0]}〜{span[1]}", flush=True)


def main():
    ap = argparse.ArgumentParser(description="netkeiba過去レースのバックフィル")
    ap.add_argument("--from", dest="date_from", default="20230101")
    ap.add_argument("--to", dest="date_to",
                    default=(date.today() - timedelta(days=4)).strftime("%Y%m%d"),
                    help="デフォルトは4日前(db.netkeibaへの反映待ちを避ける)")
    ap.add_argument("--db", dest="db_path", default=None)
    ap.add_argument("--limit", type=int, default=None, help="今回取得する最大レース数(テスト用)")
    ap.add_argument("--retry-failures", action="store_true")
    ap.add_argument("--payouts-only", action="store_true",
                    help="取得済みレースの払戻だけを埋める(過去分の払戻収集用)")
    args = ap.parse_args()

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
