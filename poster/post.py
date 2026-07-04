"""
mamahuhu X auto-poster

■ 前日ポスト(金・土 20:00 JST)
  翌日のG3以上レースについて期待値分析の結論を投稿する。
  v1モデルは β が小さく、オッズに依らず期待値の上界が閾値未満のため
  「見送り」を前日にリークなしで宣言できる(run_preview 内の上界判定)。
  上界が閾値を超えるモデルに更新された場合は投稿をスキップして警告する
  (前日オッズ取得パイプラインの実装が必要になったシグナル)。

■ 結果検証ポスト(火 20:00 JST, --verify)
  直近週末の判断(見送り/推奨)と検証(的中・回収率・全馬ベット基準)を投稿。

データは同一リポジトリ内の public/data/site.db / data/keiba.db から読む。
モデルパラメータ(β・EV閾値)は scraper/predict.py を単一の真実として参照。

cost: メイン($0.015) + リプライ($0.01) = 約$0.025/投稿
"""

from __future__ import annotations

import argparse
import logging
import math
import os
import re
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
import tweepy
from bs4 import BeautifulSoup

# モデルパラメータは scraper/predict.py を単一の真実として参照する
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scraper"))
from predict import BETA, EV_THRESHOLD, MODEL_VERSION  # noqa: E402

# ---------- 定数 ----------
JST = timezone(timedelta(hours=9))
NETKEIBA_BASE = "https://race.netkeiba.com"
SITE_DB_PATH = Path(__file__).resolve().parent.parent / "public" / "data" / "site.db"
USER_AGENT = "mamahuhu-bot/1.0 (+https://mamahuhu.app/)"
MAMAHUHU_URL = "https://mamahuhu.app/"
REQUEST_TIMEOUT = 30
SLEEP_BETWEEN_SCRAPE = 2   # netkeibaへの負荷軽減
SLEEP_BETWEEN_POST = 5     # X側のレート制限対策
TWEET_LIMIT = 280          # X weighted length 上限
KEIBA_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "keiba.db"
RESULT_URL = "https://mamahuhu.app/result"
# JRA単勝のオーバーラウンド(Σ1/オッズ)の下限目安。期待値上界の計算に使う
OVERROUND_FLOOR = 1.15

# race_id の 5-6 桁目 = JRA 競馬場コード
TRACK_CODES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟", "05": "東京",
    "06": "中山", "07": "中京", "08": "京都", "09": "阪神", "10": "小倉",
}

# ---------- ログ ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("mamahuhu-bot")


# ---------- HTTP ----------
def fetch_html(url: str) -> str:
    """netkeibaからGET。文字コードはshift_jis or utf-8を自動判定。"""
    r = requests.get(
        url, headers={"User-Agent": USER_AGENT}, timeout=REQUEST_TIMEOUT
    )
    r.raise_for_status()
    r.encoding = r.apparent_encoding
    return r.text


# ---------- mamahuhu data (site.db) ----------
_site_db_conn: sqlite3.Connection | None = None


def _site_db() -> sqlite3.Connection:
    global _site_db_conn
    if _site_db_conn is None:
        if not SITE_DB_PATH.exists():
            raise FileNotFoundError(f"site.db がありません: {SITE_DB_PATH}")
        _site_db_conn = sqlite3.connect(f"file:{SITE_DB_PATH}?mode=ro", uri=True)
    return _site_db_conn


# 「来てる」判定(サイト側 db.js の favoredGroup と同じ定義)
DEV_THRESHOLD = 0.01
MIN_N = 8


def fetch_race_data_on(course: str, date_dt: datetime) -> dict | None:
    """指定日のバイアス分析(bias3)を site.db から読む。
    戻り値: {surface: {"frame": grp|None, "frame_dev": float|None,
                       "style": grp|None, "max_x": float}}
    frame/style は有意な有利化グループ(n>=8, Δ<=-0.01, 最小Δ)。
    max_x は全グループの正の補正項(-Δ)の最大値で、期待値上界の計算に使う。
    データなしはNone。"""
    date_str = date_dt.strftime("%Y%m%d")
    conn = _site_db()

    surfaces: dict = {}
    for surface, kind, grp, deviation, n in conn.execute(
        "SELECT surface, kind, grp, deviation, n FROM bias3_stats"
        " WHERE date = ? AND place = ? AND dist_cat = 'ALL'",
            (date_str, course)):
        key = "frame" if kind == "frame3" else "style"
        s = surfaces.setdefault(
            surface, {"frame": None, "frame_dev": None, "style": None,
                      "max_x": 0.0, "_best": {}})
        dev = deviation if deviation is not None else 0.0
        s["max_x"] = max(s["max_x"], -dev)
        if (n or 0) < MIN_N or dev > -DEV_THRESHOLD:
            continue
        best = s["_best"].get(key)
        if best is None or dev < best:
            s["_best"][key] = dev
            s[key] = grp
            if key == "frame":
                s["frame_dev"] = dev

    if not surfaces:
        log.info("%s: %s のバイアス分析データなし", course, date_str)
        return None
    for s in surfaces.values():
        s.pop("_best", None)
    log.info("%s: バイアス分析取得 (%s) %s", course, date_str, surfaces)
    return surfaces


def ev_upper_bound(max_x: float) -> float:
    """オッズに依らない期待値の上界。
    市場確率×オッズ ≦ 1/オーバーラウンド下限、モデル補正は e^(β·x) 倍まで。
    この値が閾値未満なら、どんなオッズでも推奨は発生しない=見送り確定。"""
    return (1.0 / OVERROUND_FLOOR) * math.exp(BETA * max(0.0, max_x))


def baba_memo(verdicts: dict, course: str, surface: str) -> str:
    """馬場メモの一行を生成(サイトの用語と一致させる)。"""
    v = (verdicts or {}).get(surface)
    label = f"{course}・{surface}"
    if not v:
        return f"馬場メモ: {label}は分析データなし"
    if v.get("frame") and v.get("frame_dev") is not None:
        memo = (f"馬場メモ: {label}は{v['frame']}枠グループが有利化"
                f"(Δ{v['frame_dev']:+.2f})".replace("+", "+").replace("-", "−"))
        if v.get("style"):
            memo += f"、脚質は{v['style']}優位"
        return memo
    return f"馬場メモ: {label}はベースラインからの乖離なし"


# ---------- netkeiba スクレイピング ----------
def course_from_race_id(race_id: str) -> str | None:
    """race_id の競馬場コード(5-6桁目)から競馬場名を返す。"""
    return TRACK_CODES.get(race_id[4:6]) if len(race_id) >= 6 else None


def parse_graded_races(html: str) -> list[dict]:
    """
    レース一覧ページのHTMLから、G1/G2/G3のレース情報を抽出する。
    競馬場は race_id のコードから判定する(一覧の見出しは当てにしない)。

    返り値: [{race_id, race_name, course, grade}, ...]
    """
    soup = BeautifulSoup(html, "html.parser")
    races: list[dict] = []

    for item in soup.select(".RaceList_DataItem"):
        link = item.find("a", href=True)
        if not link:
            continue
        m = re.search(r"race_id=(\d+)", link["href"])
        if not m:
            continue
        race_id = m.group(1)

        grade = None
        icon = item.select_one("[class*='Icon_GradeType']")
        if icon:
            for cls in icon.get("class", []):
                if cls.endswith("Type1"):
                    grade = "G1"
                elif cls.endswith("Type2"):
                    grade = "G2"
                elif cls.endswith("Type3"):
                    grade = "G3"
        if not grade:
            continue

        course = course_from_race_id(race_id)
        if not course:
            continue

        name_elem = item.select_one(".ItemTitle") or item.select_one(
            ".RaceList_ItemTitle"
        )
        race_name = (
            name_elem.get_text(strip=True) if name_elem else "Unknown"
        )
        race_name = re.sub(r"\s+", "", race_name)

        races.append(
            {
                "race_id": race_id,
                "race_name": race_name,
                "course": course,
                "grade": grade,
            }
        )
    return races


def _horse_name(tr) -> str | None:
    a = (
        tr.select_one(".HorseName a")
        or tr.select_one(".Horse02 a")
        or tr.select_one("a[href*='/horse/']")
    )
    return a.get_text(strip=True) if a else None


def fetch_race_meta(race_id: str) -> dict:
    """出馬表ページからレース名・騎手名・芝/ダート種別・各馬の枠番を取得。"""
    url = f"{NETKEIBA_BASE}/race/shutuba.html?race_id={race_id}"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    name_el = soup.select_one(".RaceName")
    race_name = name_el.get_text(strip=True) if name_el else None

    # 出馬表本体のみを対象にする。同じページの展開予想テーブル
    # (PredictRap_Table) も tr.HorseList を持つため、スコープしないと
    # 逃げ想定馬が二重に拾われる。
    table = soup.select_one("table.ShutubaTable") or soup

    jockeys = [
        c.get_text(strip=True)
        for c in table.select("td.Jockey a")
        if c.get_text(strip=True)
    ]

    surface = None
    data_elem = soup.select_one(".RaceData01") or soup.select_one(".RaceData")
    if data_elem:
        text = data_elem.get_text()
        if "ダート" in text or re.search(r"ダ\s*\d", text):
            surface = "ダート"
        elif re.search(r"芝\s*\d", text):
            surface = "芝"

    # 各馬の枠番 (枠順確定前は空 → None)
    entries: list[dict] = []
    for tr in table.select("tr.HorseList"):
        name = _horse_name(tr)
        if not name:
            continue
        waku = None
        waku_el = tr.select_one("td[class*='Waku']")
        if waku_el:
            m = re.search(r"\d+", waku_el.get_text())
            if m:
                waku = int(m.group())
        entries.append({"horse": name, "waku": waku})

    return {
        "race_name": race_name,
        "jockeys": jockeys,
        "surface": surface,
        "entries": entries,
    }


# ---------- 投稿 ----------
def race_name_to_hashtag(race_name: str) -> str:
    cleaned = re.sub(r"[((].*?[))]", "", race_name)
    # 長音記号 ー は #オークス 等で有効な文字なので残す。
    # 区切りになる空白・中点・各種ハイフンのみ除去。
    cleaned = re.sub(r"[ \s・\-‐]", "", cleaned)
    return cleaned


def tweet_weight(text: str) -> int:
    """Xの重み付き文字数。CJK等(>=U+1100)は2、ASCII等は1。"""
    return sum(1 if ord(c) < 0x1100 else 2 for c in text)


def compose_pass_tweet(race_name: str, memo: str) -> str:
    """前日ポスト(見送り)。分析レポート調。"""
    hashtag = race_name_to_hashtag(race_name)
    lines = [
        f"#{hashtag} の期待値分析",
        "",
        "結論: 見送り",
        f"オッズとモデルの見解差が基準(期待値{EV_THRESHOLD})に届く馬はいません。",
        "",
        memo,
        "",
        "全レースの分析はリプライから",
    ]
    body = "\n".join(lines)
    if tweet_weight(body) > TWEET_LIMIT:
        # 長すぎる場合は馬場メモを落とす
        body = "\n".join(lines[:5] + lines[6:])
    return body


def compose_verify_tweet(dates: list[str], n_races: int, n_reco: int,
                         n_hit: int, roi: float | None,
                         bench_roi: float) -> str:
    """結果検証ポスト(火曜)。"""
    labels = "・".join(f"{int(d[4:6])}/{int(d[6:8])}" for d in dates)
    lines = [f"週末の結果検証({labels})", ""]
    if n_reco == 0:
        lines.append(f"判断: 全{n_races}レースを見送り")
    else:
        lines.append(f"判断: 推奨{n_reco}頭(他は見送り)")
        lines.append(
            f"結果: 的中{n_hit}頭、単勝回収率{0 if roi is None else round(roi)}%")
    lines += [
        f"参考: 全馬に単勝100円を投じた場合の回収率は{round(bench_roi)}%",
        "",
        "「買わない」も検証可能な判断として記録しています。",
        "詳細はリプライから",
    ]
    return "\n".join(lines)


def write_step_summary(lines: list[str]) -> None:
    """GitHub Actions のジョブサマリーへ Markdown を出力。

    ローカル実行(GITHUB_STEP_SUMMARY 未設定)では何もしない。
    """
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not path:
        return
    try:
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
    except OSError as e:
        log.warning("step summary 書き込み失敗: %s", e)


def post_thread(text: str, reply_url: str) -> str:
    client = tweepy.Client(
        consumer_key=os.environ["X_API_KEY"],
        consumer_secret=os.environ["X_API_SECRET"],
        access_token=os.environ["X_ACCESS_TOKEN"],
        access_token_secret=os.environ["X_ACCESS_SECRET"],
    )
    main = client.create_tweet(text=text)
    main_id = main.data["id"]
    log.info("posted main tweet id=%s", main_id)

    time.sleep(2)
    reply = client.create_tweet(text=reply_url, in_reply_to_tweet_id=main_id)
    log.info("posted reply id=%s", reply.data["id"])
    return main_id


# ---------- 結果検証ポスト(火曜) ----------
def run_verify(now_dt: datetime, dry_run: bool) -> int:
    """直近7日の判断と結果を site.db / keiba.db から集計して投稿する。"""
    since = (now_dt - timedelta(days=7)).strftime("%Y%m%d")
    site = _site_db()

    dates = [d for (d,) in site.execute(
        "SELECT DISTINCT date FROM pred_horses"
        " WHERE rank IS NOT NULL AND date >= ? ORDER BY date", (since,))]
    if not dates:
        log.info("直近7日に検証可能なデータなし。終了。")
        write_step_summary(["### 検証投稿 0 件 — 直近7日に開催データなし(正常)"])
        return 0

    ph = ",".join("?" for _ in dates)
    n_races = site.execute(
        f"SELECT COUNT(*) FROM pred_races WHERE date IN ({ph})", dates,
    ).fetchone()[0]
    n_reco, n_hit, payout = site.execute(
        f"SELECT COUNT(*), SUM(CASE WHEN rank = 1 THEN 1 ELSE 0 END),"
        f" SUM(CASE WHEN rank = 1 THEN odds ELSE 0 END)"
        f" FROM pred_horses WHERE recommended = 1 AND date IN ({ph})", dates,
    ).fetchone()
    n_reco, n_hit = n_reco or 0, n_hit or 0
    roi = (payout or 0) / n_reco * 100 if n_reco else None

    # 参考基準: 全出走馬に単勝100円を投じた場合の回収率(keiba.dbから)
    keiba = sqlite3.connect(f"file:{KEIBA_DB_PATH}?mode=ro", uri=True)
    n_all, ret_all = keiba.execute(
        f"SELECT COUNT(*), SUM(CASE WHEN h.finish = 1 THEN h.win_odds ELSE 0 END)"
        f" FROM results h JOIN races r ON r.race_id = h.race_id"
        f" WHERE r.date IN ({ph}) AND r.surface IN ('芝','ダート')"
        f" AND h.win_odds IS NOT NULL", dates,
    ).fetchone()
    bench_roi = (ret_all or 0) / n_all * 100 if n_all else 0.0

    text = compose_verify_tweet(dates, n_races, n_reco, n_hit, roi, bench_roi)
    log.info("検証投稿本文(重み%d):\n%s", tweet_weight(text), text)

    if not dry_run:
        post_thread(text, RESULT_URL)

    write_step_summary([
        "### 結果検証ポスト",
        "",
        "```",
        text,
        "```",
        "DRY RUN(未投稿)" if dry_run else "✅ 投稿済み",
    ])
    return 0


# ---------- メイン ----------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="mamahuhu X auto-poster")
    p.add_argument(
        "--date",
        help=(
            "テスト用: 実行日を YYYYMMDD で上書き(JST 20:00 として扱う)。"
            "曜日判定とJSON取得日に反映される。"
        ),
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="実投稿せずログだけ出す (DRY_RUN=1 env と同等)",
    )
    p.add_argument(
        "--verify",
        action="store_true",
        help="結果検証ポスト(火曜用): 直近週末の判断と回収率を投稿",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    dry_run = args.dry_run or os.environ.get("DRY_RUN") == "1"
    if dry_run:
        log.info("=== DRY RUN MODE (実投稿しない) ===")

    if args.date:
        d = datetime.strptime(args.date, "%Y%m%d")
        now_dt = d.replace(hour=20, tzinfo=JST)
        log.info("=== TEST: 実行日付を %s に上書き ===", args.date)
    else:
        now_dt = datetime.now(JST)

    if args.verify:
        return run_verify(now_dt, dry_run)

    target_dt = now_dt + timedelta(days=1)  # 翌日(=投稿対象)のレース
    log.info(
        "now=%s (%s) target=%s",
        now_dt.strftime("%Y-%m-%d %H:%M"),
        ["月", "火", "水", "木", "金", "土", "日"][now_dt.weekday()],
        target_dt.strftime("%Y-%m-%d"),
    )

    # 1. 翌日のG3以上レース一覧
    list_url = (
        f"{NETKEIBA_BASE}/top/race_list_sub.html"
        f"?kaisai_date={target_dt.strftime('%Y%m%d')}"
    )
    log.info("fetching race list: %s", list_url)
    html = fetch_html(list_url)
    races = parse_graded_races(html)

    target_label = (
        f"{target_dt.strftime('%-m/%-d')}"
        f"({['月','火','水','木','金','土','日'][target_dt.weekday()]})"
    )

    if not races:
        log.info("翌日のG3以上レースが見つからず。終了。")
        write_step_summary(
            [
                f"### 投稿 0 件 — {target_label} はG3以上の重賞なし",
                "",
                "対象レースがないため投稿していません(正常)。",
            ]
        )
        return 0

    # 該当レースの競馬場(重複除去、順序保持)
    target_courses: list[str] = []
    for r in races:
        if r["course"] not in target_courses:
            target_courses.append(r["course"])

    log.info("対象競馬場: %s", target_courses)
    log.info(
        "対象レース: %s",
        [(r["course"], r["grade"], r["race_name"]) for r in races],
    )

    # 2. 開催中の全競馬場のバイアスデータを取得 (キャッシュ)
    #    - 金曜投稿(=土曜レース): 先週日曜のJSON (今日から5日前)
    #    - それ以外(土曜投稿=日曜レース): 当日(土曜)のJSON
    #    404 ならスキップ。フォールバックなし。
    is_friday = now_dt.weekday() == 4
    src_dt = now_dt - timedelta(days=5) if is_friday else now_dt
    log.info(
        "バイアスデータソース: %s (%s)",
        "先週日曜" if is_friday else "当日",
        src_dt.strftime("%Y-%m-%d"),
    )
    data_cache: dict[str, dict] = {}
    for course in target_courses:
        d = fetch_race_data_on(course, src_dt)
        if d is not None:
            data_cache[course] = d

    # 3. 各レース処理
    posted = 0
    outcomes: list[tuple[str, str, str, str]] = []  # (場, grade, レース名, 結果)
    for race in races:
        try:
            time.sleep(SLEEP_BETWEEN_SCRAPE)
            meta = fetch_race_meta(race["race_id"])
            surface = meta["surface"]
            # 一覧の名前は短縮されることがあるので出馬表のフルネーム優先
            race_name = meta["race_name"] or race["race_name"]
            log.info("%s: surface=%s", race_name, surface)

            data = data_cache.get(race["course"])
            if not data:
                log.info("%s: %s のバイアス分析なし、スキップ",
                         race_name, race["course"])
                outcomes.append((race["course"], race["grade"], race_name,
                                 "スキップ: バイアス分析なし"))
                continue
            if not surface:
                log.warning("%s: 芝/ダート判定不可、スキップ", race_name)
                outcomes.append((race["course"], race["grade"], race_name,
                                 "スキップ: 芝/ダート判定不可"))
                continue

            # 見送り判定: 期待値の上界がオッズに依らず閾値未満であることを確認。
            # 上界が閾値以上になった場合(将来の強いモデル)は、前日オッズを
            # 取得しないと結論が出せないため投稿をスキップして警告する。
            v = data.get(surface) or {}
            bound = ev_upper_bound(v.get("max_x", 0.0))
            if bound >= EV_THRESHOLD:
                log.warning(
                    "%s: 期待値上界%.3f≥閾値%.2f — 前日オッズが必要。投稿スキップ",
                    race_name, bound, EV_THRESHOLD)
                outcomes.append((race["course"], race["grade"], race_name,
                                 f"⚠️ 要オッズ確認(上界{bound:.2f})"))
                continue

            memo = baba_memo(data, race["course"], surface)
            text = compose_pass_tweet(race_name, memo)
            log.info("投稿本文(重み%d):\n%s", tweet_weight(text), text)

            if not dry_run:
                post_thread(text, MAMAHUHU_URL)
                time.sleep(SLEEP_BETWEEN_POST)
            posted += 1
            outcomes.append(
                (race["course"], race["grade"], race_name,
                 "DRY RUN(未投稿)" if dry_run else "✅ 投稿(見送り宣言)")
            )
        except Exception as e:  # 1レース失敗しても他は続行
            log.exception("error on race %s: %s", race["race_name"], e)
            outcomes.append(
                (race["course"], race["grade"], race["race_name"],
                 f"⚠️ エラー: {e}")
            )
            continue

    log.info("done. posted=%d", posted)

    summary = [
        f"### 投稿 {posted} 件 — {target_label} の重賞 {len(races)} 件",
        "",
        "| 場 | grade | レース | 結果 |",
        "| --- | --- | --- | --- |",
    ]
    for course, grade, name, result in outcomes:
        summary.append(f"| {course} | {grade} | {name} | {result} |")
    write_step_summary(summary)
    return 0


if __name__ == "__main__":
    sys.exit(main())
