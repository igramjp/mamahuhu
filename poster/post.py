"""
mamahuhu X auto-poster

毎週金・土の20時、翌日のG3以上のメインレースを netkeiba から取得し、
直近開催のバイアスデータを組み合わせて X に投稿する。
バイアスデータは同一リポジトリ内の public/data/site.db (SQLite) から読む
(旧: mamahuhu.app の JSON を HTTP 取得)。

メインツイート: バイアス + 該当馬
リプライ: mamahuhu の URL

cost: メイン($0.015) + リプライ($0.01) = 約$0.025/レース
"""

from __future__ import annotations

import argparse
import logging
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
    """指定日の馬場よみ(bias3)を site.db から読み、コース種別ごとの
    「来てる」グループを返す: {surface: {"frame": 内/中/外|None, "style": ...|None}}。
    データなしはNone。"""
    date_str = date_dt.strftime("%Y%m%d")
    conn = _site_db()

    surfaces: dict = {}
    for surface, kind, grp, deviation, n in conn.execute(
        "SELECT surface, kind, grp, deviation, n FROM bias3_stats"
        " WHERE date = ? AND place = ?", (date_str, course)):
        key = "frame" if kind == "frame3" else "style"
        s = surfaces.setdefault(surface, {"frame": None, "style": None,
                                          "_best": {}})
        if (n or 0) < MIN_N or deviation is None or deviation > -DEV_THRESHOLD:
            continue
        best = s["_best"].get(key)
        if best is None or deviation < best:
            s["_best"][key] = deviation
            s[key] = grp

    if not surfaces:
        log.info("%s: %s の馬場よみデータなし", course, date_str)
        return None
    for s in surfaces.values():
        s.pop("_best", None)
    log.info("%s: 馬場よみ取得 (%s) %s", course, date_str, surfaces)
    return surfaces


def derive_bias(verdicts: dict, surface: str) -> str | None:
    """コース種別の「来てる」グループを投稿文用に整形。どちらも無ければNone。"""
    v = (verdicts or {}).get(surface)
    if not v:
        return None
    parts = []
    if v.get("frame"):
        parts.append(f"{v['frame']}枠")
    if v.get("style"):
        parts.append(v["style"])
    if not parts:
        return None
    return "いつもより" + "・".join(parts) + "が来てる"


def style_from_last_corner(last_corner: int, field_size: int) -> str:
    """前走の4コーナー通過順位を脚質2分類に。頭数の前半なら逃げ先行。"""
    return "逃げ先行" if last_corner <= field_size / 2 else "差し追込"


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


def fetch_last_run_styles(race_id: str) -> dict[str, str]:
    """馬柱ページから各馬の前走脚質(逃げ先行/差し追込)を判定して返す。

    返り値: {馬名: 脚質}。前走情報が取れない馬は含めない。
    """
    url = f"{NETKEIBA_BASE}/race/shutuba_past.html?race_id={race_id}"
    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    # 馬柱本体のみ対象 (shutuba_meta と同様、展開予想テーブル等を除外)
    table = soup.select_one("table.Shutuba_Past5_Table") or soup

    styles: dict[str, str] = {}
    for tr in table.select("tr.HorseList"):
        name = _horse_name(tr)
        if not name:
            continue
        past = tr.select("td[class*='Past']")
        if not past:
            continue
        text = past[0].get_text(" ", strip=True)  # 先頭セル = 前走
        field_m = re.search(r"(\d+)頭", text)
        corner_m = re.search(r"\d+(?:-\d+){1,3}", text)
        if not field_m or not corner_m:
            continue
        field_size = int(field_m.group(1))
        last_corner = int(corner_m.group(0).split("-")[-1])
        styles[name] = style_from_last_corner(last_corner, field_size)
    return styles


def match_bias_horses(
    entries: list[dict],
    last_styles: dict[str, str],
    bias_frame: str | None,
    bias_style: str | None,
) -> list[str]:
    """相対枠位置(内/中/外)と前走脚質が両方「来てる」側に一致する馬名を返す。
    相対枠位置は出馬表の並び(=馬番順)で3等分する(集計側と同じ定義)。"""
    if not bias_frame or not bias_style:
        return []
    n = len(entries)
    if n == 0:
        return []
    matched: list[str] = []
    for i, e in enumerate(entries):
        p = (i + 1) / n
        frame3 = "内" if p <= 1 / 3 + 1e-9 else ("中" if p <= 2 / 3 + 1e-9 else "外")
        if frame3 != bias_frame:
            continue
        if last_styles.get(e["horse"]) != bias_style:
            continue
        matched.append(e["horse"])
    return matched


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


def compose_tweet(
    race_name: str,
    course: str,
    bias: str,
    matched_horses: list[str],
) -> str:
    hashtag = race_name_to_hashtag(race_name)
    head = [f"#{hashtag} のバイアスは...", f"「{bias}」"]
    tail = ["", "詳しい集計データはツリーから"]

    if not matched_horses:
        return "\n".join(head + tail)

    # 原則 全頭表示。280を超える時だけ末尾から削って「、…」を付す。
    shown = list(matched_horses)
    while shown:
        names = "、".join(shown) + ("、…" if shown != matched_horses else "")
        body = "\n".join(head + ["", f"該当馬は{names}。"] + tail)
        if tweet_weight(body) <= TWEET_LIMIT:
            return body
        shown.pop()
    return "\n".join(head + tail)


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
            jockeys = meta["jockeys"]
            surface = meta["surface"]
            entries = meta["entries"]
            # 一覧の名前は短縮されることがあるので出馬表のフルネーム優先
            race_name = meta["race_name"] or race["race_name"]
            log.info(
                "%s: surface=%s, %d 頭 / 騎手 %s",
                race["race_name"],
                surface,
                len(jockeys),
                jockeys[:3],
            )

            data = data_cache.get(race["course"])
            if not data:
                log.info(
                    "%s: %s の過去開催データなし、スキップ",
                    race["race_name"],
                    race["course"],
                )
                outcomes.append(
                    (race["course"], race["grade"], race_name,
                     "スキップ: バイアスデータなし")
                )
                continue
            if not surface:
                log.warning(
                    "%s: 芝/ダート判定不可、スキップ", race["race_name"]
                )
                outcomes.append(
                    (race["course"], race["grade"], race_name,
                     "スキップ: 芝/ダート判定不可")
                )
                continue

            bias = derive_bias(data, surface)
            if not bias:
                log.info(
                    "%s: %s はいつも通りの馬場(ズレなし)、スキップ",
                    race["race_name"],
                    surface,
                )
                outcomes.append(
                    (race["course"], race["grade"], race_name,
                     "スキップ: いつも通りの馬場")
                )
                continue

            time.sleep(SLEEP_BETWEEN_SCRAPE)
            last_styles = fetch_last_run_styles(race["race_id"])
            v = data.get(surface) or {}
            matched_horses = match_bias_horses(
                entries, last_styles, v.get("frame"), v.get("style")
            )
            log.info("該当馬(枠%s/脚質%s): %s",
                     v.get("frame"), v.get("style"), matched_horses)

            text = compose_tweet(
                race_name, race["course"], bias, matched_horses
            )
            log.info("投稿本文:\n%s", text)

            if not dry_run:
                post_thread(text, MAMAHUHU_URL)
                time.sleep(SLEEP_BETWEEN_POST)
            posted += 1
            outcomes.append(
                (race["course"], race["grade"], race_name,
                 "DRY RUN(未投稿)" if dry_run else "✅ 投稿")
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
