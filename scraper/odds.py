"""
発走前オッズパイプライン。

翌日(または指定日)の全JRAレースについて、出馬表と発売中の単勝オッズを
取得し、data/keiba.db の forward_races / forward_entries に保存する。
再実行すると forward_entries は最新値に置き換わり、置き換え前の各時点は
forward_odds_history に残る(前日夜→当日朝の変動が追える)。

用途:
  1. 順方向の期待値計算 (predict.py --forward): 発走前にEV・注目馬を出す。
     rebuild_site.py --forward がサイト(pred_* forward=1)へ公開する
  2. X前日ポストに注目馬の実名を載せる (poster/post.py)
  3. 前日vs当日朝vs確定オッズの研究データ蓄積(スマートマネー検出の材料)

GitHub Actions想定: 金曜18時(翌土曜ぶん)・土日18時(翌日ぶん)・
土日朝8時(当日ぶんを再取得して予想を更新)。
月曜開催なし等でレースが見つからない日は何もせず正常終了する。

使い方:
  python scraper/odds.py                 # 翌日(JST)
  python scraper/odds.py --date 20260711
"""

import argparse
import re
import time
from datetime import date, datetime, timedelta

import requests
from bs4 import BeautifulSoup

import db as keiba_db
from scrape import COURSE, HEADERS

ODDS_API = "https://race.netkeiba.com/api/api_get_jra_odds.html"
SLEEP_BETWEEN_RACES = 1.5
SLEEP_BETWEEN_REQUESTS = 0.5   # 同一レースの出馬表→オッズ間


def get_race_ids_by_place(yyyymmdd):
    """SP版race_listから {place: [race_id]} を1リクエストで取得。
    (scrape.get_race_ids_realtime の全場版)"""
    url = f"https://race.sp.netkeiba.com/?pid=race_list&kaisai_date={yyyymmdd}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")

    code2place = {v: k for k, v in COURSE.items()}
    tabs = soup.select("li[data-kaisaidate]")
    slides = soup.select("div.RaceList_SlideBoxItem")
    out = {}
    for idx, tab in enumerate(tabs):
        if tab.get("data-kaisaidate") != yyyymmdd or idx >= len(slides):
            continue
        m = re.fullmatch(r"cd(\d{2})", tab.get("id") or "")
        place = code2place.get(m.group(1)) if m else None
        if not place:
            continue
        ids = set()
        for a in slides[idx].select("a[href*='race_id']"):
            mm = re.search(r"race_id=(\d{12})", a.get("href", ""))
            if mm:
                ids.add(mm.group(1))
        if ids:
            out[place] = sorted(ids)
    return out


def fetch_win_odds(race_id):
    """netkeibaオッズAPIから単勝オッズを取得。
    {umaban: (odds|None, popularity|None)}。発売前・取消はodds None。"""
    r = requests.get(
        ODDS_API,
        params={"race_id": race_id, "type": "1", "action": "init"},
        headers=HEADERS, timeout=20)
    win = (((r.json().get("data") or {}).get("odds") or {}).get("1")) or {}
    out = {}
    for k, v in win.items():
        try:
            umaban = int(k)
        except ValueError:
            continue
        try:
            odds = float(v[0])
        except (ValueError, TypeError, IndexError):
            odds = None
        if odds is not None and odds <= 0:
            odds = None            # "0.0" = 発売前/取消
        try:
            pop = int(v[2])
        except (ValueError, TypeError, IndexError):
            pop = None
        out[umaban] = (odds, pop)
    return out


def fetch_shutuba(race_id):
    """出馬表ページからレースメタと出走各馬を取得。
    {race_name, surface, distance, entries:[{umaban, wakuban, horse, jockey}]}"""
    url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")

    name_el = soup.select_one(".RaceName")
    race_name = name_el.get_text(strip=True) if name_el else None
    # グレード: レース名横のアイコン(Icon_GradeType1/2/3 = G1/G2/G3)。
    # 出馬表のレース名文字列には(GIII)等が入らないため、ここで拾うしかない
    grade = None
    icon = soup.select_one(".RaceName [class*='Icon_GradeType']")
    if icon:
        for cls in icon.get("class") or []:
            m = re.fullmatch(r"Icon_GradeType([123])", cls)
            if m:
                grade = f"G{m.group(1)}"
                break
    info = soup.select_one("div.RaceData01")
    text = info.get_text(" ", strip=True) if info else ""
    if "障" in text:               # "障芝" があるので障害を先に判定
        surface = "障害"
    elif re.search(r"芝\s*\d", text):
        surface = "芝"
    elif re.search(r"ダ\s*\d", text):
        surface = "ダート"
    else:
        surface = None
    dist_m = re.search(r"(\d{3,4})m", text)
    distance = int(dist_m.group(1)) if dist_m else None
    # 柵設定 A/B/C/D: "芝1200m (右 A)" の距離直後の括弧内から拾う
    setting = None
    m = re.search(r"m\s*[((]([^))]*)[))]", text)
    if m:
        sm = re.search(r"(?:^|\s)([A-D])(?:\s|$)", m.group(1))
        if sm:
            setting = sm.group(1)

    # 出馬表本体のみ(同ページの展開予想テーブルもtr.HorseListを持つ)
    table = soup.select_one("table.ShutubaTable") or soup
    entries = []
    for tr in table.select("tr.HorseList"):
        umaban_el = tr.select_one("td[class*='Umaban']")
        try:
            umaban = int(umaban_el.get_text(strip=True))
        except (AttributeError, ValueError):
            continue
        waku = None
        waku_el = tr.select_one("td[class*='Waku']")
        if waku_el:
            m = re.search(r"\d+", waku_el.get_text())
            if m:
                waku = int(m.group())
        horse_a = tr.select_one(".HorseName a") or tr.select_one("a[href*='/horse/']")
        jockey_a = tr.select_one("td.Jockey a")
        horse = horse_a.get_text(strip=True) if horse_a else None
        if not horse:
            continue
        entries.append({
            "umaban": umaban,
            "wakuban": waku,
            "horse": horse,
            "jockey": jockey_a.get_text(strip=True) if jockey_a else None,
        })
    entries.sort(key=lambda e: e["umaban"])
    return {"race_name": race_name, "grade": grade, "surface": surface,
            "distance": distance, "course_setting": setting,
            "entries": entries}


def snapshot_date(yyyymmdd):
    """指定日の全レースをスナップショットしてkeiba.dbへ。取得レース数を返す。"""
    conn = keiba_db.connect()
    by_place = get_race_ids_by_place(yyyymmdd)
    if not by_place:
        print(f"{yyyymmdd}: 開催なし(または出馬表未公開)")
        return 0

    total = 0
    for place, ids in sorted(by_place.items()):
        n_ok = n_odds = 0
        for rid in ids:
            try:
                meta = fetch_shutuba(rid)
                time.sleep(SLEEP_BETWEEN_REQUESTS)
                odds = fetch_win_odds(rid)
            except Exception as e:
                print(f"  skip {rid}: {str(e)[:100]}")
                time.sleep(SLEEP_BETWEEN_RACES)
                continue
            if not meta["entries"]:
                print(f"  skip {rid}: 出馬表が空")
                time.sleep(SLEEP_BETWEEN_RACES)
                continue
            snapped = datetime.now().isoformat(timespec="seconds")
            entries = []
            for e in meta["entries"]:
                od, pop = odds.get(e["umaban"], (None, None))
                entries.append({**e, "race_id": rid, "win_odds": od,
                                "popularity": pop, "snapped_at": snapped})
            race = {
                "race_id": rid, "date": yyyymmdd, "place": place,
                "race_no": int(rid[10:12]), "race_name": meta["race_name"],
                "grade": meta["grade"],
                "surface": meta["surface"], "distance": meta["distance"],
                "course_setting": meta["course_setting"],
                "snapped_at": snapped,
            }
            keiba_db.upsert_forward(conn, race, entries)
            n_ok += 1
            total += 1
            if any(e["win_odds"] is not None for e in entries):
                n_odds += 1
            time.sleep(SLEEP_BETWEEN_RACES)
        print(f"[{place}] {n_ok}/{len(ids)}R 取得 (うちオッズあり{n_odds}R)")
    return total


def main():
    ap = argparse.ArgumentParser(description="発走前オッズスナップショット")
    ap.add_argument("--date", default=None,
                    help="対象日 YYYYMMDD (既定: 翌日JST)")
    args = ap.parse_args()
    target = args.date or (date.today() + timedelta(days=1)).strftime("%Y%m%d")
    print(f"発走前オッズスナップショット: {target}")
    n = snapshot_date(target)
    print(f"完了: {n}R")


if __name__ == "__main__":
    main()
