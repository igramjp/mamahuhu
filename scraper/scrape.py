"""
netkeibaから直近開催のレース結果を取得し、各競馬場ごとに分析結果を
public/data/{date}_{place}.json として出力する。

GitHub Actionsから1日1回呼び出される想定。
"""

import json
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

COURSE = {
    "札幌": "01", "函館": "02", "福島": "03", "新潟": "04", "東京": "05",
    "中山": "06", "中京": "07", "京都": "08", "阪神": "09", "小倉": "10",
}
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}
DATA_DIR = Path(__file__).resolve().parent.parent / "public" / "data"
SLEEP_BETWEEN_RACES = 1.5  # 秒
SLEEP_BETWEEN_PLACES = 2.0


# ---------- スクレイピング ----------
def get_race_ids(yyyymmdd, place):
    url = f"https://db.netkeiba.com/race/list/{yyyymmdd}/"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.encoding = "EUC-JP"
    soup = BeautifulSoup(r.text, "html.parser")
    cc = COURSE[place]
    ids = set()
    for a in soup.select("a[href*='/race/']"):
        href = a.get("href", "")
        m = re.search(r"/race/(\d{12})", href)
        if m and m.group(1)[4:6] == cc:
            ids.add(m.group(1))
    return sorted(ids)


def _extract_race_table(html_text):
    """netkeibaの結果テーブル(table.race_table_01)を抽出。
    pandas.read_htmlは複雑なthセル(指数列のネストspan等)で列数を誤るため、
    手動BeautifulSoup→pandasフォールバックの順で試す。"""
    # 戦略1: race_table_01 を手動でparse (最も信頼できる)
    soup = BeautifulSoup(html_text, "html.parser")
    table = soup.select_one("table.race_table_01")
    if table:
        rows = table.find_all("tr")
        if len(rows) >= 2:
            header = [c.get_text(strip=True) for c in rows[0].find_all(["th", "td"])]
            data = []
            for row in rows[1:]:
                cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
                if len(cells) == len(header):
                    data.append(cells)
            if data:
                return pd.DataFrame(data, columns=header)

    # 戦略2: pandas read_html フォールバック
    for flavor in ("bs4", "lxml"):
        try:
            tables = pd.read_html(StringIO(html_text), flavor=flavor)
            if tables:
                return tables[0]
        except Exception:
            pass

    raise ValueError("結果テーブルが見つかりません")


def get_result(race_id):
    url = f"https://db.netkeiba.com/race/{race_id}/"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.encoding = "EUC-JP"

    df = _extract_race_table(r.text)
    df["race_id"] = race_id

    soup = BeautifulSoup(r.text, "html.parser")
    info_text = ""
    for sel in ["diary_snap_cut", "p.diary_snap_cut", "div.data_intro"]:
        tag = soup.select_one(sel)
        if tag:
            t = tag.get_text(" ", strip=True)
            if "m" in t:
                info_text = t
                break

    if "芝" in info_text:
        course = "芝"
    elif "ダ" in info_text:
        course = "ダート"
    elif "障" in info_text:
        course = "障害"
    else:
        course = "?"

    dist_m = re.search(r"(\d{3,4})m", info_text)
    distance = int(dist_m.group(1)) if dist_m else None
    condition = next((c for c in ["不良", "稍重", "重", "良"] if c in info_text), None)

    df["コース"] = course
    df["距離"] = distance
    df["馬場"] = condition
    return df


def find_latest_race_date(place, max_back=14):
    for i in range(max_back):
        d = (date.today() - timedelta(days=i)).strftime("%Y%m%d")
        try:
            ids = get_race_ids(d, place)
        except Exception as e:
            print(f"  [{place}] {d} エラー: {e}")
            ids = []
        if ids:
            return d, ids
        time.sleep(1)
    return None, []


# ---------- 分析 ----------
def derive_style(passing, n_horses):
    if pd.isna(passing) or passing == "":
        return None
    try:
        positions = [int(p) for p in str(passing).split("-")]
    except ValueError:
        return None
    last = positions[-1]
    return "逃げ先行" if last <= n_horses / 2 else "差し追込"


def analyze_to_dict(df, place, yyyymmdd):
    df = df.copy()
    for col in ["着順", "枠番", "馬番", "人気"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["上り"] = pd.to_numeric(df["上り"], errors="coerce")
    df = df.dropna(subset=["着順", "枠番", "人気"])
    df["頭数"] = df.groupby("race_id")["馬番"].transform("count")
    df["脚質"] = df.apply(lambda r: derive_style(r["通過"], r["頭数"]), axis=1)
    df["内外"] = df["枠番"].apply(lambda x: "内" if x <= 4 else "外")

    def bias_rows(sub, key, labels):
        out = []
        for label in labels:
            grp = sub[sub[key] == label]
            if grp.empty:
                continue
            out.append({
                key: label,
                "勝率": round(float((grp["着順"] == 1).mean()), 3),
                "複勝率": round(float((grp["着順"] <= 3).mean()), 3),
                "出走数": int(len(grp)),
            })
        return out

    def best_combo(sub, min_n=3):
        sub2 = sub.dropna(subset=["脚質"])
        if sub2.empty:
            return None
        grp = (
            sub2.groupby(["内外", "脚質"])
            .agg(n=("着順", "count"),
                 rate=("着順", lambda x: float((x <= 3).mean())))
            .reset_index()
        )
        grp = grp[grp["n"] >= min_n]
        if grp.empty:
            return None
        best = grp.sort_values(["rate", "n"], ascending=[False, False]).iloc[0]
        return {
            "内外": str(best["内外"]),
            "脚質": str(best["脚質"]),
            "複勝率": round(float(best["rate"]), 3),
            "出走数": int(best["n"]),
        }

    df["人気差"] = df["人気"] - df["着順"]

    def hot_jockeys_for(sub):
        if sub.empty:
            return []
        sub = sub.copy()
        # 複勝圏内(1-3着)のときだけ人気差を残す
        sub["人気差_複勝"] = sub["人気差"].where(sub["着順"] <= 3)
        jockey = sub.groupby("騎手").agg(
            最大人気差=("人気差_複勝", "max"),
            騎乗数=("着順", "count"),
            勝利=("着順", lambda x: int((x == 1).sum())),
            複勝=("着順", lambda x: int((x <= 3).sum())),
        )
        # 複勝圏内で人気差+5以上のサプライズを記録した騎手
        jockey = jockey[jockey["最大人気差"] >= 5]
        jockey = jockey.sort_values("最大人気差", ascending=False)
        return [
            {
                "騎手": str(name),
                "最大人気差": float(row["最大人気差"]),
                "騎乗数": int(row["騎乗数"]),
                "勝利": int(row["勝利"]),
                "複勝": int(row["複勝"]),
            }
            for name, row in jockey.iterrows()
        ]

    surfaces = {}
    for surface in ["芝", "ダート"]:
        sub = df[df["コース"] == surface]
        n_races = sub["race_id"].nunique()
        if n_races == 0:
            continue

        surfaces[surface] = {
            "race_count": int(n_races),
            "frame_bias": bias_rows(sub, "内外", ["内", "外"]),
            "style_bias": bias_rows(sub, "脚質", ["逃げ先行", "差し追込"]),
            "best_combo": best_combo(sub),
        }

    return {
        "place": place,
        "date": yyyymmdd,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_races": int(df["race_id"].nunique()),
        "surfaces": surfaces,
        "hot_jockeys": hot_jockeys_for(df),
    }


# ---------- 実行制御 ----------
def is_opening_day(race_ids):
    """その開催の初日(=開催日コードが01)かどうか判定。
    レースID 12桁の9-10桁目が開催日。
    初日は過去走の馬場データが「別競馬場」のものになるためバイアス比較が無意味。"""
    if not race_ids:
        return False
    days = set(rid[8:10] for rid in race_ids)
    return days == {"01"}


def process_place(place, target_date=None):
    print(f"\n=== {place} ===")
    if target_date:
        ids = get_race_ids(target_date, place)
        yyyymmdd = target_date
        if not ids:
            print(f"[{place}] {target_date} 開催なし")
            return None
    else:
        yyyymmdd, ids = find_latest_race_date(place)
        if not ids:
            print(f"[{place}] 直近2週間に開催なし")
            return None

    if is_opening_day(ids):
        print(f"[{place}] {yyyymmdd}: 開催初日のためスキップ(競馬場異なるとバイアス比較無意味)")
        return None

    print(f"[{place}] 開催日: {yyyymmdd} / {len(ids)}R")

    out_path = DATA_DIR / f"{yyyymmdd}_{place}.json"
    if out_path.exists():
        print(f"[{place}] 既存ファイルあり、スキップ: {out_path.name}")
        return {"place": place, "date": yyyymmdd, "filename": out_path.name}

    dfs = []
    for rid in ids:
        try:
            dfs.append(get_result(rid))
            time.sleep(SLEEP_BETWEEN_RACES)
        except Exception as e:
            msg = str(e)[:150]  # 長いエラーは切る(HTMLが出てくることがある)
            print(f"  skip {rid}: {msg}")

    if not dfs:
        return None

    df = pd.concat(dfs, ignore_index=True)
    analysis = analyze_to_dict(df, place, yyyymmdd)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(analysis, f, ensure_ascii=False, indent=2)
    print(f"[{place}] 保存: {out_path.name}")
    return {"place": place, "date": yyyymmdd, "filename": out_path.name}


def update_index():
    items = []
    for f in DATA_DIR.glob("*.json"):
        if f.name == "index.json":
            continue
        m = re.match(r"(\d{8})_(.+)\.json", f.name)
        if m:
            items.append({
                "date": m.group(1),
                "place": m.group(2),
                "filename": f.name,
            })
    items.sort(key=lambda x: (x["date"], x["place"]), reverse=True)

    index = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "count": len(items),
        "items": items,
    }
    with open(DATA_DIR / "index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    print(f"\nindex.json: {len(items)}件")


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    if target_date:
        print(f"対象日付: {target_date}")
    for place in COURSE.keys():
        try:
            process_place(place, target_date)
        except Exception as e:
            print(f"[{place}] エラー: {e}")
        time.sleep(SLEEP_BETWEEN_PLACES)
    update_index()
    print("\n完了")


if __name__ == "__main__":
    main()
