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

    def combo_matrix(sub):
        sub2 = sub.dropna(subset=["脚質"])
        if sub2.empty:
            return []
        grp = (
            sub2.groupby(["内外", "脚質"])
            .agg(n=("着順", "count"),
                 rate=("着順", lambda x: float((x <= 3).mean())))
            .reset_index()
        )
        return [
            {
                "内外": str(row["内外"]),
                "脚質": str(row["脚質"]),
                "複勝率": round(float(row["rate"]), 3),
                "出走数": int(row["n"]),
            }
            for _, row in grp.iterrows()
        ]

    def best_combo(sub, frame_bias, style_bias, min_n=3):
        # 4マス直接最大化はセルあたりn<25で分散が大きく、1頭分の差でブレる。
        # 周辺(内外/脚質)はnが倍以上あって安定するので、各次元で最大を選び
        # その組合せの実セルの率を返す。詳細な4マスは combo_matrix に残す。
        sub2 = sub.dropna(subset=["脚質"])
        if sub2.empty or not frame_bias or not style_bias:
            return None
        best_frame = max(frame_bias, key=lambda x: (x["複勝率"], x["出走数"]))
        best_style = max(style_bias, key=lambda x: (x["複勝率"], x["出走数"]))
        cell = sub2[(sub2["内外"] == best_frame["内外"]) & (sub2["脚質"] == best_style["脚質"])]
        if len(cell) < min_n:
            return None
        return {
            "内外": str(best_frame["内外"]),
            "脚質": str(best_style["脚質"]),
            "複勝率": round(float((cell["着順"] <= 3).mean()), 3),
            "出走数": int(len(cell)),
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

        fb = bias_rows(sub, "内外", ["内", "外"])
        sb = bias_rows(sub, "脚質", ["逃げ先行", "差し追込"])
        surfaces[surface] = {
            "race_count": int(n_races),
            "frame_bias": fb,
            "style_bias": sb,
            "best_combo": best_combo(sub, fb, sb),
            "combo_matrix": combo_matrix(sub),
        }

    return {
        "place": place,
        "date": yyyymmdd,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "total_races": int(df["race_id"].nunique()),
        "surfaces": surfaces,
        "hot_jockeys": hot_jockeys_for(df),
        "races": per_race_top3(df),
    }


def per_race_top3(df):
    """各レースの上位3頭の属性(着順/内外/脚質/騎手)を抽出。
    結果ページの前日バイアス照合に使う。"""
    races = []
    for rid, group in df.groupby("race_id"):
        race_no = int(str(rid)[-2:])
        surface = str(group["コース"].iloc[0]) if len(group) else "?"
        top3 = group[group["着順"].isin([1, 2, 3])].sort_values("着順")
        top3_data = []
        for _, row in top3.iterrows():
            top3_data.append({
                "着順": int(row["着順"]),
                "馬番": int(row["馬番"]) if pd.notna(row["馬番"]) else None,
                "内外": str(row["内外"]) if pd.notna(row["内外"]) else None,
                "脚質": str(row["脚質"]) if pd.notna(row["脚質"]) else None,
                "騎手": str(row["騎手"]) if pd.notna(row["騎手"]) else None,
            })
        races.append({"R": race_no, "surface": surface, "top3": top3_data})
    races.sort(key=lambda x: x["R"])
    return races


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
        try:
            existing = json.loads(out_path.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
        if "races" in existing:
            print(f"[{place}] 既存ファイルあり、スキップ: {out_path.name}")
            return {"place": place, "date": yyyymmdd, "filename": out_path.name}
        print(f"[{place}] 既存ファイルにレース詳細なし、再処理します")

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
    # date は降順 (新しい順)、同一日内では place は昇順
    items.sort(key=lambda x: x["place"])
    items.sort(key=lambda x: x["date"], reverse=True)

    index = {
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "count": len(items),
        "items": items,
    }
    with open(DATA_DIR / "index.json", "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, indent=2)
    print(f"\nindex.json: {len(items)}件")


FRAME_LABEL = {"内": "内枠", "外": "外枠"}


def _winning_entry(bias_list, key):
    """frame_bias / style_bias から複勝率最大の区分名を返す。"""
    if not bias_list:
        return None
    best = max(bias_list, key=lambda x: (x["複勝率"], x["出走数"]))
    return best[key]


def _build_kekka_for_place(today_data, prev_data):
    winning_by_surface = {}
    surfaces_combo = {}
    for surface, sdata in (prev_data.get("surfaces") or {}).items():
        winning_by_surface[surface] = {
            "frame": _winning_entry(sdata.get("frame_bias", []), "内外"),
            "style": _winning_entry(sdata.get("style_bias", []), "脚質"),
        }
        bc = sdata.get("best_combo")
        if bc:
            surfaces_combo[surface] = bc

    hot_jockey_names = {hj["騎手"] for hj in (prev_data.get("hot_jockeys") or [])}

    races_out = []
    for race in today_data.get("races", []):
        surface = race.get("surface")
        win = winning_by_surface.get(surface, {"frame": None, "style": None})
        hits = []
        for horse in race.get("top3", []):
            labels = []
            if win["frame"] and horse.get("内外") == win["frame"]:
                labels.append(FRAME_LABEL.get(horse["内外"], horse["内外"]))
            if win["style"] and horse.get("脚質") == win["style"]:
                labels.append(horse["脚質"])
            if horse.get("騎手") and horse["騎手"] in hot_jockey_names:
                labels.append(horse["騎手"])
            hits.append({
                "着順": horse["着順"],
                "馬番": horse.get("馬番"),
                "labels": labels,
            })
        races_out.append({"R": race["R"], "surface": surface, "hits": hits})

    return {
        "place": today_data["place"],
        "prev_date": prev_data["date"],
        "surfaces": surfaces_combo,
        "races": races_out,
    }


def build_kekka(target_date):
    """{target_date}_結果.json を生成。前日JSONがある場のみ対象。"""
    today_date_obj = datetime.strptime(target_date, "%Y%m%d").date()
    prev_date = (today_date_obj - timedelta(days=1)).strftime("%Y%m%d")
    out_filename = f"{target_date}_結果.json"

    places_out = []
    for f in sorted(DATA_DIR.glob(f"{target_date}_*.json")):
        if f.name == out_filename:
            continue
        m = re.match(rf"{target_date}_(.+)\.json", f.name)
        if not m:
            continue
        place = m.group(1)
        prev_path = DATA_DIR / f"{prev_date}_{place}.json"
        if not prev_path.exists():
            continue
        today_data = json.loads(f.read_text(encoding="utf-8"))
        prev_data = json.loads(prev_path.read_text(encoding="utf-8"))
        if "races" not in today_data:
            print(f"  [{place}] racesフィールドなし、結果スキップ")
            continue
        places_out.append(_build_kekka_for_place(today_data, prev_data))

    if not places_out:
        print(f"\n結果({target_date}): 前日データなし、生成スキップ")
        return

    out = {
        "date": target_date,
        "prev_date": prev_date,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "places": places_out,
    }
    out_path = DATA_DIR / out_filename
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(f"\n結果: {out_path.name} ({len(places_out)}場)")


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    target_date = sys.argv[1] if len(sys.argv) > 1 else None
    if target_date:
        print(f"対象日付: {target_date}")

    dates_touched = set()
    for place in COURSE.keys():
        try:
            result = process_place(place, target_date)
            if result:
                dates_touched.add(result["date"])
        except Exception as e:
            print(f"[{place}] エラー: {e}")
        time.sleep(SLEEP_BETWEEN_PLACES)

    for d in sorted(dates_touched):
        build_kekka(d)

    update_index()
    print("\n完了")


if __name__ == "__main__":
    main()
