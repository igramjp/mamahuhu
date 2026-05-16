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

# race.netkeiba.com(リアルタイム側)の列名 → db側列名へのマッピング。
# 当日スクレイプ用パスでのみ使う。
REALTIME_COLMAP = {
    "枠": "枠番",
    "後3F": "上り",
    "コーナー通過順": "通過",
}
# target_date が今日からこの日数以内ならリアルタイム側を使う。
# db.netkeiba.com の当週分は火曜前後まで反映されないため。
REALTIME_WINDOW_DAYS = 3


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


def get_race_ids_realtime(yyyymmdd, place):
    """race.sp.netkeiba.com のSP版race_listからrace_idを抽出。
    db.netkeiba.com/race/list/ は当週分のrace_idが火曜頃まで入らないので、
    土日当日のスクレイプはこちら経由。"""
    url = f"https://race.sp.netkeiba.com/?pid=race_list&kaisai_date={yyyymmdd}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.encoding = "EUC-JP"
    soup = BeautifulSoup(r.text, "html.parser")

    venue_code = COURSE[place]
    tabs = soup.select("li[data-kaisaidate]")
    target_index = None
    for idx, tab in enumerate(tabs):
        if (tab.get("data-kaisaidate") == yyyymmdd
                and tab.get("id") == f"cd{venue_code}"):
            target_index = idx
            break
    if target_index is None:
        return []

    slides = soup.select("div.RaceList_SlideBoxItem")
    if target_index >= len(slides):
        return []

    ids = set()
    for a in slides[target_index].select("a[href*='race_id']"):
        m = re.search(r"race_id=(\d{12})", a.get("href", ""))
        if m:
            ids.add(m.group(1))
    return sorted(ids)


def _fetch_zenso_styles(race_id):
    """馬柱(5走表示)ページから各馬の前走脚質を推定。
    realtime結果ページの "コーナー通過順" はレース終了直後だと未反映で空のことが
    多い。代替として各馬の前走の通過順位+頭数から脚質を推定する。
    {馬番(int): "逃げ先行"|"差し追込"|None} を返す。
    新馬戦・休養明け等で前走情報がない馬は脚質Noneになる。"""
    url = f"https://race.netkeiba.com/race/shutuba_past.html?race_id={race_id}&rf=shutuba_submenu"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.encoding = "EUC-JP"
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.select_one("table.Shutuba_Past5_Table")
    if not table:
        return {}
    out = {}
    for row in table.find_all("tr")[1:]:
        cells = row.find_all(["th", "td"])
        if len(cells) < 6:
            continue
        try:
            umaban = int(cells[1].get_text(strip=True))
        except ValueError:
            continue
        zenso_text = cells[5].get_text(" | ", strip=True)
        # 通過: "2-5-5" or "1-1-1-1" 等。直線/障害は "コーナー無し" などになり該当しない。
        m_pass = re.search(r"(\d+(?:-\d+){1,4})\s*\(\s*\d+\.\d+\s*\)", zenso_text)
        m_head = re.search(r"(\d+)頭", zenso_text)
        if not m_pass or not m_head:
            out[umaban] = None
            continue
        out[umaban] = derive_style(m_pass.group(1), int(m_head.group(1)))
    return out


def get_result_realtime(race_id):
    """race.netkeiba.com の結果ページ(レース終了直後反映)から結果テーブル取得。
    列名は db.netkeiba.com 側に合わせて正規化(枠→枠番, 後3F→上り,
    コーナー通過順→通過)。"""
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.encoding = "EUC-JP"
    soup = BeautifulSoup(r.text, "html.parser")

    table = soup.select_one("table.RaceTable01.ResultRefund")
    if not table:
        raise ValueError("結果テーブル(RaceTable01.ResultRefund)が見つかりません")
    rows = table.find_all("tr")
    if len(rows) < 2:
        raise ValueError("結果テーブルが空")
    # ヘッダのcellは "着 順" 等の内部空白あり、除去してから正規化
    header = [re.sub(r"\s+", "", c.get_text(strip=True))
              for c in rows[0].find_all(["th", "td"])]
    data = []
    for row in rows[1:]:
        cells = [c.get_text(strip=True) for c in row.find_all(["th", "td"])]
        if len(cells) == len(header):
            data.append(cells)
    if not data:
        raise ValueError("結果テーブルにデータ行なし")
    df = pd.DataFrame(data, columns=header).rename(columns=REALTIME_COLMAP)
    df["race_id"] = race_id

    info_tag = soup.select_one("div.RaceData01")
    info_text = info_tag.get_text(" ", strip=True) if info_tag else ""

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
    # realtime側は "馬場:稍" のような一字表記。db側の二字表記("稍重")に揃える。
    m = re.search(r"馬場\s*[:：]\s*(不良|稍重|稍|重|良)", info_text)
    raw = m.group(1) if m else None
    condition = {"稍": "稍重"}.get(raw, raw)

    df["コース"] = course
    df["距離"] = distance
    df["馬場"] = condition

    # realtime結果ページの通過順は反映タイミングが安定しない(土曜18時=未反映、
    # 日曜18時=一部反映 などSat/Sunで挙動が変わる)。一貫性のため realtime
    # 経路では常に前走脚質をfallback値として持たせ、analyze_to_dictで優先採用する。
    # 火曜rebuild時(db経路)は本物の通過順から計算するので、この列は使われない。
    zenso = _fetch_zenso_styles(race_id)
    df["脚質_前走"] = pd.to_numeric(df["馬番"], errors="coerce").map(
        lambda b: zenso.get(int(b)) if pd.notna(b) else None
    )
    return df


def _use_realtime(target_date):
    """target_date が今日から REALTIME_WINDOW_DAYS 以内ならrealtime側を使う。"""
    if not target_date:
        return False
    try:
        d = datetime.strptime(target_date, "%Y%m%d").date()
    except ValueError:
        return False
    delta = (date.today() - d).days
    return 0 <= delta <= REALTIME_WINDOW_DAYS


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


def analyze_to_dict(df, place, yyyymmdd, source="db"):
    df = df.copy()
    for col in ["着順", "枠番", "馬番", "人気"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["上り"] = pd.to_numeric(df["上り"], errors="coerce")
    df = df.dropna(subset=["着順", "枠番", "人気"])
    df["頭数"] = df.groupby("race_id")["馬番"].transform("count")
    # realtime取得時は "脚質_前走" 列が常に付与されている。これを脚質として採用し、
    # 当日通過順の反映タイミング差(Sat/Sunで一部入る/入らない)による分析揺れを避ける。
    # 火曜rebuild(db経路)では "脚質_前走" 列がないので通過順から計算する。
    if "脚質_前走" in df.columns:
        df["脚質"] = df["脚質_前走"]
    else:
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
        "source": source,
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
    use_realtime = _use_realtime(target_date)
    id_fetcher = get_race_ids_realtime if use_realtime else get_race_ids
    result_fetcher = get_result_realtime if use_realtime else get_result
    if use_realtime:
        print(f"[{place}] source=realtime (race.netkeiba.com)")

    if target_date:
        ids = id_fetcher(target_date, place)
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

    new_source = "realtime" if use_realtime else "db"
    out_path = DATA_DIR / f"{yyyymmdd}_{place}.json"
    if out_path.exists():
        try:
            existing = json.loads(out_path.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
        if "races" in existing:
            existing_source = existing.get("source", "db")
            # 火曜rebuildで realtime → db に格上げするケースだけは上書き許可。
            # 既存と同じ精度(realtime→realtime, db→db) や db→realtime の格下げはスキップ。
            if existing_source == "realtime" and new_source == "db":
                print(f"[{place}] 既存={existing_source} を db で上書きします")
            else:
                print(f"[{place}] 既存ファイルあり、スキップ: {out_path.name} (source={existing_source})")
                return {"place": place, "date": yyyymmdd, "filename": out_path.name}
        else:
            print(f"[{place}] 既存ファイルにレース詳細なし、再処理します")

    dfs = []
    for rid in ids:
        try:
            dfs.append(result_fetcher(rid))
            time.sleep(SLEEP_BETWEEN_RACES)
        except Exception as e:
            msg = str(e)[:150]  # 長いエラーは切る(HTMLが出てくることがある)
            print(f"  skip {rid}: {msg}")

    if not dfs:
        return None

    df = pd.concat(dfs, ignore_index=True)
    analysis = analyze_to_dict(df, place, yyyymmdd, source=new_source)

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


def _normalize_jockey_name(name):
    """評価記号(▲★△◇▽◎○☆▼)を先頭から剥がす。
    realtime取得分は3字省略+減量印付き("▲森田"等)で、db取得分はフル名
    ("森田Sもう一字"等)。比較は別途prefix一致で吸収する。"""
    if not name:
        return ""
    return re.sub(r"^[▲★△◇▽◎○☆▼]+", "", name).strip()


def _jockey_in_hot_set(jockey_today, hot_names):
    """today側(realtime=3字省略のことが多い)と prev側hot_jockeyのフル名を
    prefix一致で照合。記号は両側で除去してから判定する。"""
    j = _normalize_jockey_name(jockey_today)
    if not j:
        return False
    for hn in hot_names:
        hnn = _normalize_jockey_name(hn)
        if not hnn:
            continue
        if hnn.startswith(j) or j.startswith(hnn):
            return True
    return False


def _winning_entry(bias_list, key):
    """frame_bias / style_bias から複勝率最大の区分名を返す。"""
    if not bias_list:
        return None
    best = max(bias_list, key=lambda x: (x["複勝率"], x["出走数"]))
    return best[key]


def _build_kekka_for_place(today_data, prev_data, hot_jockey_names):
    """hot_jockey_names は前日全場合算のセット。騎手は土日で競馬場を移動する
    ことがあるため、その場の前日だけでなく全場union で照合する。"""
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
            if horse.get("騎手") and _jockey_in_hot_set(horse["騎手"], hot_jockey_names):
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


def _find_prev_kaisai_date(target_date):
    """target_date 未満で最も新しい開催日(YYYYMMDD)を返す。なければ None。
    "前日" だと土曜は金曜=非開催で照合不能になるため、"直近の開催日" を採用する
    (例: 20260516(土) → 20260510(前週日) を返す)。"""
    candidates = set()
    for f in DATA_DIR.glob("*.json"):
        if f.name == "index.json":
            continue
        m = re.match(r"(\d{8})_(.+)\.json", f.name)
        if not m:
            continue
        if m.group(2) == "結果":
            continue
        if m.group(1) < target_date:
            candidates.add(m.group(1))
    return max(candidates) if candidates else None


def build_kekka(target_date):
    """{target_date}_結果.json を生成。直近開催日(target_date 未満で最も新しい
    データ日)を prev とし、そのバイアス・hot_jockeysと今日のtop3を照合する。"""
    prev_date = _find_prev_kaisai_date(target_date)
    if not prev_date:
        print(f"\n結果({target_date}): 直近開催データなし、生成スキップ")
        return
    out_filename = f"{target_date}_結果.json"
    prev_kekka = f"{prev_date}_結果.json"

    # 騎手は土日で開催場を移動する(土=東京、日=京都 等)。前日hot_jockeysは
    # 「前日同一場」ではなく「前日全場の合算」で照合する。
    hot_jockey_names = set()
    for pf in DATA_DIR.glob(f"{prev_date}_*.json"):
        if pf.name == prev_kekka:
            continue
        try:
            pdata = json.loads(pf.read_text(encoding="utf-8"))
        except Exception:
            continue
        for hj in (pdata.get("hot_jockeys") or []):
            if hj.get("騎手"):
                hot_jockey_names.add(hj["騎手"])

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
        places_out.append(_build_kekka_for_place(today_data, prev_data, hot_jockey_names))

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
