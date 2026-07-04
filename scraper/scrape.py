"""
netkeibaから直近開催のレース結果を取得し、各競馬場ごとの分析結果を
public/data/site.db (SQLite) に書き込む。

GitHub Actionsから1日1回呼び出される想定。
旧: public/data/{date}_{place}.json 出力。2026-07にSQLite化(JSON廃止)。
結果ふりかえり(旧{date}_結果.json)とindex.jsonはフロント側でsite.dbから
導出するため生成しない。
"""

import re
import sys
import time
from datetime import date, datetime, timedelta
from io import StringIO

import pandas as pd
import requests
from bs4 import BeautifulSoup

import site_db

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

    # レース名: db.netkeiba は data_intro 内 <h1> もしくは racedata 内
    name_tag = (soup.select_one("div.data_intro h1")
                or soup.select_one("dl.racedata h1")
                or soup.select_one("h1.race_name"))
    race_name = name_tag.get_text(strip=True) if name_tag else None

    df["コース"] = course
    df["距離"] = distance
    df["馬場"] = condition
    df["レース名"] = race_name

    # 前走脚質も付与。realtime経路と同じ列を持たせることで、
    # bias計算と注目レース(entryも前走脚質)のchip判断の意味を揃える。
    zenso = _fetch_zenso_styles(race_id)
    df["脚質_前走"] = pd.to_numeric(df["馬番"], errors="coerce").map(
        lambda b: zenso.get(int(b)) if pd.notna(b) else None
    )
    return df


def get_race_ids_realtime(yyyymmdd, place):
    """race.sp.netkeiba.com のSP版race_listからrace_idを抽出。
    db.netkeiba.com/race/list/ は当週分のrace_idが火曜頃まで入らないので、
    土日当日のスクレイプはこちら経由。"""
    url = f"https://race.sp.netkeiba.com/?pid=race_list&kaisai_date={yyyymmdd}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    # race.sp.netkeiba.com は UTF-8 配信(db.netkeiba.com は引き続き EUC-JP)。
    r.encoding = "utf-8"
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


def _extract_grade(name_tag):
    """RaceName内のグレードアイコン(Icon_GradeTypeN)から重賞グレードを判定。
    netkeibaは G1=Type1 / G2=Type2 / G3=Type3。L/OP/未グレードや
    地方重賞等(Type5,13...)は None を返す = 「G3以上のJRA重賞」のみ拾う。"""
    if not name_tag:
        return None
    for span in name_tag.find_all("span"):
        for cls in span.get("class", []):
            m = re.fullmatch(r"Icon_GradeType(\d+)", cls)
            if m and int(m.group(1)) in (1, 2, 3):
                return f"G{int(m.group(1))}"
    return None


def fetch_main_race_entries(race_id):
    """11R(注目レース)の出馬表を shutuba_past.html から全頭抽出。
    {race_name, grade, surface, entries:[{枠,馬番,馬名,騎手,内外,脚質}]} を返す。
    grade は G1/G2/G3 か None。脚質は各馬の前走通過順から推定
    (直近5走の総合ラベルではない)。取れない(出馬表未公開等)場合は None。"""
    url = f"https://race.netkeiba.com/race/shutuba_past.html?race_id={race_id}&rf=shutuba_submenu"
    r = requests.get(url, headers=HEADERS, timeout=20)
    # race.netkeiba.com は UTF-8 配信(db.netkeiba.com は引き続き EUC-JP)。
    r.encoding = "utf-8"
    soup = BeautifulSoup(r.text, "html.parser")
    table = soup.select_one("table.Shutuba_Past5_Table")
    if not table:
        return None

    entries = []
    for row in table.find_all("tr")[1:]:
        cells = row.find_all(["th", "td"])
        if len(cells) < 6:
            continue
        try:
            waku = int(cells[0].get_text(strip=True))
            umaban = int(cells[1].get_text(strip=True))
        except ValueError:
            continue
        horse_a = cells[3].select_one(".Horse02 a")
        horse_name = horse_a.get_text(strip=True) if horse_a else ""
        jockey_a = cells[4].select_one("a")
        jockey = jockey_a.get_text(strip=True) if jockey_a else ""
        style = _style_from_zenso_cell(cells[5])
        if not horse_name:
            continue
        entries.append({
            "枠": waku,
            "馬番": umaban,
            "馬名": horse_name,
            "騎手": jockey,
            "内外": "内" if waku <= 4 else "外",
            "脚質": style,
        })

    if not entries:
        return None

    name_tag = soup.select_one(".RaceName")
    race_name = name_tag.get_text(strip=True) if name_tag else None
    grade = _extract_grade(name_tag)
    info_tag = soup.select_one("div.RaceData01")
    info_text = info_tag.get_text(" ", strip=True) if info_tag else ""
    if "芝" in info_text:
        surface = "芝"
    elif "ダ" in info_text:
        surface = "ダート"
    else:
        surface = None

    entries.sort(key=lambda e: e["馬番"])
    return {"race_name": race_name, "grade": grade, "surface": surface, "entries": entries}


_NEXT_KAISAI_CACHE = {}


def find_next_kaisai_date(from_date, place, max_forward=7):
    """from_date(YYYYMMDD)より後で最初に開催のある日とそのrace_idsを返す。
    なければ (None, [])。最大 max_forward 日先まで探索。"""
    try:
        base = datetime.strptime(from_date, "%Y%m%d").date()
    except ValueError:
        return None, []
    for i in range(1, max_forward + 1):
        d = (base + timedelta(days=i)).strftime("%Y%m%d")
        key = (d, place)
        if key in _NEXT_KAISAI_CACHE:
            ids = _NEXT_KAISAI_CACHE[key]
        else:
            try:
                ids = get_race_ids_realtime(d, place)
            except Exception:
                ids = []
            _NEXT_KAISAI_CACHE[key] = ids
            time.sleep(0.5)
        if ids:
            return d, ids
    return None, []


def build_notable_race(from_date, place):
    """from_date より後の最初の開催日の 11R を取得して notable_race dict を返す。
    取れなければ None。"""
    next_date, ids = find_next_kaisai_date(from_date, place)
    if not next_date:
        return None
    rid_11 = next((rid for rid in ids if rid.endswith("11")), None)
    if not rid_11:
        return None
    try:
        info = fetch_main_race_entries(rid_11)
    except Exception as e:
        print(f"  [{place}] 注目レース取得失敗: {str(e)[:100]}")
        return None
    if not info:
        return None
    info["date"] = next_date
    info["R"] = 11
    return info


def _style_from_zenso_cell(cell):
    """馬柱の前走セルから脚質を推定。通過順 "2-5-5" + "16頭" を読み derive_style に渡す。
    直線/障害(コーナー無し)や前走なしは None。"""
    text = cell.get_text(" | ", strip=True)
    m_pass = re.search(r"(\d+(?:-\d+){1,4})\s*\(\s*\d+\.\d+\s*\)", text)
    m_head = re.search(r"(\d+)頭", text)
    if not m_pass or not m_head:
        return None
    return derive_style(m_pass.group(1), int(m_head.group(1)))


def _fetch_zenso_styles(race_id):
    """馬柱(5走表示)ページから各馬の前走脚質を推定。
    realtime結果ページの "コーナー通過順" はレース終了直後だと未反映で空のことが
    多い。代替として各馬の前走の通過順位+頭数から脚質を推定する。
    {馬番(int): "逃げ先行"|"差し追込"|None} を返す。
    新馬戦・休養明け等で前走情報がない馬は脚質Noneになる。"""
    url = f"https://race.netkeiba.com/race/shutuba_past.html?race_id={race_id}&rf=shutuba_submenu"
    r = requests.get(url, headers=HEADERS, timeout=20)
    # race.netkeiba.com は UTF-8 配信(db.netkeiba.com は引き続き EUC-JP)。
    r.encoding = "utf-8"
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
        out[umaban] = _style_from_zenso_cell(cells[5])
    return out


def get_result_realtime(race_id):
    """race.netkeiba.com の結果ページ(レース終了直後反映)から結果テーブル取得。
    列名は db.netkeiba.com 側に合わせて正規化(枠→枠番, 後3F→上り,
    コーナー通過順→通過)。"""
    url = f"https://race.netkeiba.com/race/result.html?race_id={race_id}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    # race.netkeiba.com は UTF-8 配信(db.netkeiba.com は引き続き EUC-JP)。
    r.encoding = "utf-8"
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

    name_tag = soup.select_one(".RaceName")
    race_name = name_tag.get_text(strip=True) if name_tag else None

    df["コース"] = course
    df["距離"] = distance
    df["馬場"] = condition
    df["レース名"] = race_name

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
    # 騎手の評価記号(▲★△等)を集計前に剥がす。realtimeソースだと同一騎手が
    # 減量印付き/無しで2行に分裂し hot_jockeys が別エントリで二重計上される。
    if "騎手" in df.columns:
        df["騎手"] = df["騎手"].apply(
            lambda x: _normalize_jockey_name(x) if pd.notna(x) else x
        )
    df["頭数"] = df.groupby("race_id")["馬番"].transform("count")
    # realtime/db両経路で "脚質_前走" 列を付与しており、これを脚質として採用する。
    # 注目レースのentryも前走脚質ベースなので、bias計算と意味が揃いchip判断が
    # 一貫する(rebuild時に当該レースの通過順を使うと、entry側=前走と意味がズレる)。
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
        race_name = None
        if "レース名" in group.columns:
            rn = group["レース名"].iloc[0]
            if pd.notna(rn) and rn:
                race_name = str(rn)
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
        races.append({
            "R": race_no,
            "surface": surface,
            "race_name": race_name,
            "top3": top3_data,
        })
    races.sort(key=lambda x: x["R"])
    return races


# ---------- 実行制御 ----------
def is_opening_day(race_ids):
    """その開催の初日(=開催日コードが01)かどうか判定。
    レースID 12桁の9-10桁目が開催日。"""
    if not race_ids:
        return False
    days = set(rid[8:10] for rid in race_ids)
    return days == {"01"}


def raced_same_track_prev_week(conn, place, yyyymmdd, lookback=8):
    """前週(直近lookback日以内)に同一競馬場の開催データが存在するか。
    回の初日(コード01)でも、前週が同じ競馬場なら(例: 2回東京→3回東京)
    馬場が連続しておりバイアス比較は有効。逆に前走の馬場データが「別競馬場」
    になるのは、前週まで別の場で開催していた新規場入りの初日のみ。"""
    try:
        d = datetime.strptime(yyyymmdd, "%Y%m%d").date()
    except ValueError:
        return False
    date_from = (d - timedelta(days=lookback)).strftime("%Y%m%d")
    row = conn.execute(
        "SELECT 1 FROM reports WHERE place = ? AND date >= ? AND date < ? LIMIT 1",
        (place, date_from, yyyymmdd)).fetchone()
    return row is not None


def process_place(conn, place, target_date=None):
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

    if is_opening_day(ids) and not raced_same_track_prev_week(conn, place, yyyymmdd):
        print(f"[{place}] {yyyymmdd}: 新規場入りの開催初日のためスキップ(別競馬場とバイアス比較無意味)")
        return None

    print(f"[{place}] 開催日: {yyyymmdd} / {len(ids)}R")

    new_source = "realtime" if use_realtime else "db"
    existing = conn.execute(
        "SELECT source FROM reports WHERE date = ? AND place = ?",
        (yyyymmdd, place)).fetchone()
    if existing:
        existing_source = existing[0] or "db"
        # 火曜rebuildで realtime → db に格上げするケースだけは上書き許可。
        # 既存と同じ精度(realtime→realtime, db→db) や db→realtime の格下げはスキップ。
        if existing_source == "realtime" and new_source == "db":
            print(f"[{place}] 既存={existing_source} を db で上書きします")
        else:
            print(f"[{place}] 既存データあり、スキップ: {yyyymmdd}_{place} (source={existing_source})")
            return {"place": place, "date": yyyymmdd}

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

    nr = build_notable_race(yyyymmdd, place)
    if nr:
        analysis["notable_race"] = nr
        print(f"[{place}] 注目レース: {nr['date']} 11R "
              f"({nr.get('race_name') or '?'}, {len(nr['entries'])}頭)")
    else:
        print(f"[{place}] 注目レース: 翌開催日の出馬表未公開のためスキップ")

    site_db.write_report(conn, analysis)
    print(f"[{place}] site.db 保存: {yyyymmdd}_{place}")
    return {"place": place, "date": yyyymmdd}


def _normalize_jockey_name(name):
    """評価記号(▲★△◇▽◎○☆▼)を先頭から剥がす。
    realtime取得分は3字省略+減量印付き("▲森田"等)で、db取得分はフル名
    ("森田Sもう一字"等)。比較は別途prefix一致で吸収する。"""
    if not name:
        return ""
    return re.sub(r"^[▲★△◇▽◎○☆▼]+", "", name).strip()


def _find_latest_kaisai_date(conn):
    """site.db 内で最も新しい開催日(YYYYMMDD)を返す。なければ None。"""
    row = conn.execute("SELECT MAX(date) FROM reports").fetchone()
    return row[0] if row else None


def update_notable_races(conn, target_date=None):
    """target_date(未指定なら直近開催日)の各場について、notable_race だけ
    再取得して上書きする。金曜18時JST想定: 翌日(土)の出馬表は公開済みなので、
    日曜scrape時点で None だった notable_race を埋め直せる。"""
    if not target_date:
        target_date = _find_latest_kaisai_date(conn)
        if not target_date:
            print("対象データなし")
            return
    print(f"\n=== notable_race 更新: {target_date} ===")
    updated = 0
    places = [r[0] for r in conn.execute(
        "SELECT place FROM reports WHERE date = ? ORDER BY place", (target_date,))]
    for place in places:
        nr = build_notable_race(target_date, place)
        if not nr:
            print(f"  [{place}] 注目レース: 取れず(出馬表未公開?)、スキップ")
            continue
        site_db.write_notable(conn, target_date, place, nr)
        site_db.touch_generated_at(
            conn, target_date, place,
            datetime.now().isoformat(timespec="seconds"))
        print(f"  [{place}] 注目レース更新: {nr['date']} 11R ({nr.get('race_name') or '?'})")
        updated += 1
        time.sleep(SLEEP_BETWEEN_PLACES)
    print(f"\n完了: {updated}場更新")


def main():
    conn = site_db.connect()
    args = sys.argv[1:]
    if "--notable-only" in args:
        args.remove("--notable-only")
        target_date = args[0] if args else None
        update_notable_races(conn, target_date)
        return
    target_date = args[0] if args else None
    if target_date:
        print(f"対象日付: {target_date}")

    for place in COURSE.keys():
        try:
            process_place(conn, place, target_date)
        except Exception as e:
            print(f"[{place}] エラー: {e}")
        time.sleep(SLEEP_BETWEEN_PLACES)

    print("\n完了")


if __name__ == "__main__":
    main()
