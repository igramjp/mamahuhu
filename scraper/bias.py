"""
新バイアス集計 (bias_analysis_spec.md フェーズ②)。

data/keiba.db(バックフィル済みの生データ)から:
- 直近開催日のバイアスを「相対枠位置3分割 × 正規化着順」で算出
- 過去2〜3年の長期ベースラインに向けてベイズ的に縮小(少サンプル補正)
- セグメント(場×トラック×距離カテゴリ×馬場2区分)は階層フォールバック
- 確度の目安(Nレース中Mレースで内有利)を併記

指標の定義:
  正規化着順 delta = mean(着順/出走頭数) - レース期待値((完走数+1)/2/出走頭数)
  delta < 0 ならそのグループは平均より前の着順 = 有利。

使い方:
  python scraper/bias.py 東京                # DB内の最新開催日
  python scraper/bias.py 京都 --date 20230129
"""

import argparse
import json
from datetime import datetime, timedelta

import pandas as pd

import db

# ---- パラメータ(バックテストで要調整) ----
SHRINK_K = 30           # 縮小の擬似サンプル数(頭)。大きいほど長期側に寄る
BASELINE_YEARS = 3      # 長期ベースラインの対象期間
MIN_CELL_N = 300        # セグメントセルの最小頭数。未満なら一段粗い階層へ
SURFACES = ("芝", "ダート")

# 路盤改修等でデータをリセットする競馬場 {場: この日以降のみ使用}
RESET_DATES = {
    "京都": "20230422",  # 2020-2023改修工事、2023-04-22リニューアルオープン
}


def dist_category(distance):
    """距離カテゴリ: 短距離 / マイル〜中距離 / 長距離"""
    if distance is None:
        return None
    if distance <= 1400:
        return "短距離"
    if distance <= 2000:
        return "マイル〜中距離"
    return "長距離"


def cond2(track_condition):
    """馬場状態2区分: 良 / 道悪"""
    if track_condition is None:
        return None
    return "良" if track_condition == "良" else "道悪"


# ---------- データ読み込み ----------
BASE_QUERY = """
SELECT r.race_id, r.date, r.place, r.surface, r.distance, r.track_condition,
       r.kai, r.nichi, r.n_starters,
       h.umaban, h.finish, h.finish_raw, h.passing
FROM races r JOIN results h ON r.race_id = h.race_id
WHERE r.surface IN ('芝', 'ダート')
"""


def load_horses(conn, place=None, date_from=None, date_to=None, date_eq=None):
    q, params = BASE_QUERY, []
    if place:
        q += " AND r.place = ?"
        params.append(place)
    if date_eq:
        q += " AND r.date = ?"
        params.append(date_eq)
    if date_from:
        q += " AND r.date >= ?"
        params.append(date_from)
    if date_to:
        q += " AND r.date < ?"
        params.append(date_to)
    df = pd.read_sql_query(q, conn, params=params)
    return prepare(df)


def prepare(df):
    """出走馬のみに絞り、相対枠位置・正規化着順・脚質を付与する。"""
    if df.empty:
        return df
    # 取消・除外(=出走していない)を落とす
    starters = ~df["finish_raw"].fillna("").str.contains("取|除")
    df = df[starters].copy()

    # 相対枠位置: 出走馬の中での馬番順位 ÷ 出走頭数(欠番があっても歪まない)
    df["rel_pos"] = df.groupby("race_id")["umaban"].rank(method="first") / \
        df.groupby("race_id")["umaban"].transform("count")
    df["frame3"] = df["rel_pos"].map(
        lambda p: "内" if p <= 1 / 3 + 1e-9 else ("中" if p <= 2 / 3 + 1e-9 else "外"))

    # 正規化着順(完走馬のみ)。中止・失格は指標から除外
    df["n_run"] = df.groupby("race_id")["umaban"].transform("count")
    df["norm_rank"] = df["finish"] / df["n_run"]
    # レース期待値: 完走馬の norm_rank 平均 = (完走数+1)/2/出走頭数
    n_fin = df.groupby("race_id")["finish"].transform("count")
    df["race_exp"] = (n_fin + 1) / 2 / df["n_run"]
    df["delta"] = df["norm_rank"] - df["race_exp"]

    # 脚質: そのレースの最終コーナー通過順位から(前半=逃げ先行)
    def style(row):
        p = row["passing"]
        if not p or pd.isna(p):
            return None
        try:
            last = int(str(p).split("-")[-1])
        except ValueError:
            return None
        return "逃げ先行" if last <= row["n_run"] / 2 else "差し追込"
    df["style"] = df.apply(style, axis=1)

    df["dist_cat"] = df["distance"].map(dist_category)
    df["cond2"] = df["track_condition"].map(cond2)
    return df


# ---------- 集計コア ----------
def group_deltas(df, key, labels):
    """グループごとの delta 平均・標準誤差・頭数。finishが無い馬(中止等)は除外。"""
    sub = df.dropna(subset=["delta"])
    out = {}
    for label in labels:
        grp = sub[sub[key] == label]
        if len(grp):
            se = float(grp["delta"].std(ddof=1) / len(grp) ** 0.5) if len(grp) >= 2 else None
            out[label] = {"delta": float(grp["delta"].mean()),
                          "se": se, "n": int(len(grp))}
    return out


def baseline_for_races(base_df, day_races, key, labels):
    """当日の各レースのセグメントに対応する長期ベースラインを、階層フォールバック
    しながら取得し、当日のレース構成で加重平均する。
    階層: (dist_cat, cond2) → (dist_cat) → (全体)。place×surface は呼び出し側で絞り済み。
    戻り値: {label: {delta, n}}, 使用階層の内訳"""
    levels_used = {}
    acc = {label: {"wsum": 0.0, "w": 0} for label in labels}

    for _, race in day_races.iterrows():
        candidates = [
            ("距離×馬場", base_df[(base_df["dist_cat"] == race["dist_cat"]) &
                                  (base_df["cond2"] == race["cond2"])]),
            ("距離", base_df[base_df["dist_cat"] == race["dist_cat"]]),
            ("全体", base_df),
        ]
        chosen_name, chosen = "全体", base_df
        for name, cand in candidates:
            if len(cand.dropna(subset=["delta"])) >= MIN_CELL_N:
                chosen_name, chosen = name, cand
                break
        levels_used[chosen_name] = levels_used.get(chosen_name, 0) + 1

        gd = group_deltas(chosen, key, labels)
        w = int(race["n_run"])
        for label in labels:
            if label in gd:
                acc[label]["wsum"] += gd[label]["delta"] * w
                acc[label]["w"] += w

    baseline = {}
    for label in labels:
        if acc[label]["w"] > 0:
            baseline[label] = {"delta": acc[label]["wsum"] / acc[label]["w"]}
    return baseline, levels_used


def shrink(recent, baseline, k=SHRINK_K):
    """直近の推定値を長期ベースラインに向けて縮小(経験ベイズ的な部分プーリング)。
    adjusted = (n*recent + k*baseline) / (n + k)"""
    out = {}
    for label, r in recent.items():
        b = baseline.get(label)
        if b is None:
            out[label] = {**r, "baseline_delta": None, "adjusted_delta": r["delta"],
                          "deviation": None, "dev_se": None}
            continue
        n = r["n"]
        w = n / (n + k)
        adj = w * r["delta"] + (1 - w) * b["delta"]
        # deviation = 縮小後の「ベースラインとの乖離」。絶対値のadjusted_deltaは
        # 「前にいる馬・内の馬は常に着順が良い」という恒常的な相関を含むため、
        # トラックバイアスの表示にはこちらを使う。
        # dev_se: ベースラインを既知とみなした近似(deviation = w×(recent−base))
        dev_se = w * r["se"] if r.get("se") is not None else None
        out[label] = {**r, "baseline_delta": b["delta"], "adjusted_delta": adj,
                      "deviation": adj - b["delta"], "dev_se": dev_se}
    return out


def per_race_favor(df, key, a, b):
    """レース単位で aグループ平均 < bグループ平均(=a有利)だった数を数える。
    確度表示「12レース中8レースで内有利」用。"""
    favor = total = 0
    for _, grp in df.dropna(subset=["delta"]).groupby("race_id"):
        ma = grp[grp[key] == a]["delta"].mean()
        mb = grp[grp[key] == b]["delta"].mean()
        if pd.isna(ma) or pd.isna(mb):
            continue
        total += 1
        if ma < mb:
            favor += 1
    return favor, total


# ---------- 1開催日ぶんのレポート ----------
def latest_date_for_place(conn, place):
    row = conn.execute(
        "SELECT MAX(date) FROM races WHERE place = ? AND surface IN ('芝','ダート')",
        (place,)).fetchone()
    return row[0] if row else None


def build_bias_report(conn, place, target_date=None):
    """place の target_date(未指定なら最新)のバイアスレポート dict を返す。"""
    if target_date is None:
        target_date = latest_date_for_place(conn, place)
        if target_date is None:
            return None

    day = load_horses(conn, place=place, date_eq=target_date)
    if day.empty:
        return None

    # 長期ベースライン: target_date より前 BASELINE_YEARS 年、改修リセット考慮
    d = datetime.strptime(target_date, "%Y%m%d")
    date_from = (d - timedelta(days=365 * BASELINE_YEARS)).strftime("%Y%m%d")
    if place in RESET_DATES:
        date_from = max(date_from, RESET_DATES[place])
    base_all = load_horses(conn, place=place, date_from=date_from, date_to=target_date)

    KINDS = [
        ("frame3", ["内", "中", "外"], ("内", "外"), "frame_bias"),
        ("style", ["逃げ先行", "差し追込"], ("逃げ先行", "差し追込"), "style_bias"),
    ]

    def analyze_subset(sub, base, day_races):
        """1つのサブセット(全体 or 距離カテゴリ)の frame/style 推定を返す。"""
        out = {}
        for key, labels, favor_pair, name in KINDS:
            recent = group_deltas(sub, key, labels)
            if not base.empty:
                baseline, levels = baseline_for_races(base, day_races, key, labels)
            else:
                baseline, levels = {}, {}
            merged = shrink(recent, baseline)
            favor, total = per_race_favor(sub, key, *favor_pair)
            out[name] = {
                "groups": [{"group": g, **{k: (round(v, 4) if isinstance(v, float) else v)
                                           for k, v in merged[g].items()}}
                           for g in labels if g in merged],
                "confidence": {"favor_label": favor_pair[0], "favor_races": favor,
                               "total_races": total},
                "baseline_levels": levels,
            }
        return out

    surfaces = {}
    for surface in SURFACES:
        sub = day[day["surface"] == surface]
        if sub.empty:
            continue
        base = base_all[base_all["surface"] == surface] if not base_all.empty else base_all
        day_races = sub.drop_duplicates("race_id")[["race_id", "dist_cat", "cond2", "n_run"]]

        result = {"race_count": int(sub["race_id"].nunique()),
                  "n_horses": int(sub["delta"].notna().sum()),
                  "baseline_n": int(len(base.dropna(subset=["delta"]))) if not base.empty else 0}
        result.update(analyze_subset(sub, base, day_races))

        # 距離カテゴリ別の内訳。1日あたりのnが小さいため縮小が強く効く
        # (=ベースライン寄りの保守的な推定になる)。表示側でnを明示する。
        by_distance = {}
        for cat in ["短距離", "マイル〜中距離", "長距離"]:
            sub_cat = sub[sub["dist_cat"] == cat]
            if sub_cat.empty:
                continue
            dr_cat = day_races[day_races["dist_cat"] == cat]
            by_distance[cat] = {
                "race_count": int(sub_cat["race_id"].nunique()),
                **analyze_subset(sub_cat, base, dr_cat),
            }
        result["by_distance"] = by_distance
        surfaces[surface] = result

    # コース替わり注記(A/B/C柵は未取得のため開催替わりで代用)
    notes = []
    nichis = day["nichi"].unique()
    if 1 in nichis:
        notes.append("開催替わり初日: 柵移動等でバイアスがリセットされた可能性が高い")

    conds = sorted(day["track_condition"].dropna().unique().tolist())
    return {
        "place": place,
        "date": target_date,
        "track_conditions": conds,
        "surfaces": surfaces,
        "notes": notes,
        "params": {"shrink_k": SHRINK_K, "baseline_years": BASELINE_YEARS,
                   "min_cell_n": MIN_CELL_N,
                   "baseline_from": date_from},
    }


def export_to_site(raw_conn, site_conn, place, target_date=None):
    """バイアスレポートを計算して site.db の bias3_* テーブルへ書き込む。"""
    import site_db
    report = build_bias_report(raw_conn, place, target_date)
    if report is None:
        return None
    site_db.write_bias3(site_conn, report)
    return report


def main():
    ap = argparse.ArgumentParser(description="新バイアス集計(SQLiteベース)")
    ap.add_argument("place", help="競馬場名(東京など)")
    ap.add_argument("--date", default=None, help="YYYYMMDD(未指定なら最新)")
    ap.add_argument("--db", dest="db_path", default=None)
    ap.add_argument("--export", action="store_true",
                    help="site.db の bias3_* テーブルへ書き込む")
    args = ap.parse_args()

    conn = db.connect(args.db_path)
    if args.export:
        import site_db
        site_conn = site_db.connect()
        report = export_to_site(conn, site_conn, args.place, args.date)
        if report is None:
            print(f"{args.place} のデータがありません")
            return
        print(f"export: {report['date']}_{report['place']} → site.db bias3_*")
        return
    report = build_bias_report(conn, args.place, args.date)
    if report is None:
        print(f"{args.place} のデータがありません")
        return
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
