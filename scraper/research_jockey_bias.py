"""
騎手×トラックバイアス適応力の研究(コンテンツ研究・第C1回)。

問い: 「馬場を読める騎手」は実在するか。
  当日それまでのレースで観測できた脚質バイアス(前残り/差し優勢)に対して、
  騎手は馬の普段のポジションより前後に調整しているか(適応力)。
  そして、その調整は着順を実際に改善しているか(効果)。

リーク防止:
  - 馬場の傾き tilt は「同じ日・場・コース種別のそれより前のレース」だけから計算
    (騎手がパドックやモニターで観察できた情報の代理)
  - 馬の基準ポジションは過去走(当日を含まない)のみから計算

定義:
  early_pos  = 最初のコーナー通過順位 / 出走頭数 (0寄り=前)
  horse_base = 同馬の過去2〜10走の early_pos 平均(2023年〜をウォームアップに使用)
  pos_delta  = horse_base - early_pos_today (正 = 普段より前に出した)
  race_tilt  = レース内の (逃げ先行の平均delta) - (差し追込の平均delta)
               (負 = 前残り。delta は正規化着順の期待値からの乖離、負=好走)
  tilt       = 当日同場同コースの先行レース(race_no昇順で当該より前、3R以上)の
               race_tilt 平均から、場×コース種別の長期平均を引いた乖離。
               逃げ先行は構造的に常に有利(race_tiltの平均は負)なので、
               生の値ではなく「いつもよりどれだけ前残りか」を使う
  適応スロープ = 騎手ごとの回帰係数 slope(pos_delta ~ -tilt)
               (正 = 前残りの日ほど前に出す)

使い方:
  python scraper/research_jockey_bias.py            # 2024-01〜 全体
  python scraper/research_jockey_bias.py --min-rides 300
"""

import argparse
import re

import numpy as np
import pandas as pd

import db

ANALYSIS_FROM = "20240101"   # 2023年は馬の基準ポジションのウォームアップのみ
MIN_EARLIER_RACES = 3        # tilt計算に必要な同日先行レース数
MIN_HORSE_RUNS = 2           # 馬の基準ポジションに必要な過去走数
TILT_THRESHOLD = 0.02        # 前残り日/差し日の判定閾値
POS_THRESHOLD = 0.05         # 「普段より前/後ろ」の判定閾値

QUERY = """
SELECT r.race_id, r.date, r.place, r.surface, r.distance, r.race_no,
       h.umaban, h.horse, h.jockey, h.finish, h.finish_raw, h.passing
FROM races r JOIN results h ON r.race_id = h.race_id
WHERE r.surface IN ('芝', 'ダート')
"""

MARK_RE = re.compile(r"^[▲△☆◇★◆]")


def load(conn):
    df = pd.read_sql_query(QUERY, conn)
    starters = ~df["finish_raw"].fillna("").str.contains("取|除")
    df = df[starters].copy()

    # 騎手名の正規化(減量記号を落とす)
    df["jockey"] = df["jockey"].fillna("").map(lambda j: MARK_RE.sub("", j).strip())
    df = df[df["jockey"] != ""]

    # 最初のコーナー通過順位(通過が無い行=realtime未確定などは除外)
    def first_corner(p):
        if not p or pd.isna(p):
            return None
        try:
            return int(str(p).split("-")[0])
        except ValueError:
            return None
    df["fc"] = df["passing"].map(first_corner)
    df = df.dropna(subset=["fc"])

    df["n_run"] = df.groupby("race_id")["umaban"].transform("count")
    df["early_pos"] = df["fc"] / df["n_run"]

    # 正規化着順の期待値乖離(bias.py と同じ定義)
    df["norm_rank"] = df["finish"] / df["n_run"]
    n_fin = df.groupby("race_id")["finish"].transform("count")
    df["race_exp"] = (n_fin + 1) / 2 / df["n_run"]
    df["delta"] = df["norm_rank"] - df["race_exp"]

    # 脚質(最終コーナー)
    def last_corner(p):
        try:
            return int(str(p).split("-")[-1])
        except (ValueError, TypeError):
            return None
    df["lc"] = df["passing"].map(last_corner)
    df["front"] = df["lc"] <= df["n_run"] / 2
    return df


def add_horse_base(df):
    """同馬の過去走(当日除く)の early_pos 平均。直近10走・最低2走。"""
    df = df.sort_values(["horse", "date", "race_id"]).copy()
    g = df.groupby("horse")["early_pos"]
    df["horse_base"] = (
        g.transform(lambda s: s.shift(1).rolling(10, min_periods=MIN_HORSE_RUNS).mean()))
    return df


def add_tilt(df):
    """レースごとの race_tilt → 当日同場同コースの先行レース平均 tilt。"""
    ok = df.dropna(subset=["delta"])
    front_d = ok[ok["front"]].groupby("race_id")["delta"].mean()
    back_d = ok[~ok["front"]].groupby("race_id")["delta"].mean()
    race_tilt = (front_d - back_d).rename("race_tilt")

    races = (df[["race_id", "date", "place", "surface", "race_no"]]
             .drop_duplicates().merge(race_tilt, on="race_id", how="left")
             .sort_values(["date", "place", "surface", "race_no"]))

    def expanding_prior_mean(s):
        return s.shift(1).expanding(min_periods=MIN_EARLIER_RACES).mean()

    races["tilt_raw"] = (races.groupby(["date", "place", "surface"])["race_tilt"]
                         .transform(expanding_prior_mean))
    # 逃げ先行の恒常的有利(場×コース種別で異なる)を除去し、
    # 「いつもよりどれだけ前残り/差しか」の乖離にする
    races["tilt"] = (races["tilt_raw"]
                     - races.groupby(["place", "surface"])["race_tilt"]
                            .transform("mean"))
    return df.merge(races[["race_id", "tilt"]], on="race_id", how="left")


def main():
    ap = argparse.ArgumentParser(description="騎手×バイアス適応力")
    ap.add_argument("--min-rides", type=int, default=200,
                    help="ランキング掲載に必要な有効騎乗数")
    ap.add_argument("--db", dest="db_path", default=None)
    args = ap.parse_args()

    conn = db.connect(args.db_path)
    df = load(conn)
    df = add_horse_base(df)
    df = add_tilt(df)

    a = df[(df["date"] >= ANALYSIS_FROM)
           & df["horse_base"].notna() & df["tilt"].notna()].copy()
    a["pos_delta"] = a["horse_base"] - a["early_pos"]
    a["neg_tilt"] = -a["tilt"]          # 正 = 前残りの日
    print(f"有効サンプル: {len(a):,}騎乗 "
          f"({a['date'].min()}〜{a['date'].max()}、騎手{a['jockey'].nunique()}人)\n")

    # ---------- 1. 全体: そもそも騎手集団は馬場に適応しているか ----------
    x, y = a["neg_tilt"], a["pos_delta"]
    slope_all = np.cov(x, y)[0, 1] / np.var(x)
    r_all = np.corrcoef(x, y)[0, 1]
    se = (np.std(y) / (np.std(x) * np.sqrt(len(a))))
    print("[1] 全体の適応: slope(pos_delta ~ 前残り度) = "
          f"{slope_all:+.3f} (r={r_all:+.4f}, SE≈{se:.3f}, n={len(a):,})")
    print("    正なら「前残りの日ほど、馬の普段より前に出している」\n")

    # ---------- 2. 適応は着順を改善するか ----------
    # 「前に出す」は構造的に常に得(逃げ先行有利)なので、日タイプ×ポジション
    # 調整の2×3で見る。適応に価値があるなら、同じ「前に出す」でも
    # 前残り日の方が中立日・差し日より効いているはず(交互作用)
    def day_type(t):
        if t < -TILT_THRESHOLD:
            return "前残り日"
        if t > TILT_THRESHOLD:
            return "差し日"
        return "中立日"
    a["day_type"] = a["tilt"].map(day_type)
    a["pos_move"] = np.select(
        [a["pos_delta"] > POS_THRESHOLD, a["pos_delta"] < -POS_THRESHOLD],
        ["前に出した", "後ろに控えた"], default="普段どおり")
    print("[2] 日タイプ×ポジション調整のdelta平均(負=好走)。"
          "行間の差ではなく列間の差(交互作用)が「馬場読みの価値」:")
    pivot = a.pivot_table(index="pos_move", columns="day_type",
                          values="delta", aggfunc=["mean", "size"])
    print(pivot.round(4).to_string(), "\n")

    # ---------- 3. 騎手別ランキング ----------
    rows = []
    for j, sub in a.groupby("jockey"):
        if len(sub) < args.min_rides or sub["neg_tilt"].std() == 0:
            continue
        s = np.cov(sub["neg_tilt"], sub["pos_delta"])[0, 1] / np.var(sub["neg_tilt"])
        se_j = sub["pos_delta"].std() / (sub["neg_tilt"].std() * np.sqrt(len(sub)))
        rows.append({"jockey": j, "n": len(sub), "slope": s, "se": se_j,
                     "t": s / se_j if se_j > 0 else 0,
                     "mean_delta": sub["delta"].mean()})
    rk = pd.DataFrame(rows).sort_values("slope", ascending=False)

    pd.set_option("display.unicode.east_asian_width", True)
    print(f"[3] 馬場を読む騎手ランキング(有効騎乗{args.min_rides}以上、slope降順)")
    print("    slope正=前残りの日ほど前に出す。|t|>2で個人として有意\n")
    show = rk.assign(slope=rk["slope"].round(3), se=rk["se"].round(3),
                     t=rk["t"].round(1), mean_delta=rk["mean_delta"].round(4))
    print("--- 適応上位15 ---")
    print(show.head(15).to_string(index=False))
    print("\n--- 適応下位15 ---")
    print(show.tail(15).to_string(index=False))

    # ---------- 4. 上位/下位グループの成績差と多重検定の警告 ----------
    if len(rk) >= 20:
        top = set(rk.head(10)["jockey"])
        bot = set(rk.tail(10)["jockey"])
        tilted = a[a["tilt"].abs() > TILT_THRESHOLD]
        td = tilted[tilted["jockey"].isin(top)]["delta"].mean()
        bd = tilted[tilted["jockey"].isin(bot)]["delta"].mean()
        print(f"\n[4] バイアスが出た日の成績(delta平均、負=好走): "
              f"適応上位10人 {td:+.4f} / 下位10人 {bd:+.4f}")
        n_sig = int((rk["t"].abs() > 2).sum())
        print(f"    |t|>2 の騎手: {n_sig}/{len(rk)}人 "
              f"(偶然でも約{len(rk) * 0.05:.0f}人は出る — 個人名の解釈は慎重に)")


if __name__ == "__main__":
    main()
