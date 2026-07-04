"""
特徴量研究 第5回: 日内更新の精緻化。

第1回(research_features.py)で x_intraday(当日それまでのレースからの
枠バイアス再推定)は train で5%有意(2ΔLL=6.5)だったが valid で再現しなかった。
残された仮説: 効果は特定の文脈に集中しており、全体に薄めると消える。

検証する変分(すべて当該レース発走前に計算可能な情報のみ):
  V1 全日     第1回の再現(K=30, MIN_N=15, 距離帯ALL)
  V2 急変日   V1 × 急変日フラグ(当日中に馬場状態が変化 or 前開催日から悪化/回復)
  V3 安定日   V1 × 非急変日(対照)
  V4 距離帯   日内集計を同一距離カテゴリ内に限定(MIN_N=8, 縮小強め)
  V5 開催後半 V1 × 開催日次nichi≥5(馬場摩耗が進む後半に限定)
  V6 縮小K    最良変分のKグリッド(10/30/60)

評価: backtest.py と同じ時系列3分割。trainで係数学習+尤度比検定、
valid/testはtrain係数固定でのΔLL(再現性)。validで再現した変分のみEVスキャン。

使い方:
  python scraper/research_intraday.py
"""

import numpy as np
import pandas as pd

import db
import site_db
from backtest import TRAIN, VALID, TEST, split, roi_at, build_dataset
from bias import dist_category

COND_RANK = {"良": 0, "稍重": 1, "重": 2, "不良": 3}
CHI2_1DF = 3.84


# ---------- 条件付きロジット(offset付き・入力行順のprobs) ----------
class CLogit:
    """score = log_m + X @ beta。probs()は入力行順で返す。"""

    def __init__(self, df, feature_cols):
        g = df["race_id"].to_numpy()
        self.order = np.argsort(g, kind="mergesort")
        gs = g[self.order]
        self.X = df[feature_cols].to_numpy(dtype=float)[self.order]
        self.offset = df["log_m"].to_numpy(dtype=float)[self.order]
        self.win = df["win"].to_numpy(dtype=bool)[self.order]
        self.starts = np.r_[0, np.flatnonzero(gs[1:] != gs[:-1]) + 1]
        self.seg_id = np.repeat(np.arange(len(self.starts)),
                                np.r_[self.starts[1:], len(gs)] - self.starts)
        self.n_races = len(self.starts)

    def _probs_sorted(self, beta):
        s = self.offset + self.X @ beta
        smax = np.maximum.reduceat(s, self.starts)
        e = np.exp(s - smax[self.seg_id])
        tot = np.add.reduceat(e, self.starts)
        return e / tot[self.seg_id]

    def probs(self, beta):
        p = self._probs_sorted(beta)
        out = np.empty_like(p)
        out[self.order] = p
        return out

    def loglik(self, beta):
        p = self._probs_sorted(beta)
        return float(np.log(np.clip(p[self.win], 1e-300, None)).sum())

    def fit(self, n_iter=500):
        beta = np.zeros(self.X.shape[1])
        lr = 50.0 / max(1, self.n_races)
        ll = self.loglik(beta)
        for _ in range(n_iter):
            p = self._probs_sorted(beta)
            grad = self.X.T @ (self.win.astype(float) - p)
            step = lr
            for _ in range(40):
                cand = beta + step * grad
                ll_new = self.loglik(cand)
                if ll_new > ll:
                    beta, ll = cand, ll_new
                    break
                step /= 2
            else:
                break
        return beta, ll


# ---------- 文脈フラグ ----------
def add_context(df, raw_conn):
    """track_condition・開催日次を結合し、急変日フラグを作る。
    急変日 = 当日中に同一場×馬面の馬場状態が変化 or 前開催日から状態が変わった。"""
    races = pd.read_sql_query(
        "SELECT race_id, track_condition, nichi FROM races", raw_conn)
    df = df.merge(races, on="race_id", how="left")
    df["cond_rank"] = df["track_condition"].map(COND_RANK)

    # 当日内の変化(同一日×場×馬面で複数の馬場状態)
    day_grp = df.groupby(["date", "place", "surface"])["cond_rank"]
    intraday_shift = (day_grp.transform("max") != day_grp.transform("min"))

    # 前開催日との差(場×馬面の代表値=中央値で比較)
    day_cond = (df.groupby(["date", "place", "surface"])["cond_rank"]
                  .median().rename("day_cond").reset_index())
    prev_cond = day_cond.rename(columns={"date": "prev_date",
                                         "day_cond": "prev_cond"})
    df = df.merge(day_cond, on=["date", "place", "surface"], how="left")
    df = df.merge(prev_cond, on=["prev_date", "place", "surface"], how="left")
    vs_prev = (df["day_cond"].notna() & df["prev_cond"].notna()
               & (df["day_cond"] != df["prev_cond"]))

    df["is_change"] = (intraday_shift | vs_prev).astype(float)
    df["is_late_meet"] = (df["nichi"] >= 5).astype(float)
    cover = df.drop_duplicates(["date", "place", "surface"])
    print(f"  急変日(日×場×馬面): {cover['is_change'].mean()*100:.1f}%"
          f" / 開催後半: {cover['is_late_meet'].mean()*100:.1f}%")
    return df


# ---------- 日内更新特徴量(パラメータ化) ----------
def load_baselines(site_conn):
    """{(date, place, surface, dist_cat, grp): baseline_delta}"""
    out = {}
    for d, p, s, dc, g, b in site_conn.execute(
        "SELECT date, place, surface, dist_cat, grp, baseline_delta"
        " FROM bias3_stats WHERE kind = 'frame3'"):
        out[(d, p, s, dc, g)] = b if b is not None else 0.0
    return out


def compute_intraday(df, base_map, k=30, min_n=15, dist_match=False):
    """当日それまでのレースから枠グループ成績を集計し、縮小した
    「前開催日ベースラインからの日内乖離」を返す(符号: 正=有利化)。
    dist_match=True なら同一距離カテゴリ内でのみ集計・比較する。"""
    df = df.sort_values(["date", "place", "surface", "race_id"],
                        kind="mergesort")
    x = np.zeros(len(df))
    pos = {idx: i for i, idx in enumerate(df.index)}

    for (date, place, sf), g in df.groupby(["date", "place", "surface"],
                                           sort=False):
        sums, cnts = {}, {}
        prev_date = g["prev_date"].iloc[0]
        for rid in g["race_id"].unique():
            rows = g[g["race_id"] == rid]
            dc = rows["dist_cat"].iloc[0] if dist_match else "ALL"
            # 先に「それまでの集計」から特徴量を計算(=リークなし)
            for idx, fr in zip(rows.index, rows["frame3"]):
                key = (fr, dc)
                c = cnts.get(key, 0)
                if c >= min_n:
                    mean_now = sums[key] / c
                    w = c / (c + k)
                    base = base_map.get((prev_date, place, sf, dc, fr))
                    if base is None:  # 距離帯ベースライン未整備ならALLへ
                        base = base_map.get(
                            (prev_date, place, sf, "ALL", fr), 0.0)
                    x[pos[idx]] = -(w * (mean_now - base))
            # その後に当該レースを集計へ追加
            for fr, dl in zip(rows["frame3"], rows["delta"]):
                if not np.isnan(dl):
                    key = (fr, dc)
                    sums[key] = sums.get(key, 0.0) + dl
                    cnts[key] = cnts.get(key, 0) + 1
    out = pd.Series(0.0, index=df.index)
    out.loc[df.index] = x[[pos[i] for i in df.index]]
    return out.sort_index()


# ---------- 評価 ----------
def evaluate(name, col, tr, va, te):
    clg_tr = CLogit(tr, [col])
    beta, ll = clg_tr.fit()
    lr_tr = 2 * (ll - clg_tr.loglik(np.zeros(1)))
    lr_va = 2 * (CLogit(va, [col]).loglik(beta)
                 - CLogit(va, [col]).loglik(np.zeros(1)))
    lr_te = 2 * (CLogit(te, [col]).loglik(beta)
                 - CLogit(te, [col]).loglik(np.zeros(1)))
    cover = float((tr[col] != 0).mean())
    sig = "★5%有意" if lr_tr > CHI2_1DF else "(n.s.)"
    rep = "再現" if (lr_tr > CHI2_1DF and lr_va > 0 and lr_te > 0) else "—"
    print(f"  {name:<10} β={beta[0]:+7.2f} 被覆{cover*100:4.1f}%"
          f"  train 2ΔLL={lr_tr:6.2f}{sig}"
          f"  valid ΔLL寄与={lr_va:+6.2f} test={lr_te:+6.2f} {rep}")
    return beta[0], lr_tr, lr_va, lr_te


def main():
    raw = db.connect()
    site = site_db.connect()

    print("データセット構築中...", flush=True)
    data = build_dataset(raw, site)

    # 正規化着順delta(日内集計の材料)と距離カテゴリ
    n = data.groupby("race_id")["umaban"].transform("count")
    n_fin = data.groupby("race_id")["finish"].transform("count")
    data["delta"] = data["finish"] / n - (n_fin + 1) / 2 / n
    dist = pd.read_sql_query("SELECT race_id, distance FROM races", raw)
    data = data.merge(dist, on="race_id", how="left")
    data["dist_cat"] = data["distance"].map(dist_category)

    data = add_context(data, raw)
    base_map = load_baselines(site)

    print("日内特徴量を計算中...", flush=True)
    data["x_all"] = compute_intraday(data, base_map, k=30, min_n=15)
    data["x_dist"] = compute_intraday(data, base_map, k=30, min_n=8,
                                      dist_match=True)
    data["x_chg"] = data["x_all"] * data["is_change"]
    data["x_stable"] = data["x_all"] * (1.0 - data["is_change"])
    data["x_late"] = data["x_all"] * data["is_late_meet"]

    tr, va, te = [split(data, p).copy() for p in (TRAIN, VALID, TEST)]
    print(f"  train {tr['race_id'].nunique()}R / valid {va['race_id'].nunique()}R"
          f" / test {te['race_id'].nunique()}R")

    print("\n=== 変分比較(単変量, train学習→valid/testは係数固定) ===")
    results = {}
    for name, col in [("V1 全日", "x_all"), ("V2 急変日", "x_chg"),
                      ("V3 安定日", "x_stable"), ("V4 距離帯", "x_dist"),
                      ("V5 開催後半", "x_late")]:
        results[col] = evaluate(name, col, tr, va, te)

    # V6: 最良変分(train有意かつvalid正)のKグリッド
    cands = [c for c, (_, lt, lv, _) in results.items()
             if lt > CHI2_1DF and lv > 0]
    if cands:
        best = max(cands, key=lambda c: results[c][2])
        print(f"\n=== Kグリッド(ベース変分: {best}) ===")
        is_dist = best == "x_dist"
        mask_col = {"x_chg": "is_change", "x_stable": None,
                    "x_late": "is_late_meet"}.get(best)
        for k in (10, 30, 60):
            col = f"xk{k}"
            base = compute_intraday(data, base_map, k=k,
                                    min_n=8 if is_dist else 15,
                                    dist_match=is_dist)
            data[col] = base if mask_col is None and best != "x_chg" \
                else base * data[mask_col] if mask_col else base
            trk, vak, tek = [split(data, p).copy()
                             for p in (TRAIN, VALID, TEST)]
            evaluate(f"K={k}", col, trk, vak, tek)

        # EVスキャン(最良変分のtrain係数固定)
        print(f"\n=== EVスキャン({best}, train係数固定) ===")
        beta = results[best][0]
        for name, d in [("valid", va), ("test", te)]:
            p = CLogit(d, [best]).probs(np.array([beta]))
            line = f"  {name:<5}"
            for tau in (1.00, 1.05, 1.10):
                roi, nbet = roi_at(d, p, tau)
                line += f"  τ{tau:.2f}:{'—' if not nbet else f'{roi:.0f}%({nbet})'}"
            print(line)
    else:
        print("\n  train有意かつvalid正の変分なし → Kグリッド・EVスキャン省略")

    print("\n⚠️ 確定オッズでの検証(実運用の前日オッズでは下振れしうる)")


if __name__ == "__main__":
    main()
