"""
特徴量研究: 市場が織り込んでいない情報を探す。

候補(現有データで計算可能なもの):
  x_frame    前開催日の枠バイアス乖離(v1の既存特徴量)
  x_style    前開催日の脚質バイアス乖離 × 当該馬の予想脚質(自身の前走通過順から)
  x_intraday 当日それまでのレースから再推定した枠バイアス乖離(日内更新)

方法:
  backtest.py と同じ時系列3分割。ネストしたモデル M0(市場のみ)→M1(+frame)
  →M2(+style)→M3(+intraday) を多変量条件付きロジット(勾配上昇)で学習し、
  尤度比検定・test対数損失・EV閾値スキャンで評価する。
  リーク防止: すべての特徴量は当該レース発走前に計算可能な情報のみ。

使い方:
  python scraper/research_features.py
"""

import math

import numpy as np
import pandas as pd

import db
import site_db
from backtest import (TRAIN, VALID, TEST, build_dataset, split, roi_at)

INTRADAY_K = 30       # 日内推定の縮小擬似サンプル(頭)
INTRADAY_MIN_N = 15   # 日内グループ最低頭数(未満は特徴量0)
CHI2_5PCT = {1: 3.84, 2: 5.99, 3: 7.81}


# ---------- 追加特徴量 ----------
def add_style_feature(df, raw_conn, site_conn):
    """x_style: 前開催日の脚質バイアス乖離を、当該馬の予想脚質に適用。
    予想脚質 = その馬の前走(keiba.db内)の最終コーナー位置が頭数の前半なら
    逃げ先行。前走が無い/通過順が無い馬は0(情報なし)。"""
    # 全馬の全出走履歴(通過順あり)から脚質ラベルを作る
    hist = pd.read_sql_query("""
        SELECT h.horse, r.date, h.passing, r.n_starters
        FROM results h JOIN races r ON r.race_id = h.race_id
        WHERE r.surface IN ('芝','ダート') AND h.horse IS NOT NULL
    """, raw_conn)

    def style_of(passing, n):
        if not passing or not isinstance(passing, str):
            return None
        try:
            last = int(passing.split("-")[-1])
        except ValueError:
            return None
        return "逃げ先行" if last <= (n or 14) / 2 else "差し追込"

    hist["style"] = [style_of(p, n) for p, n in zip(hist["passing"], hist["n_starters"])]
    hist = hist.dropna(subset=["style"]).sort_values(["horse", "date"])

    # 各馬・各日付時点での「直近の前走脚質」を merge_asof で引く
    df = df.sort_values("date").reset_index(drop=True)
    horses = pd.read_sql_query("""
        SELECT h.race_id, h.umaban, h.horse
        FROM results h JOIN races r ON r.race_id = h.race_id
        WHERE r.surface IN ('芝','ダート')
    """, raw_conn)
    df = df.merge(horses, on=["race_id", "umaban"], how="left")

    hist_idx = {}
    for horse, g in hist.groupby("horse"):
        hist_idx[horse] = (g["date"].to_numpy(), g["style"].to_numpy())

    prev_styles = []
    for horse, date in zip(df["horse"], df["date"]):
        rec = hist_idx.get(horse)
        if rec is None:
            prev_styles.append(None)
            continue
        dates, styles = rec
        i = np.searchsorted(dates, date) - 1  # date より前の最後の走
        prev_styles.append(styles[i] if i >= 0 else None)
    df["prev_style"] = prev_styles

    # 前開催日の脚質バイアス乖離(site.db bias3_stats kind='style', ALL)
    dev_map = {}
    for d, p, sf, g, dev in site_conn.execute(
        "SELECT date, place, surface, grp, deviation FROM bias3_stats"
        " WHERE dist_cat = 'ALL' AND kind = 'style'"):
        dev_map[(d, p, sf, g)] = dev or 0.0

    df["x_style"] = [
        -(dev_map.get((pd_, pl, sf, st), 0.0)) if st else 0.0
        for pd_, pl, sf, st in zip(df["prev_date"], df["place"],
                                   df["surface"], df["prev_style"])]
    cover = float((df["prev_style"].notna()).mean())
    print(f"  x_style: 前走脚質の被覆率 {cover*100:.1f}%")
    return df


def add_intraday_feature(df, site_conn):
    """x_intraday: 当日それまでの同コース種別レースから枠グループの成績を
    集計し、縮小推定した「日内の乖離」。前開催日のベースライン推定
    (bias3_stats.baseline_delta)との差分を取る。"""
    # 正規化着順delta(オッズあり出走馬ベースの近似)
    n = df.groupby("race_id")["umaban"].transform("count")
    n_fin = df.groupby("race_id")["finish"].transform("count")
    df["delta"] = df["finish"] / n - (n_fin + 1) / 2 / n

    base_map = {}
    for d, p, sf, g, b in site_conn.execute(
        "SELECT date, place, surface, grp, baseline_delta FROM bias3_stats"
        " WHERE dist_cat = 'ALL' AND kind = 'frame3'"):
        base_map[(d, p, sf, g)] = b if b is not None else 0.0

    df = df.sort_values(["date", "place", "surface", "race_id"]).reset_index(drop=True)
    x = np.zeros(len(df))
    for (date, place, sf), g in df.groupby(["date", "place", "surface"], sort=False):
        race_ids = g["race_id"].unique()  # race_id末尾2桁=R順にソート済み
        sums = {"内": 0.0, "中": 0.0, "外": 0.0}
        cnts = {"内": 0, "中": 0, "外": 0}
        for rid in race_ids:
            rows = g[g["race_id"] == rid]
            # 先に「それまでの集計」から特徴量を計算(=リークなし)
            for idx, fr in zip(rows.index, rows["frame3"]):
                if cnts[fr] >= INTRADAY_MIN_N:
                    mean_now = sums[fr] / cnts[fr]
                    w = cnts[fr] / (cnts[fr] + INTRADAY_K)
                    base = base_map.get((rows["prev_date"].iloc[0], place, sf, fr), 0.0)
                    x[idx] = -(w * (mean_now - base))
            # その後に当該レースを集計へ追加
            for fr, dl in zip(rows["frame3"], rows["delta"]):
                if not np.isnan(dl):
                    sums[fr] += dl
                    cnts[fr] += 1
    df["x_intraday"] = x
    cover = float((df["x_intraday"] != 0).mean())
    print(f"  x_intraday: 特徴量が乗る行の割合 {cover*100:.1f}%(序盤レースは0)")
    return df


# ---------- 多変量条件付きロジット ----------
class CLogit:
    """score = log_m + X @ beta の条件付きロジット(レース内softmax)。"""

    def __init__(self, df, feature_cols):
        d = df.sort_values("race_id").reset_index(drop=True)
        self.X = d[feature_cols].to_numpy(dtype=float)
        self.log_m = d["log_m"].to_numpy(dtype=float)
        self.win = d["win"].to_numpy(dtype=bool)
        g = d["race_id"].to_numpy()
        self.starts = np.r_[0, np.flatnonzero(g[1:] != g[:-1]) + 1]
        self.ends = np.r_[self.starts[1:], len(g)]
        self.seg_id = np.repeat(np.arange(len(self.starts)),
                                self.ends - self.starts)
        self.n_races = len(self.starts)

    def _probs(self, beta):
        s = self.log_m + self.X @ beta
        smax = np.maximum.reduceat(s, self.starts)
        e = np.exp(s - smax[self.seg_id])
        tot = np.add.reduceat(e, self.starts)
        return e / tot[self.seg_id]

    def loglik(self, beta):
        p = self._probs(beta)
        return float(np.log(np.clip(p[self.win], 1e-300, None)).sum())

    def fit(self, n_iter=500, lr=None):
        """勾配上昇。凹関数なので単純なステップ半減で十分収束する。"""
        k = self.X.shape[1]
        beta = np.zeros(k)
        lr = lr or 1.0 / max(1, self.n_races) * 50
        ll = self.loglik(beta)
        for _ in range(n_iter):
            p = self._probs(beta)
            resid = self.win.astype(float) - p
            grad = self.X.T @ resid
            step = lr
            for _ in range(30):
                cand = beta + step * grad
                ll_new = self.loglik(cand)
                if ll_new > ll:
                    beta, ll = cand, ll_new
                    break
                step /= 2
            else:
                break  # 改善不能=収束
        return beta, ll


def logloss(clg, beta):
    p = clg._probs(beta)
    return float(-np.log(np.clip(p[clg.win], 1e-12, None)).mean())


# ---------- メイン ----------
def main():
    raw = db.connect()
    site = site_db.connect()

    print("データセット構築中...", flush=True)
    data = build_dataset(raw, site)
    print("特徴量追加中...", flush=True)
    data = add_style_feature(data, raw, site)
    data = add_intraday_feature(data, site)

    tr, va, te = split(data, TRAIN), split(data, VALID), split(data, TEST)
    print(f"  train {tr['race_id'].nunique()}R / valid {va['race_id'].nunique()}R"
          f" / test {te['race_id'].nunique()}R")

    FEATURE_SETS = [
        ("M0: 市場のみ", []),
        ("M1: +枠バイアス", ["x"]),
        ("M2: +脚質バイアス", ["x", "x_style"]),
        ("M3: +日内更新", ["x", "x_style", "x_intraday"]),
    ]

    print("\n=== ネストモデル比較(train学習 → 尤度比検定) ===")
    results = []
    prev_ll = None
    for name, cols in FEATURE_SETS:
        if cols:
            clg = CLogit(tr, cols)
            beta, ll = clg.fit()
        else:
            clg = CLogit(tr, ["x"])  # dummy X
            beta, ll = np.zeros(1), clg.loglik(np.zeros(1))
        lr_stat = None if prev_ll is None else 2 * (ll - prev_ll)
        sig = ""
        if lr_stat is not None:
            sig = " ★5%有意" if lr_stat > CHI2_5PCT[1] else " (n.s.)"
        beta_str = ", ".join(f"{c}={b:+.2f}" for c, b in zip(cols, beta)) or "—"
        print(f"  {name:<16} logLik={ll:9.1f}"
              f"  2ΔLL={'—' if lr_stat is None else f'{lr_stat:6.2f}'}{sig}  [{beta_str}]")
        results.append((name, cols, beta, ll))
        prev_ll = ll

    # 最良モデル(有意に改善した最後のモデル)でtest評価
    print("\n=== test評価(全モデル) ===")
    for name, cols, beta, _ in results:
        if not cols:
            clg_te = CLogit(te, ["x"])
            b = np.zeros(1)
        else:
            clg_te = CLogit(te, cols)
            b = beta
        ll_te = logloss(clg_te, b)

        # EVスキャン
        d = te.sort_values("race_id").reset_index(drop=True)
        p = clg_te._probs(b)
        best = ""
        for tau in (1.05, 1.10, 1.20):
            roi, nbet = roi_at(d, p, tau)
            if nbet:
                best += f" τ{tau}:ROI={roi:.0f}%({nbet}bet)"
        print(f"  {name:<16} test対数損失={ll_te:.4f}{best or '  ベットなし'}")


if __name__ == "__main__":
    main()
