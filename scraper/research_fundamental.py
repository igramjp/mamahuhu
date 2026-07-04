"""
特徴量研究 第2回: Benter型2段階モデル。

第1段階(ファンダメンタルモデル): 市場情報を使わず、王道因子のみの
条件付きロジットで勝率 f を推定する。
第2段階(合成): p ∝ exp(α·ln f + β·ln π) で市場確率 π と合成し、
α・β を train で推定する。市場のみのモデルは (α,β)=(0,1) でネスト。

因子(すべて当該レース発走前に計算可能な情報のみ / 2023年は履歴ウォームアップ):
  f_form5   直近5走の正規化着順(良いほど正)
  f_last    前走の正規化着順
  f_bm_h    馬のbeat-market履歴: (人気順位−着順)/頭数 の累積平均
  f_l3f     直近5走の上がり3F順位率(速いほど正)
  f_days    休養日数 log(days/30)、初出走は0
  f_nohist  期間内に前走なしフラグ
  f_nstart  期間内キャリア数 log1p
  f_dist    同距離帯での過去平均成績
  f_bw      馬体重増減(当日発表値)
  f_wtc     斤量の前走からの増減
  j_bm      騎手のbeat-market残差(勝利−市場確率)の縮小推定・前日まで
  j_switch  乗り替わりフラグ

評価: backtest.py と同じ時系列3分割。対数損失(市場/ファンダ単体/合成)、
尤度比検定、EV閾値スキャン、因子別drop-one検定。

使い方:
  python scraper/research_fundamental.py
"""

import math

import numpy as np
import pandas as pd

import db
from backtest import TRAIN, VALID, TEST, split, roi_at

MIN_HORSES = 5
K_JOCKEY = 200.0     # 騎手beat-market縮小の擬似騎乗数
HIST_WIN = 5         # 直近フォームの窓
CHI2_5PCT = {1: 3.84, 2: 5.99}

FEATS = ["f_form5", "f_last", "f_bm_h", "f_l3f", "f_days", "f_nohist",
         "f_nstart", "f_dist", "f_bw", "f_wtc", "j_bm", "j_switch"]


# ---------- 条件付きロジット(入力行順を保持する版) ----------
class CLogit:
    """score = offset + X @ beta のレース内softmax。probs()は入力行順で返す。"""

    def __init__(self, df, feature_cols):
        g = df["race_id"].to_numpy()
        self.order = np.argsort(g, kind="mergesort")  # 安定ソート
        gs = g[self.order]
        self.X = df[feature_cols].to_numpy(dtype=float)[self.order]
        self.offset = np.zeros(len(df))
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
        """入力DataFrameの行順に並んだ確率。"""
        p = self._probs_sorted(beta)
        out = np.empty_like(p)
        out[self.order] = p
        return out

    def loglik(self, beta):
        p = self._probs_sorted(beta)
        return float(np.log(np.clip(p[self.win], 1e-300, None)).sum())

    def fit(self, n_iter=800):
        k = self.X.shape[1]
        beta = np.zeros(k)
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
                break  # 改善不能=収束
        return beta, ll

    def logloss(self, beta):
        p = self._probs_sorted(beta)
        return float(-np.log(np.clip(p[self.win], 1e-12, None)).mean())


# ---------- データ+特徴量 ----------
def load_runs(conn):
    df = pd.read_sql_query("""
        SELECT r.race_id, r.date, r.place, r.surface, r.distance,
               h.umaban, h.finish, h.win_odds, h.popularity, h.horse,
               h.weight_carried, h.jockey, h.time, h.passing, h.last3f,
               h.horse_weight
        FROM races r JOIN results h ON r.race_id = h.race_id
        WHERE r.surface IN ('芝','ダート') AND h.horse IS NOT NULL
        ORDER BY r.date, r.race_id, h.umaban
    """, conn)
    df["dt"] = pd.to_datetime(df["date"], format="%Y%m%d")

    # 正規化着順delta(research_features.pyと同定義)と派生量
    n = df.groupby("race_id")["umaban"].transform("count")
    n_fin = df.groupby("race_id")["finish"].transform("count")
    df["good"] = -(df["finish"] / n - (n_fin + 1) / 2 / n)   # 正=好走
    df["bm_rank"] = (df["popularity"] - df["finish"]) / n     # 正=人気より好走
    l3f_rank = df.groupby("race_id")["last3f"].rank(method="average")
    df["l3f_good"] = 0.5 - l3f_rank / n                       # 正=上がり上位

    df["dist_cat"] = np.where(df["distance"] <= 1400, "短",
                              np.where(df["distance"] <= 2000, "中", "長"))
    # 馬体重増減 "512(+4)" → +4
    bw = df["horse_weight"].astype(str).str.extract(r"\(([+-]?\d+)\)")[0]
    df["bw_chg"] = pd.to_numeric(bw, errors="coerce").clip(-20, 20)
    return df


def add_horse_features(df):
    df = df.sort_values(["horse", "dt", "race_id"], kind="mergesort").reset_index(drop=True)
    g = df.groupby("horse", sort=False)

    prior = g.cumcount()
    df["f_nstart"] = np.log1p(prior)
    df["f_nohist"] = (prior == 0).astype(float)

    df["f_form5"] = g["good"].transform(
        lambda s: s.shift(1).rolling(HIST_WIN, min_periods=1).mean())
    df["f_last"] = g["good"].shift(1)
    df["f_bm_h"] = g["bm_rank"].transform(lambda s: s.shift(1).expanding().mean())
    df["f_l3f"] = g["l3f_good"].transform(
        lambda s: s.shift(1).rolling(HIST_WIN, min_periods=1).mean())

    days = g["dt"].diff().dt.days
    df["f_days"] = np.log(days.clip(lower=7) / 30.0).clip(-2, 2)
    df["f_wtc"] = g["weight_carried"].diff()
    df["f_bw"] = df["bw_chg"]

    # 同距離帯での過去平均成績
    g2 = df.groupby(["horse", "dist_cat"], sort=False)
    df["f_dist"] = g2["good"].transform(lambda s: s.shift(1).expanding().mean())

    # 乗り替わり
    df["prev_jockey"] = g["jockey"].shift(1)
    df["j_switch"] = np.where(df["prev_jockey"].isna(), 0.0,
                              (df["jockey"] != df["prev_jockey"]).astype(float))

    for c in ["f_form5", "f_last", "f_bm_h", "f_l3f", "f_days", "f_wtc",
              "f_bw", "f_dist"]:
        df[c] = df[c].fillna(0.0)
    return df


def add_jockey_feature(df):
    """j_bm: 騎手の(勝利−市場確率)累積平均を前日までで縮小推定。"""
    m = df["win_odds"].notna() & df["finish"].notna()
    d = df.loc[m, ["race_id", "date", "jockey", "finish", "win_odds"]].copy()
    inv = 1.0 / d["win_odds"]
    d["mkt"] = inv / inv.groupby(d["race_id"]).transform("sum")
    d["res"] = (d["finish"] == 1).astype(float) - d["mkt"]

    jd = (d.groupby(["jockey", "date"])
            .agg(s=("res", "sum"), c=("res", "size"))
            .reset_index().sort_values(["jockey", "date"], kind="mergesort"))
    gj = jd.groupby("jockey", sort=False)
    # 累積から当日分を引く=前日までの厳密な集計(当日リークなし)
    cs = gj["s"].cumsum() - jd["s"]
    cc = gj["c"].cumsum() - jd["c"]
    jd["j_bm"] = cs / (cc + K_JOCKEY)

    df = df.merge(jd[["jockey", "date", "j_bm"]], on=["jockey", "date"], how="left")
    df["j_bm"] = df["j_bm"].fillna(0.0)
    return df


def build_model_rows(df):
    """モデリング対象行: TRAIN開始以降・オッズあり・5頭以上。"""
    m = (df["date"] >= TRAIN[0]) & df["win_odds"].notna()
    d = df[m].copy()
    sizes = d.groupby("race_id")["umaban"].transform("count")
    d = d[sizes >= MIN_HORSES].copy()
    inv = 1.0 / d["win_odds"]
    d["market_prob"] = inv / inv.groupby(d["race_id"]).transform("sum")
    d["log_m"] = np.log(d["market_prob"])
    d["win"] = (d["finish"] == 1).astype(int)
    return d.reset_index(drop=True)


def standardize(tr, others, cols):
    stats = {c: (tr[c].mean(), max(tr[c].std(), 1e-9)) for c in cols}
    for d in [tr] + others:
        for c in cols:
            mu, sd = stats[c]
            d[c] = (d[c] - mu) / sd
    return tr, others


# ---------- メイン ----------
def main():
    raw = db.connect()
    print("全出走履歴の読み込み+特徴量構築中...", flush=True)
    runs = load_runs(raw)
    runs = add_horse_features(runs)
    runs = add_jockey_feature(runs)
    data = build_model_rows(runs)

    tr, va, te = [split(data, p).copy() for p in (TRAIN, VALID, TEST)]
    tr, (va, te) = standardize(tr, [va, te], FEATS)
    for name, d in [("train", tr), ("valid", va), ("test", te)]:
        print(f"  {name}: {d['race_id'].nunique()}R / {len(d)}頭")
    cover = float((tr["f_nohist"] < tr["f_nohist"].max()).mean()) \
        if tr["f_nohist"].std() > 0 else 1.0
    print(f"  train内 前走履歴あり: {cover*100:.1f}%")

    # ===== 第1段階: ファンダメンタルモデル(市場なし) =====
    print("\n=== 第1段階: ファンダメンタルモデル(市場情報なし) ===", flush=True)
    clg_f = CLogit(tr, FEATS)                      # offset=0(一様)
    beta_f, ll_f = clg_f.fit()
    ll_unif = clg_f.loglik(np.zeros(len(FEATS)))
    print(f"  train logLik: {ll_f:.1f} (一様 {ll_unif:.1f}, 改善 {ll_f-ll_unif:+.1f})")
    for c, b in sorted(zip(FEATS, beta_f), key=lambda t: -abs(t[1])):
        print(f"    {c:<9} β={b:+.3f}")

    # drop-one 検定(train)
    print("  --- drop-one 尤度比検定(train) ---")
    for c in FEATS:
        cols = [f for f in FEATS if f != c]
        _, ll_d = CLogit(tr, cols).fit()
        lr = 2 * (ll_f - ll_d)
        print(f"    {c:<9} 2ΔLL={lr:7.2f} {'★5%有意' if lr > CHI2_5PCT[1] else '(n.s.)'}")

    # 各分割の対数損失: 市場 vs ファンダ単体
    print("\n  対数損失(小さいほど良い):")
    lf = {}
    for name, d in [("train", tr), ("valid", va), ("test", te)]:
        clg = CLogit(d, FEATS)
        p_f = clg.probs(beta_f)
        d["log_f"] = np.log(np.clip(p_f, 1e-12, None))
        ll_mkt = float(-np.log(np.clip(
            d.loc[d["win"] == 1, "market_prob"], 1e-12, None)).mean())
        ll_fund = float(-d.loc[d["win"] == 1, "log_f"].mean())
        lf[name] = (ll_mkt, ll_fund)
        print(f"    {name:<5} 市場={ll_mkt:.4f}  ファンダ単体={ll_fund:.4f}"
              f"  (差 {ll_fund-ll_mkt:+.4f})")

    # ===== 第2段階: 合成 p ∝ exp(α·ln f + β·ln π) =====
    print("\n=== 第2段階: 市場との合成 ===", flush=True)
    clg_c = CLogit(tr, ["log_f", "log_m"])
    ab, ll_c = clg_c.fit()
    alpha, beta_m = ab
    ll_mkt_tr = clg_c.loglik(np.array([0.0, 1.0]))
    lr = 2 * (ll_c - ll_mkt_tr)
    print(f"  α(ファンダ)={alpha:+.3f}  β(市場)={beta_m:+.3f}")
    print(f"  train: 合成logLik={ll_c:.1f} vs 市場のみ={ll_mkt_tr:.1f}"
          f"  2ΔLL={lr:.2f}(df=2) {'★5%有意' if lr > CHI2_5PCT[2] else '(n.s.)'}")

    print("\n  対数損失と勝者尤度改善(パラメータはtrain固定):")
    for name, d in [("train", tr), ("valid", va), ("test", te)]:
        clg = CLogit(d, ["log_f", "log_m"])
        p_c = clg.probs(ab)
        d["p_comb"] = p_c
        ll_comb = float(-np.log(np.clip(p_c[d["win"] == 1], 1e-12, None)).mean())
        lr_d = 2 * (clg.loglik(ab) - clg.loglik(np.array([0.0, 1.0])))
        print(f"    {name:<5} 市場={lf[name][0]:.4f} → 合成={ll_comb:.4f}"
              f"  (差 {ll_comb-lf[name][0]:+.4f}, 2ΔLL={lr_d:+.1f})")

    # ===== EVスキャン =====
    print("\n=== EV閾値スキャン(合成モデル) ===")
    for name, d in [("valid", va), ("test", te)]:
        line = f"  {name:<5}"
        for tau in (1.00, 1.05, 1.10, 1.20, 1.30):
            roi, nbet = roi_at(d, d["p_comb"].to_numpy(), tau)
            line += f"  τ{tau:.2f}:{'—' if not nbet else f'{roi:.0f}%({nbet})'}"
        print(line)

    print("\n⚠️ 確定オッズでの検証(実運用の前日オッズでは下振れしうる)")


if __name__ == "__main__":
    main()
