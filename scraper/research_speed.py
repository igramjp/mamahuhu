"""
特徴量研究 第3回: スピード指数(日次トラックバリアント補正)。

Benterの主力因子群の再現。着順は誰でも見えるが「負けたが時計は優秀」は
計算しないと見えない、という仮説の検証。

構築(すべて当該レース発走前に計算可能な情報のみ):
  v        = 走破タイム秒 / 距離 × 1000 (秒/1000m)
  par      = 場×馬面×距離ごとの v の累積平均(当日より前のみ、最低PAR_MIN_N走)
  variant  = 当日×場×馬面の全馬残差(v−par)の平均 = その日の馬場の速さ
             (良/不良の時計差はここが吸収する)
  figure   = −(v − par − variant)  正=時計が優秀。±FIG_CLIPでクリップ

馬ごとの特徴量(前走まで, shift(1)):
  f_sp_avg  直近5走の指数平均
  f_sp_best 直近5走の指数ベスト
  f_sp_last 前走の指数
  f_sp_miss 指数履歴なしフラグ

評価: research_fundamental.py の12因子モデルに追加し、
ネスト尤度比検定(df=4)・分割別の第2段階(α,β)独立推定・EVスキャン。

使い方:
  python scraper/research_speed.py
"""

import numpy as np
import pandas as pd

import db
from backtest import TRAIN, VALID, TEST, split, roi_at
from research_fundamental import (CLogit, FEATS, load_runs, add_horse_features,
                                  add_jockey_feature, build_model_rows,
                                  standardize)

PAR_MIN_N = 80    # par採用に必要な過去走数(場×馬面×距離)
DAY_MIN_N = 30    # variant採用に必要な当日走数(日×場×馬面)
FIG_CLIP = 8.0    # 指数クリップ(秒/1000m)
HIST_WIN = 5

SP_FEATS = ["f_sp_avg", "f_sp_best", "f_sp_last", "f_sp_miss"]
CHI2_5PCT_DF4 = 9.49


def parse_time_sec(s):
    """'1:33.5'→93.5, '58.9'→58.9"""
    ext = s.astype(str).str.extract(r"^(?:(\d+):)?(\d+\.?\d*)$")
    mins = pd.to_numeric(ext[0], errors="coerce").fillna(0)
    secs = pd.to_numeric(ext[1], errors="coerce")
    return mins * 60 + secs


def add_speed_figures(runs):
    """全出走履歴に figure 列を付け、馬ごとの履歴特徴量を作る。"""
    runs = runs.sort_values(["dt", "race_id", "umaban"],
                            kind="mergesort").reset_index(drop=True)
    runs["tsec"] = parse_time_sec(runs["time"])
    ok = runs["tsec"].notna() & runs["finish"].notna() & (runs["distance"] > 0)
    runs["v"] = np.where(ok, runs["tsec"] / runs["distance"] * 1000, np.nan)

    # --- par: 場×馬面×距離の累積平均(当日分を除く=前日まで) ---
    d = runs.loc[runs["v"].notna(),
                 ["place", "surface", "distance", "date", "v"]]
    day = (d.groupby(["place", "surface", "distance", "date"])
             .agg(s=("v", "sum"), c=("v", "size"))
             .reset_index()
             .sort_values(["place", "surface", "distance", "date"],
                          kind="mergesort"))
    g = day.groupby(["place", "surface", "distance"], sort=False)
    cs = g["s"].cumsum() - day["s"]
    cc = g["c"].cumsum() - day["c"]
    day["par"] = np.where(cc >= PAR_MIN_N, cs / cc.replace(0, np.nan), np.nan)

    runs = runs.merge(day[["place", "surface", "distance", "date", "par"]],
                      on=["place", "surface", "distance", "date"], how="left")
    runs["resid"] = runs["v"] - runs["par"]

    # --- variant: 当日×場×馬面の全馬残差平均 ---
    dv = runs.loc[runs["resid"].notna(), ["date", "place", "surface", "resid"]]
    var = (dv.groupby(["date", "place", "surface"])
             .agg(variant=("resid", "mean"), n_day=("resid", "size"))
             .reset_index())
    var.loc[var["n_day"] < DAY_MIN_N, "variant"] = np.nan
    runs = runs.merge(var[["date", "place", "surface", "variant"]],
                      on=["date", "place", "surface"], how="left")

    runs["figure"] = (-(runs["resid"] - runs["variant"])).clip(-FIG_CLIP, FIG_CLIP)
    cover = float(runs["figure"].notna().mean())
    print(f"  スピード指数を計算できた走: {cover*100:.1f}%")

    # --- 馬ごとの履歴特徴量(前走まで) ---
    runs = runs.sort_values(["horse", "dt", "race_id"],
                            kind="mergesort").reset_index(drop=True)
    g = runs.groupby("horse", sort=False)
    runs["f_sp_avg"] = g["figure"].transform(
        lambda s: s.shift(1).rolling(HIST_WIN, min_periods=1).mean())
    runs["f_sp_best"] = g["figure"].transform(
        lambda s: s.shift(1).rolling(HIST_WIN, min_periods=1).max())
    runs["f_sp_last"] = g["figure"].shift(1)
    runs["f_sp_miss"] = runs["f_sp_avg"].isna().astype(float)
    for c in ["f_sp_avg", "f_sp_best", "f_sp_last"]:
        runs[c] = runs[c].fillna(0.0)
    return runs


def main():
    raw = db.connect()
    print("全出走履歴の読み込み+特徴量構築中...", flush=True)
    runs = load_runs(raw)
    runs = add_speed_figures(runs)
    runs = add_horse_features(runs)
    runs = add_jockey_feature(runs)
    data = build_model_rows(runs)

    all_feats = FEATS + SP_FEATS
    tr, va, te = [split(data, p).copy() for p in (TRAIN, VALID, TEST)]
    tr, (va, te) = standardize(tr, [va, te], all_feats)
    for name, d in [("train", tr), ("valid", va), ("test", te)]:
        print(f"  {name}: {d['race_id'].nunique()}R / {len(d)}頭")

    # ===== 第1段階: 12因子 vs 12因子+スピード指数 =====
    print("\n=== 第1段階: ネスト比較(train) ===", flush=True)
    clg12 = CLogit(tr, FEATS)
    beta12, ll12 = clg12.fit()
    clg_sp = CLogit(tr, all_feats)
    beta_sp, ll_sp = clg_sp.fit()
    lr = 2 * (ll_sp - ll12)
    print(f"  12因子      logLik={ll12:.1f}")
    print(f"  +スピード指数 logLik={ll_sp:.1f}  2ΔLL={lr:.2f}(df=4)"
          f" {'★5%有意' if lr > CHI2_5PCT_DF4 else '(n.s.)'}")
    for c, b in sorted(zip(all_feats, beta_sp), key=lambda t: -abs(t[1]))[:8]:
        print(f"    {c:<10} β={b:+.3f}")

    # 対数損失(ファンダ単体)
    print("\n  対数損失(ファンダ単体, 市場なし):")
    for name, d in [("train", tr), ("valid", va), ("test", te)]:
        p12 = CLogit(d, FEATS).probs(beta12)
        psp = CLogit(d, all_feats).probs(beta_sp)
        w = d["win"] == 1
        ll_m = float(-np.log(np.clip(d.loc[w, "market_prob"], 1e-12, None)).mean())
        ll_12 = float(-np.log(np.clip(p12[w], 1e-12, None)).mean())
        ll_s = float(-np.log(np.clip(psp[w], 1e-12, None)).mean())
        print(f"    {name:<5} 市場={ll_m:.4f}  12因子={ll_12:.4f}"
              f"  +speed={ll_s:.4f}")
        d["log_f12"] = np.log(np.clip(p12, 1e-12, None))
        d["log_fsp"] = np.log(np.clip(psp, 1e-12, None))

    # ===== 第2段階: 分割別に(α,β)独立推定 =====
    print("\n=== 第2段階: 合成(分割別に独立推定) ===")
    for label, col in [("12因子", "log_f12"), ("+speed", "log_fsp")]:
        print(f"  --- {label} ---")
        for name, d in [("train", tr), ("valid", va), ("test", te)]:
            clg = CLogit(d, [col, "log_m"])
            ab, ll = clg.fit()
            lr_d = 2 * (ll - clg.loglik(np.array([0.0, 1.0])))
            print(f"    {name:<5} α={ab[0]:+.3f} β={ab[1]:+.3f} 2ΔLL={lr_d:6.2f}")

    # train学習の(α,β)固定で valid/test の対数損失とEV
    print("\n=== train固定パラメータでの評価(+speedモデル) ===")
    clg_tr = CLogit(tr, ["log_fsp", "log_m"])
    ab, _ = clg_tr.fit()
    print(f"  α={ab[0]:+.3f} β={ab[1]:+.3f}")
    for name, d in [("valid", va), ("test", te)]:
        clg = CLogit(d, ["log_fsp", "log_m"])
        p_c = clg.probs(ab)
        w = d["win"] == 1
        ll_m = float(-np.log(np.clip(d.loc[w, "market_prob"], 1e-12, None)).mean())
        ll_c = float(-np.log(np.clip(p_c[w], 1e-12, None)).mean())
        line = f"  {name:<5} 対数損失 市場={ll_m:.4f} → 合成={ll_c:.4f}"
        for tau in (1.00, 1.05, 1.10):
            roi, nbet = roi_at(d, p_c, tau)
            line += f"  τ{tau:.2f}:{'—' if not nbet else f'{roi:.0f}%({nbet})'}"
        print(line)

    print("\n⚠️ 確定オッズでの検証(実運用の前日オッズでは下振れしうる)")


if __name__ == "__main__":
    main()
