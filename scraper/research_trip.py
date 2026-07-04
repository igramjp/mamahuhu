"""
特徴量研究 第4回: trip handicapping(展開・不利の補正)。

仮説: 着順・タイムは市場に織り込み済み(第2-3回で確定)だが、
「その着順がどれだけ恵まれた/不利な展開で出たものか」の補正は
市場が系統的に行っていない可能性がある。

因子(すべて当該レース発走前に計算可能な情報のみ):
  f_pace_adv   過去5走で受けたペース逆風の平均。
               逆風 = レース前半ペースの速さ(par・馬場補正済み) × 自身の先行度。
               速いペースを先行して負けた/遅いペースを追い込んで負けた馬は
               着順以上に強い、の定量化
  f_bias_frame 過去5走で枠グループが享受した「その日の枠バイアス優位」の平均。
               有利バイアスに乗った好走は割り引くべき、の定量化
  f_bias_style 同、脚質グループ版
  f_own_front  自身の先行度(過去5走の平均、コントロール)
  f_pace_fit   今日の展開適性 = 同レース他馬の先行度平均(ペース圧) × 自身の差し度。
               先行馬多数のレースの差し馬 = 展開利、の事前予測

注意: f_bias_* の当日グループ集計には当該馬自身の結果も含まれる
(グループn≈30-60に対し1頭分の自己混入。研究段階では許容)。

評価: 第3回の16因子(12+スピード指数)に追加し、ネスト尤度比検定(df=5)・
drop-one・分割別の第2段階(α,β)独立推定・EVスキャン。

使い方:
  python scraper/research_trip.py
"""

import numpy as np
import pandas as pd

import db
from backtest import TRAIN, VALID, TEST, split, roi_at
from research_fundamental import (CLogit, FEATS, load_runs, add_horse_features,
                                  add_jockey_feature, build_model_rows,
                                  standardize)
from research_speed import SP_FEATS, add_speed_figures

HIST_WIN = 5
SHRINK_K = 30      # 当日グループ集計の縮小(bias.pyと同水準)
PAR_MIN_R = 30     # 前半ペースparに必要な過去レース数
CHI2_5PCT_DF5 = 11.07

TRIP_FEATS = ["f_pace_adv", "f_bias_frame", "f_bias_style",
              "f_own_front", "f_pace_fit"]


def _prior_group_mean(day_tbl, group_cols, val_s="s", val_c="c", min_n=1):
    """日次集計テーブルに「当日を除く累積平均」列を付ける(リークなし共通処理)。"""
    day_tbl = day_tbl.sort_values(group_cols + ["date"], kind="mergesort")
    g = day_tbl.groupby(group_cols, sort=False)
    cs = g[val_s].cumsum() - day_tbl[val_s]
    cc = g[val_c].cumsum() - day_tbl[val_c]
    day_tbl["prior_mean"] = np.where(cc >= min_n, cs / cc.replace(0, np.nan), np.nan)
    return day_tbl


def add_trip_features(runs):
    runs = runs.sort_values(["dt", "race_id", "umaban"],
                            kind="mergesort").reset_index(drop=True)
    n = runs.groupby("race_id")["umaban"].transform("count")

    # 通過順: 先頭コーナー=道中の先行度 / 最終コーナー=脚質(bias3と同定義)
    parts = runs["passing"].astype(str).str.split("-")
    early = pd.to_numeric(parts.str[0], errors="coerce")
    late = pd.to_numeric(parts.str[-1], errors="coerce")
    runs["frontness"] = (0.5 - early / n).clip(-0.5, 0.5)      # 正=前
    runs["style_grp"] = np.where(late.isna(), None,
                                 np.where(late <= n / 2, "逃げ先行", "差し追込"))

    # 相対枠位置3分割(bias.py/backtest.pyと同定義)
    rel = runs.groupby("race_id")["umaban"].rank(method="first") / n
    runs["frame3"] = np.where(rel <= 1 / 3 + 1e-9, "内",
                              np.where(rel <= 2 / 3 + 1e-9, "中", "外"))

    # ---------- 前半ペース(先行3頭のtime−上がり3F, 秒/1000m) ----------
    fsec = runs["tsec"] - runs["last3f"]
    fdist = runs["distance"] - 600
    runs["fp"] = np.where((fdist > 0) & fsec.notna() & (early <= 3),
                          fsec / fdist * 1000, np.nan)
    race_fp = runs.groupby("race_id")["fp"].mean()
    rl = (runs.drop_duplicates("race_id")
              [["race_id", "date", "place", "surface", "distance", "variant"]]
              .copy())
    rl["race_fp"] = rl["race_id"].map(race_fp)

    day = (rl.dropna(subset=["race_fp"])
             .groupby(["place", "surface", "distance", "date"])
             .agg(s=("race_fp", "sum"), c=("race_fp", "size")).reset_index())
    day = _prior_group_mean(day, ["place", "surface", "distance"],
                            min_n=PAR_MIN_R)
    rl = rl.merge(day[["place", "surface", "distance", "date", "prior_mean"]],
                  on=["place", "surface", "distance", "date"], how="left")
    # 馬場の速さ(スピード指数のvariant)も差し引いた「展開としての速さ」
    rl["fastness"] = -(rl["race_fp"] - rl["prior_mean"] - rl["variant"])
    runs = runs.merge(rl[["race_id", "fastness"]], on="race_id", how="left")
    runs["pace_adv_run"] = (runs["fastness"] * runs["frontness"]).clip(-3, 3)
    cover = float(runs["fastness"].notna().mean())
    print(f"  前半ペースを計算できた走: {cover*100:.1f}%")

    # ---------- その日のグループバイアス優位(枠・脚質) ----------
    for label, grp_col in [("frame", "frame3"), ("style", "style_grp")]:
        d = runs.loc[runs["good"].notna() & runs[grp_col].notna(),
                     ["date", "place", "surface", grp_col, "good"]]
        day = (d.groupby(["place", "surface", grp_col, "date"])
                 .agg(s=("good", "sum"), c=("good", "size")).reset_index())
        day = _prior_group_mean(day, ["place", "surface", grp_col], min_n=100)
        w = day["c"] / (day["c"] + SHRINK_K)
        day_mean = day["s"] / day["c"]
        day[f"adv_{label}"] = w * (day_mean - day["prior_mean"])  # 正=その日有利
        runs = runs.merge(
            day[["place", "surface", grp_col, "date", f"adv_{label}"]]
                .rename(columns={grp_col: grp_col}),
            on=["place", "surface", grp_col, "date"], how="left")

    # ---------- 馬ごとの履歴特徴量(前走まで) ----------
    runs = runs.sort_values(["horse", "dt", "race_id"],
                            kind="mergesort").reset_index(drop=True)
    g = runs.groupby("horse", sort=False)
    roll = lambda col: g[col].transform(
        lambda s: s.shift(1).rolling(HIST_WIN, min_periods=1).mean())
    runs["f_pace_adv"] = roll("pace_adv_run")
    runs["f_bias_frame"] = roll("adv_frame")
    runs["f_bias_style"] = roll("adv_style")
    runs["f_own_front"] = roll("frontness")

    # ---------- 今日の展開適性(レース内の他馬先行度 × 自身の差し度) ----------
    of = runs["f_own_front"].fillna(0.0)
    s = of.groupby(runs["race_id"]).transform("sum")
    c = runs.groupby("race_id")["umaban"].transform("count")
    field_pressure = (s - of) / (c - 1).clip(lower=1)
    runs["f_pace_fit"] = field_pressure * (-of)

    for col in TRIP_FEATS:
        runs[col] = runs[col].fillna(0.0)
    return runs


def main():
    raw = db.connect()
    print("全出走履歴の読み込み+特徴量構築中...", flush=True)
    runs = load_runs(raw)
    runs = add_speed_figures(runs)
    runs = add_trip_features(runs)
    runs = add_horse_features(runs)
    runs = add_jockey_feature(runs)
    data = build_model_rows(runs)

    base_feats = FEATS + SP_FEATS
    all_feats = base_feats + TRIP_FEATS
    tr, va, te = [split(data, p).copy() for p in (TRAIN, VALID, TEST)]
    tr, (va, te) = standardize(tr, [va, te], all_feats)
    for name, d in [("train", tr), ("valid", va), ("test", te)]:
        print(f"  {name}: {d['race_id'].nunique()}R / {len(d)}頭")

    # ===== 第1段階: 16因子 vs +trip =====
    print("\n=== 第1段階: ネスト比較(train) ===", flush=True)
    clg_b = CLogit(tr, base_feats)
    beta_b, ll_b = clg_b.fit()
    clg_t = CLogit(tr, all_feats)
    beta_t, ll_t = clg_t.fit()
    lr = 2 * (ll_t - ll_b)
    print(f"  16因子   logLik={ll_b:.1f}")
    print(f"  +trip5因子 logLik={ll_t:.1f}  2ΔLL={lr:.2f}(df=5)"
          f" {'★5%有意' if lr > CHI2_5PCT_DF5 else '(n.s.)'}")
    print("  trip因子の係数:")
    bmap = dict(zip(all_feats, beta_t))
    for c in TRIP_FEATS:
        print(f"    {c:<13} β={bmap[c]:+.3f}")

    print("  --- drop-one 尤度比検定(train, trip因子のみ) ---")
    for c in TRIP_FEATS:
        cols = [f for f in all_feats if f != c]
        _, ll_d = CLogit(tr, cols).fit()
        print(f"    {c:<13} 2ΔLL={2*(ll_t-ll_d):7.2f}"
              f" {'★5%有意' if 2*(ll_t-ll_d) > 3.84 else '(n.s.)'}")

    # 対数損失(ファンダ単体)
    print("\n  対数損失(ファンダ単体, 市場なし):")
    for name, d in [("train", tr), ("valid", va), ("test", te)]:
        pb = CLogit(d, base_feats).probs(beta_b)
        pt = CLogit(d, all_feats).probs(beta_t)
        w = d["win"] == 1
        ll_m = float(-np.log(np.clip(d.loc[w, "market_prob"], 1e-12, None)).mean())
        print(f"    {name:<5} 市場={ll_m:.4f}"
              f"  16因子={float(-np.log(np.clip(pb[w],1e-12,None)).mean()):.4f}"
              f"  +trip={float(-np.log(np.clip(pt[w],1e-12,None)).mean()):.4f}")
        d["log_fb"] = np.log(np.clip(pb, 1e-12, None))
        d["log_ft"] = np.log(np.clip(pt, 1e-12, None))

    # ===== 第2段階: 分割別に(α,β)独立推定 =====
    print("\n=== 第2段階: 合成(分割別に独立推定) ===")
    for label, col in [("16因子", "log_fb"), ("+trip", "log_ft")]:
        print(f"  --- {label} ---")
        for name, d in [("train", tr), ("valid", va), ("test", te)]:
            clg = CLogit(d, [col, "log_m"])
            ab, ll = clg.fit()
            lr_d = 2 * (ll - clg.loglik(np.array([0.0, 1.0])))
            print(f"    {name:<5} α={ab[0]:+.3f} β={ab[1]:+.3f} 2ΔLL={lr_d:6.2f}")

    # train学習の(α,β)固定で valid/test の対数損失とEV
    print("\n=== train固定パラメータでの評価(+tripモデル) ===")
    clg_tr = CLogit(tr, ["log_ft", "log_m"])
    ab, _ = clg_tr.fit()
    print(f"  α={ab[0]:+.3f} β={ab[1]:+.3f}")
    for name, d in [("valid", va), ("test", te)]:
        clg = CLogit(d, ["log_ft", "log_m"])
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
