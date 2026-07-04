"""
期待値モデルのバックテスト (bias_analysis_spec.md 評価・バックテスト要件)。

時系列3分割(ランダム分割禁止):
  train: β を条件付きロジットの対数尤度最大化で学習
  valid: EV閾値を選択(ROI×ベット数)
  test : 最終評価(ROI・キャリブレーション・対数損失 vs 市場)

リーク防止:
  - 特徴量は「前開催日の bias3 deviation」のみ(当日の結果は使わない)
  - deviation は site.db の bias3_stats(=build_bias_report の保存値)を参照。
    前開催日が2023年(site.db範囲外)の場合のみ keiba.db からその場で計算
  - オッズは確定オッズ(注意: 前日オッズより有利な推定になる。実運用では
    前日オッズで再検証が必要)

使い方:
  python scraper/backtest.py
"""

import math
import sys

import numpy as np
import pandas as pd

import db
import site_db
from bias import build_bias_report

TRAIN = ("20240101", "20250630")
VALID = ("20250701", "20251231")
TEST = ("20260101", "20260628")

MIN_HORSES = 5          # predict.py と同じ足切り
BETA_GRID = np.arange(-20.0, 20.01, 0.25)
TAU_GRID = np.arange(1.00, 1.51, 0.05)
MIN_VALID_BETS = 50     # 閾値選択時の最低ベット数(少なすぎる閾値は分散で選ばない)


# ---------- deviation テーブル ----------
def load_deviation_map(site_conn):
    """{(date, place, surface, grp): deviation} (dist_cat='ALL', kind='frame3')"""
    out = {}
    for d, p, s, g, dev in site_conn.execute(
        "SELECT date, place, surface, grp, deviation FROM bias3_stats"
        " WHERE dist_cat = 'ALL' AND kind = 'frame3'"):
        out[(d, p, s, g)] = dev or 0.0
    return out


def fill_missing_prev(raw_conn, dev_map, needed_pairs):
    """site.dbに無い(2023年の)前開催日ぶんを keiba.db から計算して補完。"""
    for date, place in sorted(needed_pairs):
        report = build_bias_report(raw_conn, place, date)
        if not report:
            continue
        for surface, s in report["surfaces"].items():
            fb = s.get("frame_bias")
            if not fb:
                continue
            for g in fb["groups"]:
                dev_map[(date, place, surface, g["group"])] = g.get("deviation") or 0.0
        print(f"  前開催日を補完: {date} {place}", flush=True)


# ---------- データセット構築 ----------
def build_dataset(raw_conn, site_conn):
    """馬単位のDataFrame: race_id, date, market_prob, x(=-deviation), win"""
    df = pd.read_sql_query("""
        SELECT r.race_id, r.date, r.place, r.surface,
               h.umaban, h.finish, h.win_odds
        FROM races r JOIN results h ON r.race_id = h.race_id
        WHERE r.date >= ? AND r.surface IN ('芝','ダート')
          AND h.win_odds IS NOT NULL
        ORDER BY r.race_id, h.umaban
    """, raw_conn, params=[TRAIN[0]])

    # レースサイズ足切り
    sizes = df.groupby("race_id")["umaban"].transform("count")
    df = df[sizes >= MIN_HORSES].copy()

    # 相対枠位置3分位(predict.py / bias.py と同じ定義)
    n = df.groupby("race_id")["umaban"].transform("count")
    rel = df.groupby("race_id")["umaban"].rank(method="first") / n
    df["frame3"] = np.where(rel <= 1 / 3 + 1e-9, "内",
                            np.where(rel <= 2 / 3 + 1e-9, "中", "外"))

    # 市場確率
    inv = 1.0 / df["win_odds"]
    df["market_prob"] = inv / inv.groupby(df["race_id"]).transform("sum")

    # 前開催日マップ(場ごとに開催日を時系列で)
    kaisai = pd.read_sql_query(
        "SELECT DISTINCT date, place FROM races WHERE surface IN ('芝','ダート')"
        " ORDER BY place, date", raw_conn)
    prev_map = {}
    for place, grp in kaisai.groupby("place"):
        dates = grp["date"].tolist()
        for i, d in enumerate(dates):
            prev_map[(place, d)] = dates[i - 1] if i > 0 else None

    df["prev_date"] = [prev_map.get((p, d)) for p, d in zip(df["place"], df["date"])]
    df = df[df["prev_date"].notna()].copy()

    # deviation 参照(site.db) + 2023年ぶんの補完
    dev_map = load_deviation_map(site_conn)
    have = {(d, p) for (d, p, _, _) in dev_map}
    needed = {(pd_, pl) for pd_, pl in zip(df["prev_date"], df["place"])
              if (pd_, pl) not in have}
    if needed:
        fill_missing_prev(raw_conn, dev_map, needed)

    df["dev"] = [dev_map.get((pd_, pl, sf, fr), 0.0)
                 for pd_, pl, sf, fr in zip(df["prev_date"], df["place"],
                                            df["surface"], df["frame3"])]
    df["x"] = -df["dev"]          # 正 = 前開催日に有利化していたグループ
    df["win"] = (df["finish"] == 1).astype(int)
    df["log_m"] = np.log(df["market_prob"])
    return df.reset_index(drop=True)


def split(df, period):
    return df[(df["date"] >= period[0]) & (df["date"] <= period[1])]


# ---------- 条件付きロジット ----------
def race_loglik(df, beta):
    """Σ_races log softmax(log_m + beta*x)[winner] をベクトル演算で。"""
    s = df["log_m"].to_numpy() + beta * df["x"].to_numpy()
    g = df["race_id"].to_numpy()
    # レースごとの logsumexp
    order = np.argsort(g, kind="stable")
    s, gs = s[order], g[order]
    win = df["win"].to_numpy()[order].astype(bool)
    # グループ境界
    starts = np.r_[0, np.flatnonzero(gs[1:] != gs[:-1]) + 1]
    ll = 0.0
    for i, st in enumerate(starts):
        en = starts[i + 1] if i + 1 < len(starts) else len(s)
        seg = s[st:en]
        m = seg.max()
        lse = m + math.log(np.exp(seg - m).sum())
        w = np.flatnonzero(win[st:en])
        if len(w) == 0:
            continue  # 1着該当なし(同着処理等)はスキップ
        ll += float(seg[w[0]] - lse)
    return ll


def model_probs(df, beta):
    s = np.exp(df["log_m"].to_numpy() + beta * df["x"].to_numpy())
    tot = pd.Series(s, index=df.index).groupby(df["race_id"]).transform("sum")
    return s / tot.to_numpy()


# ---------- 評価 ----------
def roi_at(df, p_model, tau):
    ev = p_model * df["win_odds"].to_numpy()
    bet = ev >= tau
    n = int(bet.sum())
    if n == 0:
        return None, 0
    ret = float((df["win_odds"].to_numpy() * df["win"].to_numpy())[bet].sum())
    return ret / n * 100, n


def calibration(df, p_model, n_bins=10):
    d = pd.DataFrame({"p": p_model, "win": df["win"].to_numpy()})
    d["bin"] = pd.qcut(d["p"], n_bins, duplicates="drop")
    return d.groupby("bin", observed=True).agg(
        pred=("p", "mean"), actual=("win", "mean"), n=("win", "size"))


def logloss(df, p):
    win = df["win"].to_numpy().astype(bool)
    return float(-np.log(np.clip(p[win], 1e-12, 1)).mean())


# ---------- メイン ----------
def main():
    raw = db.connect()
    site = site_db.connect()

    print("データセット構築中...", flush=True)
    data = build_dataset(raw, site)
    tr, va, te = split(data, TRAIN), split(data, VALID), split(data, TEST)
    for name, d in [("train", tr), ("valid", va), ("test", te)]:
        print(f"  {name}: {d['race_id'].nunique()}R / {len(d)}頭"
              f" ({d['date'].min()}〜{d['date'].max()})")

    # 1. β学習(train, 条件付きロジット・グリッド)
    print("\nβ学習(条件付きロジット, 基礎項係数=1固定)...", flush=True)
    lls = [(b, race_loglik(tr, b)) for b in BETA_GRID]
    beta_hat, ll_best = max(lls, key=lambda t: t[1])
    ll0 = race_loglik(tr, 0.0)
    n_tr_races = tr["race_id"].nunique()
    print(f"  β̂ = {beta_hat:+.2f}  (train logLik: {ll_best:.1f} vs 市場のみ {ll0:.1f}, "
          f"改善 {ll_best - ll0:+.2f} / {n_tr_races}R)")

    # 尤度比検定(自由度1): 2ΔLL > 3.84 で5%有意
    lr = 2 * (ll_best - ll0)
    print(f"  尤度比統計量 2ΔLL = {lr:.2f} ({'5%有意' if lr > 3.84 else '有意でない'})")

    # 2. EV閾値選択(valid)
    print("\nEV閾値の選択(valid):")
    p_va = model_probs(va, beta_hat)
    choices = []
    for tau in TAU_GRID:
        roi, n = roi_at(va, p_va, tau)
        mark = ""
        if roi is not None and n >= MIN_VALID_BETS:
            choices.append((tau, roi, n))
            mark = " *"
        print(f"  τ={tau:.2f}: ROI={'—' if roi is None else f'{roi:6.1f}%'} bets={n}{mark}")
    if choices:
        tau_hat = max(choices, key=lambda t: t[1])[0]
    else:
        tau_hat = 1.10
        print("  (有効な閾値なし → 既定1.10)")
    print(f"  τ̂ = {tau_hat:.2f}")

    # 3. 最終評価(test)
    print("\n=== test 最終評価 ===")
    p_te = model_probs(te, beta_hat)
    roi, n = roi_at(te, p_te, tau_hat)
    roi_m, n_m = roi_at(te, te["market_prob"].to_numpy(), tau_hat)
    print(f"  ROI(モデル, τ={tau_hat:.2f}): {'—' if roi is None else f'{roi:.1f}%'} ({n}ベット)")
    print(f"  ROI(全馬ベット基準): "
          f"{float((te['win_odds'] * te['win']).sum() / len(te) * 100):.1f}%")
    print(f"  対数損失: モデル {logloss(te, p_te):.4f} vs 市場 {logloss(te, te['market_prob'].to_numpy()):.4f}"
          f" (小さいほど良い)")
    print("\n  キャリブレーション(test, モデル確率10分位):")
    cal = calibration(te, p_te)
    for _, row in cal.iterrows():
        print(f"    予測 {row['pred']*100:5.1f}% → 実際 {row['actual']*100:5.1f}% (n={int(row['n'])})")

    print(f"\n推奨パラメータ: BETA = {beta_hat:+.2f}, EV_THRESHOLD = {tau_hat:.2f}")
    print("⚠️ 確定オッズでの検証のため、前日オッズ運用では成績が下振れする可能性あり")


if __name__ == "__main__":
    main()
