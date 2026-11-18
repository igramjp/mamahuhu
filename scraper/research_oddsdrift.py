"""
特徴量研究 第6回: オッズ変動(スマートマネー追随)。計画は RESEARCH.md
「検証待ちの仮説 1」(2026-07-05確定)。

問い: 前日夜 → 当日朝 → 確定 のオッズ変動は、確定オッズに織り込まれて
いない勝率情報を持つか(直前に売れた馬は確定オッズでもなお勝ちすぎるか)。

データ: forward_odds_history(前日夜・当日朝のスナップショット)+
results.win_odds(確定)。

  r_over  = ln(当日朝 / 前日夜)   負 = 夜のうちに売れた
  r_late  = ln(確定   / 当日朝)   負 = 当日朝以降に売れた
  r_total = ln(確定   / 前日夜)

検定: 条件付きロジット p ∝ exp(β·ln π確定 + γ·r) の γ を尤度比検定。
γ<0 なら「売れた馬は確定オッズでもまだ過小評価」。
r_late / r_total は確定オッズを含むため発走前には使えない(観測はできるが
賭けられない)。賭けに使えるのは r_over のみ。

限界(重要):
- 「当日朝」は発走の数時間前。締切直前(数分前)のスナップショットは無いので、
  r_late は「朝〜締切の合計」であり、締切間際の動きだけを切り出せない。
- 発走時刻を保存していないため、レース番号から推定した発走時刻より
  PRE_OFF_MIN 分以上前のスナップショットだけを「発走前」として採用する
  (cron遅延で朝スナップショットが10〜11時にずれ込んだ週は前半レースが落ちる)。

使い方:
  python scraper/research_oddsdrift.py
"""

import numpy as np
import pandas as pd

import db

START = "20260711"   # 完全な3点セットが揃い始めた週末
PRE_OFF_MIN = 20
# JRAの発走時刻の目安(早め=保守的)。1R 9:50、昼休みを挟み、12R 16:00前後
EST_START_MIN = {1: 590, 2: 615, 3: 645, 4: 675, 5: 725, 6: 755, 7: 785,
                 8: 815, 9: 845, 10: 880, 11: 915, 12: 955}
CHI2_1DF = 3.84
RNG = np.random.default_rng(20260705)


# ---------- データ ----------
def load(conn):
    hist = pd.read_sql_query(
        "SELECT h.race_id, h.umaban, h.snapped_at, h.win_odds, fr.date, fr.race_no"
        " FROM forward_odds_history h JOIN forward_races fr USING(race_id)"
        " WHERE fr.date >= ? AND h.win_odds IS NOT NULL AND h.win_odds > 0",
        conn, params=(START,))
    hist["t"] = pd.to_datetime(hist["snapped_at"])
    day = pd.to_datetime(hist["date"], format="%Y%m%d")
    hist["start"] = day + pd.to_timedelta(hist["race_no"].map(EST_START_MIN), unit="m")
    hist["morning_from"] = day + pd.Timedelta(hours=4)
    hist = hist[hist["t"] <= hist["start"] - pd.Timedelta(minutes=PRE_OFF_MIN)]
    hist["kind"] = np.where(hist["t"] < hist["morning_from"], "eve", "morn")
    hist["lead_h"] = (hist["start"] - hist["t"]).dt.total_seconds() / 3600

    # 各(馬, kind)で最新のスナップショット
    hist = hist.sort_values("t").groupby(["race_id", "umaban", "kind"]).tail(1)
    wide = hist.pivot(index=["race_id", "umaban"], columns="kind",
                      values=["win_odds", "lead_h"])
    wide.columns = [f"{a}_{b}" for a, b in wide.columns]
    wide = wide.reset_index()

    res = pd.read_sql_query(
        "SELECT r.race_id, r.umaban, r.finish, r.win_odds AS odds_f,"
        "       ra.date, ra.race_no, ra.place, ra.n_starters"
        " FROM results r JOIN races ra USING(race_id)"
        " WHERE ra.date >= ? AND r.win_odds IS NOT NULL AND r.win_odds > 0",
        conn, params=(START,))
    df = res.merge(wide, on=["race_id", "umaban"], how="inner")
    df = df.rename(columns={"win_odds_eve": "odds_e", "win_odds_morn": "odds_m"})
    for c in ("odds_e", "odds_m", "lead_h_eve", "lead_h_morn"):
        if c not in df:
            df[c] = np.nan
    df["win"] = (df["finish"] == 1)
    df["month"] = df["date"].str[:6]
    inv = 1 / df["odds_f"]
    df["pi_f"] = inv / inv.groupby(df["race_id"]).transform("sum")
    df["ln_pi"] = np.log(df["pi_f"])
    df["r_over"] = np.log(df["odds_m"] / df["odds_e"])
    df["r_late"] = np.log(df["odds_f"] / df["odds_m"])
    df["r_total"] = np.log(df["odds_f"] / df["odds_e"])
    # 勝ち馬がちょうど1頭いるレースのみ(同着・取消で勝ち馬が落ちたレースを除く)
    nwin = df.groupby("race_id")["win"].transform("sum")
    return df[nwin == 1].copy()


# ---------- 条件付きロジット(Newton法) ----------
def clogit_fit(df, cols):
    g = df["race_id"].to_numpy()
    order = np.argsort(g, kind="mergesort")
    gs = g[order]
    X = df[cols].to_numpy(float)[order]
    win = df["win"].to_numpy(bool)[order]
    starts = np.r_[0, np.flatnonzero(gs[1:] != gs[:-1]) + 1]
    seg = np.repeat(np.arange(len(starts)), np.diff(np.r_[starts, len(gs)]))

    def probs(b):
        s = X @ b
        s = s - np.maximum.reduceat(s, starts)[seg]
        e = np.exp(s)
        return e / np.add.reduceat(e, starts)[seg]

    def ll(b):
        return float(np.log(probs(b)[win]).sum())

    b = np.zeros(len(cols))
    b[0] = 1.0
    for _ in range(50):
        p = probs(b)
        grad = X.T @ (win - p)
        xbar = np.add.reduceat(X * p[:, None], starts)[seg]
        xc = X - xbar
        hess = (xc * p[:, None]).T @ xc
        step = np.linalg.solve(hess, grad)
        b = b + step
        if np.abs(step).max() < 1e-9:
            break
    se = np.sqrt(np.diag(np.linalg.inv(hess)))
    return b, se, ll(b), ll


def lr_test(df, feat, label):
    d = df.dropna(subset=[feat])
    d = d[d.groupby("race_id")["win"].transform("sum") == 1]
    b0, _, ll0, _ = clogit_fit(d, ["ln_pi"])
    b1, se1, ll1, _ = clogit_fit(d, ["ln_pi", feat])
    stat = 2 * (ll1 - ll0)
    sig = "有意(5%)" if stat > CHI2_1DF else "n.s."
    print(f"  {label:<22} R={d['race_id'].nunique():>4}  β0={b0[0]:.3f}"
          f"  β={b1[0]:.3f}  γ={b1[1]:+.3f}±{se1[1]:.3f}"
          f"  2ΔLL={stat:5.2f} {sig}")
    return d, b0, b1


def forward_split(df, feat):
    """前半の週で学習 → 後半の週に係数固定で適用(再現性)。"""
    d = df.dropna(subset=[feat])
    d = d[d.groupby("race_id")["win"].transform("sum") == 1]
    dates = sorted(d["date"].unique())
    cut = dates[len(dates) // 2]
    tr, te = d[d["date"] < cut], d[d["date"] >= cut]
    b0, _, _, _ = clogit_fit(tr, ["ln_pi"])
    b1, se1, _, _ = clogit_fit(tr, ["ln_pi", feat])
    _, _, _, ll0_te = clogit_fit(te, ["ln_pi"])
    _, _, _, ll1_te = clogit_fit(te, ["ln_pi", feat])
    gain = 2 * (ll1_te(b1) - ll0_te(b0))
    bt, set_, _, _ = clogit_fit(te, ["ln_pi", feat])
    print(f"  {feat}: 前半(〜{cut}未満, R={tr['race_id'].nunique()}) γ={b1[1]:+.3f}±{se1[1]:.3f}"
          f" → 後半(R={te['race_id'].nunique()})に固定適用 2ΔLL={gain:+.2f}"
          f" / 後半単独推定 γ={bt[1]:+.3f}±{set_[1]:.3f}")


# ---------- 記述統計 ----------
def roi_ci(d, n_boot=2000):
    """確定オッズで単勝100円ずつ買った回収率と、レース単位ブートストラップ95%CI。"""
    pay = np.where(d["win"], d["odds_f"], 0.0)
    roi = pay.mean()
    by_race = pd.DataFrame({"race": d["race_id"].to_numpy(), "pay": pay}).groupby("race")["pay"]
    s, c = by_race.sum().to_numpy(), by_race.count().to_numpy()
    idx = RNG.integers(0, len(s), size=(n_boot, len(s)))
    boots = s[idx].sum(1) / c[idx].sum(1)
    lo, hi = np.percentile(boots, [2.5, 97.5])
    return roi, lo, hi


def magnitude(df, feat, label):
    print(f"\n[{label}] 変動の大きさ(月別)  ※|r|=0.2 は約±22%、0.4 は約±49%")
    d = df.dropna(subset=[feat])
    rows = []
    for m, g in d.groupby("month"):
        a = g[feat].abs()
        lead = g["lead_h_morn"].median() if feat == "r_late" else np.nan
        rows.append((m, g["race_id"].nunique(), len(g), a.median(), (a > 0.2).mean(),
                     (a > 0.4).mean(), lead))
    print("  月      R    頭数  中央|r|  |r|>0.2  |r|>0.4  朝スナップ→発走(中央h)")
    for m, r, n, med, p2, p4, lead in rows:
        print(f"  {m} {r:>4} {n:>6}   {med:.3f}   {p2:6.1%}   {p4:6.1%}   "
              + ("-" if np.isnan(lead) else f"{lead:.1f}"))


def late_by_leadtime(df):
    print("\n[r_late] 朝スナップショットから発走までの時間を揃えた比較(時期×残り時間)")
    d = df.dropna(subset=["r_late"]).copy()
    d["period"] = np.where(d["date"] < "20260901", "7-8月", np.where(
        d["date"] < "20261001", "9月", "10-11月"))
    d["lead"] = pd.cut(d["lead_h_morn"], [0, 2, 4, 6, 9], labels=["〜2h", "2-4h", "4-6h", "6h〜"])
    t = d.groupby(["lead", "period"], observed=True)["r_late"].agg(
        n="size", med_abs=lambda s: s.abs().median(), big=lambda s: (s.abs() > 0.4).mean())
    print(t.round(3).to_string())


def direction(df, feat, base, label):
    print(f"\n[{label}] 起点オッズ帯別の動き(負=売れてオッズが下がった)")
    d = df.dropna(subset=[feat]).copy()
    d["band"] = pd.cut(d[base], [0, 3, 5, 10, 20, 50, 1e9],
                       labels=["〜3倍", "3-5", "5-10", "10-20", "20-50", "50〜"])
    t = d.groupby("band", observed=True)[feat].agg(
        n="size", mean="mean", median="median", shorten=lambda s: (s < 0).mean())
    print(t.round(3).to_string())


def buckets(df, feat, label, beta0):
    print(f"\n[{label}] 変動帯別の成績(確定オッズで買った場合)")
    print("  帯                 頭数  勝率    市場想定  勝率/想定  回収率 [95%CI]")
    d = df.dropna(subset=[feat]).copy()
    # 本命バイアス補正後の市場想定勝率(β0でシャープ化して正規化)
    w = d["pi_f"] ** beta0
    d["p_mkt"] = w / w.groupby(d["race_id"]).transform("sum")
    edges = [-np.inf, -0.4, -0.2, -0.05, 0.05, 0.2, 0.4, np.inf]
    names = ["大幅に売れた(<-0.4)", "売れた(-0.4〜-0.2)", "やや売れた", "ほぼ不変",
             "やや嫌われた", "嫌われた(0.2〜0.4)", "大幅に嫌われた(>0.4)"]
    d["b"] = pd.cut(d[feat], edges, labels=names)
    for name in names:
        g = d[d["b"] == name]
        if len(g) < 30:
            print(f"  {name:<16} {len(g):>5}  (少数のため省略)")
            continue
        roi, lo, hi = roi_ci(g)
        wr, ex = g["win"].mean(), g["p_mkt"].mean()
        print(f"  {name:<16} {len(g):>5}  {wr:5.1%}  {ex:5.1%}    {wr / ex:4.2f}"
              f"     {roi:5.1%} [{lo:5.1%}, {hi:5.1%}]")


def main():
    conn = db.connect()
    df = load(conn)
    n_all = pd.read_sql_query(
        "SELECT COUNT(*) n FROM races WHERE date >= ?", conn, params=(START,))["n"][0]
    has_m = df.dropna(subset=["odds_m"])["race_id"].nunique()
    has_e = df.dropna(subset=["odds_e"])["race_id"].nunique()
    has_3 = df.dropna(subset=["odds_e", "odds_m"])["race_id"].nunique()
    print(f"期間 {START}〜{df['date'].max()}  結果のあるレース {n_all}R")
    print(f"発走前スナップショットあり: 当日朝 {has_m}R / 前日夜 {has_e}R / 3点揃い {has_3}R")
    cov = df.groupby("month").apply(lambda g: pd.Series({
        "R": g["race_id"].nunique(),
        "朝あり": g.dropna(subset=["odds_m"])["race_id"].nunique(),
        "夜あり": g.dropna(subset=["odds_e"])["race_id"].nunique(),
        "3点": g.dropna(subset=["odds_e", "odds_m"])["race_id"].nunique()}),
        include_groups=False)
    print(cov.to_string())

    magnitude(df, "r_over", "前日夜→当日朝 r_over")
    magnitude(df, "r_late", "当日朝→確定 r_late")
    late_by_leadtime(df)
    direction(df, "r_late", "odds_m", "当日朝→確定 r_late")
    direction(df, "r_over", "odds_e", "前日夜→当日朝 r_over")

    print("\n[検定] p ∝ exp(β·ln π確定 + γ·r)。γ<0 = 売れた馬は確定オッズでもまだ過小評価")
    _, b0, _ = lr_test(df, "r_late", "r_late (朝→確定)")
    lr_test(df, "r_over", "r_over (夜→朝)")
    lr_test(df, "r_total", "r_total(夜→確定)")
    late = df.dropna(subset=["r_late"])
    lr_test(late[late["lead_h_morn"] <= 4], "r_late", "r_late 発走4h以内")
    lr_test(late[late["lead_h_morn"] > 4], "r_late", "r_late 発走4h超")

    print("\n[前向き分割] 前半の係数を後半に固定適用(正なら再現)")
    for f in ("r_late", "r_over", "r_total"):
        forward_split(df, f)

    buckets(df, "r_late", "当日朝→確定 r_late", b0[0])
    buckets(df, "r_over", "前日夜→当日朝 r_over", b0[0])

    print("\n[朝の1番人気を買った人が見たオッズと、実際の確定オッズ]")
    d = df.dropna(subset=["odds_m"])
    fav = d.loc[d.groupby("race_id")["odds_m"].idxmin()]
    ratio = fav["odds_f"] / fav["odds_m"]
    roi, lo, hi = roi_ci(fav)
    print(f"  R={len(fav)}  確定/朝 の中央値 {ratio.median():.2f}"
          f"  下がった割合 {(ratio < 1).mean():.1%}  2割以上下がった {(ratio < 0.8).mean():.1%}"
          f"  2割以上上がった {(ratio > 1.2).mean():.1%}")
    print(f"  勝率 {fav['win'].mean():.1%}  確定オッズでの回収率 {roi:.1%} [{lo:.1%}, {hi:.1%}]"
          f"  朝オッズのまま払われたと仮定した回収率 {(fav['win'] * fav['odds_m']).mean():.1%}")


if __name__ == "__main__":
    main()
