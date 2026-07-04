"""
期待値ベース予想のプロトタイプ (bias_analysis_spec.md 予想セクション)。

Benter型: 市場確率(オッズ逆算)をベースに、バイアス情報で補正する。
実装は仕様の優先順位3「最小の補正モデル(枠位置×バイアスのみ)」の段階。

    p_market = レース内で正規化した 1/単勝オッズ
    log p_model ∝ log p_market − BETA × deviation(その馬の枠グループ)
    期待値 EV = p_model × オッズ。EV >= EV_THRESHOLD の馬のみ推奨。

BETA は backtest.py の時系列分割(train 2024-01〜2025-06)による条件付き
ロジット学習値。尤度改善は市場比で有意でなく(2ΔLL=0.94)、現行特徴量では
市場に織り込まれていない歪みは検出されない = 推奨は構造的に稀(見送り基調)。

使い方:
  python scraper/predict.py 中山 --date 20240107            # 表示のみ
  python scraper/predict.py 中山 --date 20240107 --export   # site.dbへ
"""

import argparse
import math
from datetime import datetime

import pandas as pd

import db
from bias import build_bias_report, latest_date_for_place, load_horses

MODEL_VERSION = "v1(枠×バイアス・時系列学習済み)"
BETA = 0.5            # backtest.py学習値(2024-01〜2025-06、条件付きロジットMLE)
EV_THRESHOLD = 1.1    # 検証期間でEV≥1.1のベットは発生せず既定値を維持
# 注目馬: モデルが市場より高く評価した馬(期待値基準の推奨とは別ティア)。
# edge = model_prob/market_prob - 1 がこの値以上のレースで、最上位1頭に付す
ATTENTION_FLOOR = 0.02


def _prev_kaisai_date(conn, place, target_date):
    row = conn.execute(
        "SELECT MAX(date) FROM races WHERE place = ? AND date < ?"
        " AND surface IN ('芝','ダート')", (place, target_date)).fetchone()
    return row[0] if row else None


def build_predictions(conn, place, target_date=None):
    """target_date のレースに対する予想リストを返す。
    バイアスは「その日の朝に計算できた値」= 前開催日のレポートを使う
    (リーク防止。当日の結果は使わない)。"""
    if target_date is None:
        target_date = latest_date_for_place(conn, place)
        if target_date is None:
            return None, None

    prev_date = _prev_kaisai_date(conn, place, target_date)
    bias_report = build_bias_report(conn, place, prev_date) if prev_date else None
    deviations = {}   # {surface: {grp: deviation}}
    if bias_report:
        for surface, s in bias_report["surfaces"].items():
            fb = s.get("frame_bias")
            if fb:
                deviations[surface] = {
                    g["group"]: (g.get("deviation") or 0.0) for g in fb["groups"]}

    day = load_horses(conn, place=place, date_eq=target_date)
    if day.empty:
        return None, None

    races = []
    for rid, grp in day.groupby("race_id"):
        meta = grp.iloc[0]
        # 単勝オッズが必要。取消等でオッズ無しの馬は除外
        odds_map = dict(conn.execute(
            "SELECT umaban, win_odds FROM results WHERE race_id = ?"
            " AND win_odds IS NOT NULL", (rid,)))
        sub = grp[grp["umaban"].isin(odds_map)].copy()
        if len(sub) < 5:
            continue
        sub["odds"] = sub["umaban"].map(odds_map)

        # 1. 市場確率: 1/オッズ をレース内正規化
        inv = 1.0 / sub["odds"]
        sub["market_prob"] = inv / inv.sum()

        # 2. 補正: 枠グループの deviation(負=いつもより有利)を log 確率に加点
        dev = deviations.get(meta["surface"], {})
        sub["adj"] = sub["frame3"].map(lambda g: -BETA * dev.get(g, 0.0))
        logit = sub["market_prob"].map(math.log) + sub["adj"]
        w = logit.map(math.exp)
        sub["model_prob"] = w / w.sum()

        # 3. 期待値と買い目。市場確率はレース内正規化で控除率を除いているため、
        #    モデル=市場のときの期待値は約0.8(=控除率ぶん)に落ち着く。
        #    1.1を超えるのは補正が市場と大きく食い違う馬だけで、それが正常。
        sub["ev"] = sub["model_prob"] * sub["odds"]
        sub["recommended"] = sub["ev"] >= EV_THRESHOLD
        # 注目馬: 市場比の評価上乗せ(edge)が敷居以上のレースで、
        # 最上位(edge同率ならモデル確率最大=有利グループ内の最上位人気)1頭
        sub["edge"] = sub["model_prob"] / sub["market_prob"] - 1.0

        attention_umaban = None
        if float(sub["edge"].max()) >= ATTENTION_FLOOR:
            top = sub[sub["edge"] >= sub["edge"].max() - 1e-9]
            attention_umaban = int(top.loc[top["model_prob"].idxmax(), "umaban"])

        horses = [{
            "umaban": int(r.umaban),
            "horse": None,
            "jockey": None,
            "odds": round(float(r.odds), 1),
            "market_prob": round(float(r.market_prob), 4),
            "model_prob": round(float(r.model_prob), 4),
            "ev": round(float(r.ev), 3),
            "edge": round(float(r.edge), 4),
            "recommended": bool(r.recommended),
            "attention": int(r.umaban) == attention_umaban,
            "rank": int(r.finish) if pd.notna(r.finish) else None,
        } for r in sub.itertuples()]
        # 馬名・騎手を補完
        names = dict(conn.execute(
            "SELECT umaban, horse || '|' || COALESCE(jockey,'') FROM results"
            " WHERE race_id = ?", (rid,)))
        for h in horses:
            if h["umaban"] in names:
                nm, jk = names[h["umaban"]].split("|", 1)
                h["horse"], h["jockey"] = nm, jk or None

        n_reco = sum(1 for h in horses if h["recommended"])
        horses.sort(key=lambda h: (-h["edge"], -h["model_prob"]))
        # site.dbのサイズ抑制: 表示に使う行(推奨馬・注目馬 + 上位3頭)だけ保存
        horses = [h for i, h in enumerate(horses)
                  if h["recommended"] or h["attention"] or i < 3]
        races.append({
            "race_no": int(str(rid)[-2:]),
            "race_name": conn.execute(
                "SELECT race_name FROM races WHERE race_id = ?", (rid,)
            ).fetchone()[0],
            "surface": meta["surface"],
            "distance": int(meta["distance"]),
            "verdict": "推奨" if n_reco else "見送り",
            "model_version": MODEL_VERSION,
            "horses": horses,
        })

    races.sort(key=lambda r: r["race_no"])
    return target_date, races


def main():
    ap = argparse.ArgumentParser(description="期待値ベース予想(プロトタイプ)")
    ap.add_argument("place")
    ap.add_argument("--date", default=None)
    ap.add_argument("--db", dest="db_path", default=None)
    ap.add_argument("--export", action="store_true", help="site.dbへ書き込む")
    args = ap.parse_args()

    conn = db.connect(args.db_path)
    date, races = build_predictions(conn, args.place, args.date)
    if not races:
        print(f"{args.place} の予想対象データがありません")
        return

    if args.export:
        import site_db
        site_conn = site_db.connect()
        site_db.write_predictions(site_conn, date, args.place, races)
        n_reco = sum(1 for r in races if r["verdict"] == "推奨")
        print(f"export: {date}_{args.place} {len(races)}R (推奨{n_reco} / 見送り{len(races) - n_reco}) → site.db pred_*")
        return

    for r in races:
        print(f"\n{r['race_no']}R {r['race_name']} ({r['surface']}{r['distance']}m) — {r['verdict']}")
        for h in r["horses"][:5]:
            mark = "◎" if h["recommended"] else " "
            print(f" {mark} {h['umaban']:>2} {h['horse']:<12} odds={h['odds']:>5} "
                  f"市場={h['market_prob']:.3f} モデル={h['model_prob']:.3f} EV={h['ev']:.2f}")


if __name__ == "__main__":
    main()
