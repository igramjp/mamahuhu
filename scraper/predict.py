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
import re
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

# 確定データのレース名からグレードを拾う: "…賞(GIII)" → G3。
# 障害(JGIII等)は surface='障害' の時点で分析対象外なので考慮しない
_GRADE_RE = re.compile(r"\(G(I{1,3})\)")


def _grade_from_name(race_name):
    m = _GRADE_RE.search(race_name or "")
    return f"G{len(m.group(1))}" if m else None


def _prev_kaisai_date(conn, place, target_date):
    row = conn.execute(
        "SELECT MAX(date) FROM races WHERE place = ? AND date < ?"
        " AND surface IN ('芝','ダート')", (place, target_date)).fetchone()
    return row[0] if row else None


def _load_deviations(conn, place, target_date):
    """前開催日の枠バイアス deviation を (prev_date, {surface: {grp: dev}})
    で返す(リーク防止: target_date 当日の結果は使わない)。"""
    prev_date = _prev_kaisai_date(conn, place, target_date)
    bias_report = build_bias_report(conn, place, prev_date) if prev_date else None
    deviations = {}
    if bias_report:
        for surface, s in bias_report["surfaces"].items():
            fb = s.get("frame_bias")
            if fb:
                deviations[surface] = {
                    g["group"]: (g.get("deviation") or 0.0) for g in fb["groups"]}
    return prev_date, deviations


def _pack_race(sub, dev, race_no, race_name, surface, distance, grade=None):
    """1レース分の予想dictを作る共通処理。
    sub: umaban, odds, frame3, horse, jockey, finish 列を持つDataFrame。
    grade: G1/G2/G3ならサイズ抑制の行間引きをせず全頭を保存する。"""
    sub = sub.copy()

    # 1. 市場確率: 1/オッズ をレース内正規化
    inv = 1.0 / sub["odds"]
    sub["market_prob"] = inv / inv.sum()

    # 2. 補正: 枠グループの deviation(負=いつもより有利)を log 確率に加点
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
        "horse": r.horse if isinstance(r.horse, str) else None,
        "jockey": r.jockey if isinstance(r.jockey, str) else None,
        "odds": round(float(r.odds), 1),
        "market_prob": round(float(r.market_prob), 4),
        "model_prob": round(float(r.model_prob), 4),
        "ev": round(float(r.ev), 3),
        "edge": round(float(r.edge), 4),
        "recommended": bool(r.recommended),
        "attention": int(r.umaban) == attention_umaban,
        "rank": int(r.finish) if pd.notna(r.finish) else None,
        "frame3": r.frame3,
    } for r in sub.itertuples()]

    n_reco = sum(1 for h in horses if h["recommended"])

    # 判定根拠(サイトの「分析の内訳」表示用)。
    # overround = Σ1/オッズ。1を超えるぶんが実質控除率(スナップショット時点)
    best = max(horses, key=lambda h: h["ev"])
    analysis = {
        "deviations": {g: round(d, 4) for g, d in dev.items()},
        "beta": BETA,
        "ev_threshold": EV_THRESHOLD,
        "attention_floor": ATTENTION_FLOOR,
        "n_horses": len(horses),
        "overround": round(float((1.0 / sub["odds"]).sum()), 3),
        "max_ev": best["ev"],
        "max_ev_umaban": best["umaban"],
        "max_edge": round(float(sub["edge"].max()), 4),
    }

    horses.sort(key=lambda h: (-h["edge"], -h["model_prob"]))
    # site.dbのサイズ抑制: 表示に使う行(推奨馬・注目馬 + 上位3頭)だけ保存。
    # G3以上は見送りでも全頭を開示する(重賞は読者の照合ニーズが高い)
    if not grade:
        horses = [h for i, h in enumerate(horses)
                  if h["recommended"] or h["attention"] or i < 3]
    return {
        "race_no": race_no,
        "race_name": race_name,
        "grade": grade,
        "surface": surface,
        "distance": int(distance) if distance is not None else None,
        "verdict": "推奨" if n_reco else "見送り",
        "model_version": MODEL_VERSION,
        "horses": horses,
        "analysis": analysis,
    }


def build_predictions(conn, place, target_date=None):
    """target_date のレースに対する予想リストを返す。
    バイアスは「その日の朝に計算できた値」= 前開催日のレポートを使う
    (リーク防止。当日の結果は使わない)。"""
    if target_date is None:
        target_date = latest_date_for_place(conn, place)
        if target_date is None:
            return None, None

    prev_date, deviations = _load_deviations(conn, place, target_date)

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

        # 馬名・騎手を結合
        names = dict(conn.execute(
            "SELECT umaban, horse || '|' || COALESCE(jockey,'') FROM results"
            " WHERE race_id = ?", (rid,)))
        sub["horse"] = sub["umaban"].map(
            lambda u: names[u].split("|", 1)[0] if u in names else None)
        sub["jockey"] = sub["umaban"].map(
            lambda u: (names[u].split("|", 1)[1] or None) if u in names else None)

        race_name = conn.execute(
            "SELECT race_name FROM races WHERE race_id = ?", (rid,)).fetchone()[0]
        race = _pack_race(
            sub, deviations.get(meta["surface"], {}),
            int(str(rid)[-2:]), race_name, meta["surface"], meta["distance"],
            grade=_grade_from_name(race_name))
        race["analysis"]["prev_date"] = prev_date
        races.append(race)

    races.sort(key=lambda r: r["race_no"])
    return target_date, races


def build_forward_predictions(conn, place, target_date):
    """順方向の予想: odds.py が保存した前日スナップショット
    (forward_races/forward_entries)から、まだ走っていないレースの
    EV・注目馬を計算する。オッズは前日(前売り)時点のもの。
    データが無ければ (None, None)。"""
    rows = conn.execute(
        "SELECT race_id, race_no, race_name, grade, surface, distance,"
        " snapped_at"
        " FROM forward_races WHERE date = ? AND place = ?"
        " AND surface IN ('芝','ダート') ORDER BY race_no",
        (target_date, place)).fetchall()
    if not rows:
        return None, None

    prev_date, deviations = _load_deviations(conn, place, target_date)

    races = []
    for rid, race_no, race_name, grade, surface, distance, snapped_at in rows:
        sub = pd.read_sql_query(
            "SELECT umaban, horse, jockey, win_odds AS odds"
            " FROM forward_entries WHERE race_id = ?"
            " AND win_odds IS NOT NULL", conn, params=[rid])
        if len(sub) < 5:
            continue
        # 相対枠位置3分割(bias.py/backtest.pyと同じ定義)
        rel = sub["umaban"].rank(method="first") / len(sub)
        sub["frame3"] = rel.map(
            lambda p: "内" if p <= 1 / 3 + 1e-9
            else ("中" if p <= 2 / 3 + 1e-9 else "外"))
        sub["finish"] = None
        race = _pack_race(
            sub, deviations.get(surface, {}),
            race_no, race_name, surface, distance, grade=grade)
        race["snapped_at"] = snapped_at   # オッズ取得時点(サイトの鮮度表示用)
        race["analysis"]["prev_date"] = prev_date
        races.append(race)

    races.sort(key=lambda r: r["race_no"])
    return target_date, races


def main():
    ap = argparse.ArgumentParser(description="期待値ベース予想(プロトタイプ)")
    ap.add_argument("place")
    ap.add_argument("--date", default=None)
    ap.add_argument("--db", dest="db_path", default=None)
    ap.add_argument("--export", action="store_true", help="site.dbへ書き込む")
    ap.add_argument("--forward", action="store_true",
                    help="前日スナップショット(odds.py)から順方向に予想する")
    args = ap.parse_args()

    conn = db.connect(args.db_path)
    if args.forward:
        if not args.date:
            ap.error("--forward には --date が必要です")
        date, races = build_forward_predictions(conn, args.place, args.date)
    else:
        date, races = build_predictions(conn, args.place, args.date)
    if not races:
        print(f"{args.place} の予想対象データがありません")
        return

    if args.export:
        import site_db
        site_conn = site_db.connect()
        site_db.write_predictions(site_conn, date, args.place, races,
                                  forward=args.forward)
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
