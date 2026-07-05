"""
複雑券種の歪み研究(第7回・RESEARCH.md 検証待ち3の本題)。

問い: 単勝で観測した本命-大穴バイアスは、組合せ券種(馬連・馬単・ワイド・
三連複・三連単)で乗算的に増幅されるか。増幅されるなら「人気サイドの
三連系」に控除の壁を超えるポケットがあるかもしれない。

方法:
  1. 単勝オッズ→レース内正規化で勝率 p_i を推定
  2. Harville式(+Stern型減衰 λ2, λ3)で全組合せの確率を構築
     - 2着条件付き: s_j/(S−s_i)、s = p^λ2
     - 3着条件付き: t_k/(T−t_i−t_j)、t = p^λ3
     - λ2=λ3=1 が素のHarville。文献値 λ2≈0.81, λ3≈0.65 を並走
  3. モデル確率の帯ごとに「帯内の全組合せを100円ずつ買う」戦略のROIを実測
     (コスト=組合せ数、リターン=的中した組合せの実配当。全組合せの
     オッズ板は無くても、この形なら払戻データだけで厳密に計算できる)
  4. 人気順の定番戦略(1-2人気馬連、1-2-3人気三連複ボックス等)のROI

データ: payouts(2024-01〜2026-06、8,631R) × results.win_odds(確定単勝)。
控除率の目安: 単複20% / 枠連・馬連・ワイド22.5% / 馬単・三連複25% / 三連単27.5%。

使い方:
  python scraper/research_exotic.py            # Harville + Stern減衰
  python scraper/research_exotic.py --no-stern
"""

import argparse
from collections import defaultdict
from itertools import combinations, permutations

import numpy as np
import pandas as pd

import db

# モデル確率の帯(下限、降順)。帯 b の組合せ = prob ∈ [b, 次に大きい帯)
BANDS = [0.10, 0.03, 0.01, 3e-3, 1e-3, 3e-4, 1e-4, 1e-5, 0.0]
EDGES = np.array(sorted(BANDS) + [1.0])   # np.histogram 用(昇順)
STERN = (0.81, 0.65)                      # (λ2, λ3) 文献近似値
BET_TYPES = ["馬連", "馬単", "ワイド", "三連複", "三連単"]
ORDERED = {"馬単", "三連単"}

_tri_idx_cache = {}


def tri_indices(n):
    """i<j<k の添字グリッド(n共通でキャッシュ)。"""
    if n not in _tri_idx_cache:
        idx = np.array(list(combinations(range(n), 3)))
        _tri_idx_cache[n] = (idx[:, 0], idx[:, 1], idx[:, 2])
    return _tri_idx_cache[n]


def combo_tensors(p, l2, l3):
    """全券種の組合せ確率テンソルを返す。
    A: 馬単(n,n)  U: 馬連(n,n,対称)  W: ワイド(n,n,対称)
    tri: 三連単(n,n,n)  S3: 三連複set確率(n,n,n,置換対称)"""
    n = len(p)
    s = p ** l2
    t = p ** l3
    S, T = s.sum(), t.sum()

    off = ~np.eye(n, dtype=bool)
    p2 = s[None, :] / (S - s[:, None])          # P(j 2nd | i 1st)
    p2 = np.where(off, p2, 0.0)

    A = p[:, None] * p2                          # 馬単 i→j
    U = A + A.T                                  # 馬連 {i,j}

    denom3 = T - t[:, None] - t[None, :]         # (i,j) → 残り分母
    tri = A[:, :, None] * t[None, None, :] / denom3[:, :, None]
    ii, jj, kk = np.ogrid[:n, :n, :n]
    mask3 = (ii != jj) & (kk != ii) & (kk != jj)
    tri = np.where(mask3, tri, 0.0)              # 三連単 i→j→k

    S3 = (tri + tri.transpose(0, 2, 1) + tri.transpose(1, 0, 2)
          + tri.transpose(1, 2, 0) + tri.transpose(2, 0, 1)
          + tri.transpose(2, 1, 0))              # set{i,j,k} 確率(対称)

    # ワイド {i,j} = ペアが3着内に同居する確率 = Σ_k S3[i,j,k]
    # (kはsetの第3の馬。各setは(i,j)固定なら1回ずつしか現れない)
    W = S3.sum(axis=2)
    return A, U, W, tri, S3


def flat_probs(bt, A, U, W, tri, S3, n):
    """券種ごとの全組合せ確率を1次元配列で返す(コスト側ヒストグラム用)。"""
    iu = np.triu_indices(n, k=1)
    if bt == "馬単":
        return A[~np.eye(n, dtype=bool)]
    if bt == "馬連":
        return U[iu]
    if bt == "ワイド":
        return W[iu]
    if bt == "三連単":
        return tri[tri > 0]
    ti, tj, tk = tri_indices(n)
    return S3[ti, tj, tk]


def combo_prob(bt, combo, A, U, W, tri, S3):
    if bt == "馬単":
        return A[combo[0], combo[1]]
    if bt == "馬連":
        return U[combo[0], combo[1]]
    if bt == "ワイド":
        return W[combo[0], combo[1]]
    if bt == "三連単":
        return tri[combo[0], combo[1], combo[2]]
    return S3[combo[0], combo[1], combo[2]]


def parse_combo(combination):
    sep = "→" if "→" in combination else "-"
    try:
        return tuple(int(x) for x in combination.split(sep))
    except ValueError:
        return None


def load(conn):
    horses = pd.read_sql_query("""
        SELECT h.race_id, h.umaban, h.win_odds
        FROM results h
        WHERE h.win_odds IS NOT NULL
          AND h.race_id IN (SELECT DISTINCT race_id FROM payouts)
          AND NOT (COALESCE(h.finish_raw,'') LIKE '%取%'
                   OR COALESCE(h.finish_raw,'') LIKE '%除%')
    """, conn)
    pays = pd.read_sql_query(
        "SELECT race_id, bet_type, combination, amount FROM payouts"
        " WHERE amount IS NOT NULL", conn)
    return horses, pays[pays["bet_type"].isin(BET_TYPES)]


def main():
    ap = argparse.ArgumentParser(description="複雑券種の歪み(Harville突合)")
    ap.add_argument("--no-stern", action="store_true")
    ap.add_argument("--db", dest="db_path", default=None)
    args = ap.parse_args()

    conn = db.connect(args.db_path)
    horses, pays = load(conn)
    by_race_h = dict(tuple(horses.groupby("race_id")))
    by_race_p = dict(tuple(pays.groupby("race_id")))
    race_ids = sorted(set(by_race_h) & set(by_race_p))
    print(f"対象: {len(race_ids):,}R (払戻×確定単勝オッズが揃うレース)\n")

    variants = [("Harville(素)", 1.0, 1.0)]
    if not args.no_stern:
        variants.append((f"Stern減衰(λ2={STERN[0]}, λ3={STERN[1]})", *STERN))

    # 人気順戦略: (券種, 名前, 人気ランクの組合せ列)
    strategies = [
        ("馬連", "1-2人気", [(0, 1)]),
        ("馬連", "1人気-2,3,4人気(3点)", [(0, 1), (0, 2), (0, 3)]),
        ("馬単", "1人気→2人気", [(0, 1)]),
        ("ワイド", "1-2人気", [(0, 1)]),
        ("三連複", "1-2-3人気(1点)", [(0, 1, 2)]),
        ("三連複", "1-5人気ボックス(10点)", list(combinations(range(5), 3))),
        ("三連単", "1→2→3人気(1点)", [(0, 1, 2)]),
        ("三連単", "1-2-3人気ボックス(6点)", list(permutations(range(3)))),
    ]
    strat_stat = {(bt, name): [0, 0.0] for bt, name, _ in strategies}

    for label, l2, l3 in variants:
        agg = {bt: defaultdict(lambda: [0, 0.0]) for bt in BET_TYPES}
        first_variant = (l2 == 1.0 and l3 == 1.0)

        for rid in race_ids:
            hs = by_race_h[rid].sort_values("umaban")
            n = len(hs)
            if n < 6:
                continue
            u = hs["umaban"].to_numpy()
            inv = 1.0 / hs["win_odds"].to_numpy()
            p = inv / inv.sum()
            idx_of = {int(m): i for i, m in enumerate(u)}
            A, U, W, tri, S3 = combo_tensors(p, l2, l3)

            prow = by_race_p[rid]
            sold = set(prow["bet_type"])

            # コスト側: 発売券種の全組合せをモデル確率の帯へ
            for bt in BET_TYPES:
                if bt not in sold:
                    continue
                vals = flat_probs(bt, A, U, W, tri, S3, n)
                counts, _ = np.histogram(vals, bins=EDGES)
                for b, c in zip(EDGES[:-1], counts):
                    if c:
                        agg[bt][float(b)][0] += int(c)

            # リターン側: 的中組合せの実配当を、その組合せの帯へ
            win_map = {bt: {} for bt in BET_TYPES}
            for _, row in prow.iterrows():
                cu = parse_combo(row["combination"])
                if cu is None or any(m not in idx_of for m in cu):
                    continue
                c = tuple(idx_of[m] for m in cu)
                if row["bet_type"] not in ORDERED:
                    c = tuple(sorted(c))
                win_map[row["bet_type"]][c] = row["amount"]
                pr = combo_prob(row["bet_type"], c, A, U, W, tri, S3)
                b = EDGES[:-1][np.searchsorted(EDGES, pr, side="right") - 1]
                agg[row["bet_type"]][float(b)][1] += row["amount"]

            # 人気順戦略(最初のバリアントでのみ集計)
            if first_variant:
                pop_order = np.argsort(-p)
                for bt, name, combos in strategies:
                    if bt not in sold:
                        continue
                    st = strat_stat[(bt, name)]
                    for ranks in combos:
                        if max(ranks) >= n:
                            continue
                        c = tuple(int(pop_order[r]) for r in ranks)
                        if bt not in ORDERED:
                            c = tuple(sorted(c))
                        st[0] += 1
                        st[1] += win_map[bt].get(c, 0.0)

        print(f"===== 帯別ROI: {label} =====")
        print("帯=モデル確率の下限。ROI=帯内の全組合せを100円ずつ買った回収率")
        for bt in BET_TYPES:
            rows = []
            for b in BANDS:
                cnt, ret = agg[bt].get(b, [0, 0.0])
                if cnt:
                    rows.append((f"≥{b:g}", f"{cnt:,}", round(ret / cnt, 1)))
            print(f"\n[{bt}]")
            print(pd.DataFrame(rows, columns=["帯", "組合せ数", "ROI%"])
                  .to_string(index=False))
        print()

    print("===== 人気順の定番戦略ROI(実配当ベース、100円均等) =====")
    for (bt, name), (cnt, ret) in strat_stat.items():
        if cnt:
            print(f"  {bt} {name}: {ret / cnt:6.1f}% (n={cnt:,}点)")


if __name__ == "__main__":
    main()
