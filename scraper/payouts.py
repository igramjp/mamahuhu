"""
払戻テーブルのパーサ。

db.netkeiba.com のレースページに含まれる払戻表(table.pay_table_01)を
全券種パースする。複雑券種の歪み研究(単勝確率モデル→Harville式で
三連系を価格付けし、実払戻と突合する)の材料。

取り込み経路: backfill.py が火曜のrawdb取り込み時に同じHTMLから抽出
(追加リクエストなし)。過去分は backfill.py --payouts-only で埋められる。
"""

import re

from bs4 import BeautifulSoup

BET_TYPES = {"単勝", "複勝", "枠連", "馬連", "ワイド", "馬単", "三連複", "三連単"}


def _to_int(s):
    try:
        return int(re.sub(r"[,円\s]", "", s))
    except (ValueError, TypeError):
        return None


def parse_db_payouts(html):
    """db.netkeibaレースページHTMLから払戻行のリストを返す。
    [{bet_type, combination, amount, popularity}] (払戻表なしは空リスト)。
    複勝・ワイド・同着の複数行は <br> 区切りで来るため展開する。"""
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for table in soup.select("table.pay_table_01"):
        for tr in table.find_all("tr"):
            th = tr.find("th")
            tds = tr.find_all("td")
            if th is None or len(tds) < 2:
                continue
            bet = th.get_text(strip=True)
            if bet not in BET_TYPES:
                continue
            combos = [c.strip() for c in tds[0].get_text("\n").split("\n")
                      if c.strip()]
            amounts = [a.strip() for a in tds[1].get_text("\n").split("\n")
                       if a.strip()]
            pops = ([p.strip() for p in tds[2].get_text("\n").split("\n")]
                    if len(tds) > 2 else [])
            for i, combo in enumerate(combos):
                if i >= len(amounts):
                    break
                out.append({
                    "bet_type": bet,
                    "combination": re.sub(r"\s+", "", combo),
                    "amount": _to_int(amounts[i]),
                    "popularity": _to_int(pops[i]) if i < len(pops) else None,
                })
    return out
