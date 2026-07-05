"""
data/keiba.db から public/data/site.db を全再生成する。

各開催日×場について:
  1. reports / race_top3 (analyze_to_dict → write_report)
  2. bias3_*   (bias.build_bias_report → write_bias3)
  3. pred_*    (predict.build_predictions → write_predictions)

notable_race(次開催の出馬表)はネットワークが必要なため対象外
(週次の scrape.py --notable-only が担当)。

使い方:
  python scraper/rebuild_site.py                 # 2024-01-01以降を再生成
  python scraper/rebuild_site.py --from 20260101 # 範囲指定
  python scraper/rebuild_site.py --fresh         # site.dbを作り直してから
  python scraper/rebuild_site.py --forward 20260711
      # 前日オッズスナップショット(odds.py)から指定日の順方向予想だけを
      # site.db に書き込む(発走前の期待値分析)。結果確定後の再生成で
      # 同じ (date, place) は確定オッズ版に置き換わる。
"""

import argparse
import os
import sqlite3
import sys

import pandas as pd

import db
import site_db
from bias import export_to_site
from predict import build_forward_predictions, build_predictions
from scrape import analyze_to_dict

# 2023年は長期ベースライン専用(サイト表示はしない)
DEFAULT_FROM = "20240101"

RAW_QUERY = """
SELECT r.race_id, r.surface AS コース, r.distance AS 距離,
       r.track_condition AS 馬場, r.race_name AS レース名,
       h.finish AS 着順, h.wakuban AS 枠番, h.umaban AS 馬番,
       h.jockey AS 騎手, h.popularity AS 人気, h.last3f AS 上り,
       h.passing AS 通過
FROM races r JOIN results h ON r.race_id = h.race_id
WHERE r.date = ? AND r.place = ? AND r.surface IN ('芝','ダート')
"""


def rebuild(date_from, fresh=False):
    if fresh and site_db.SITE_DB_PATH.exists():
        os.remove(site_db.SITE_DB_PATH)
        print(f"fresh: {site_db.SITE_DB_PATH} を削除")

    raw = db.connect()
    site = site_db.connect()

    pairs = list(raw.execute(
        "SELECT DISTINCT date, place FROM races"
        " WHERE date >= ? AND surface IN ('芝','ダート')"
        " ORDER BY date, place", (date_from,)))
    print(f"対象: {len(pairs)}件 (date>={date_from})", flush=True)

    n_ok = n_skip = 0
    for i, (date, place) in enumerate(pairs, 1):
        df = pd.read_sql_query(RAW_QUERY, raw, params=[date, place])
        if df.empty:
            n_skip += 1
            continue
        analysis = analyze_to_dict(df, place, date, source="db")
        site_db.write_report(site, analysis)
        export_to_site(raw, site, place, date)
        _, races = build_predictions(raw, place, date)
        if races:
            site_db.write_predictions(site, date, place, races)
        n_ok += 1
        if i % 50 == 0 or i == len(pairs):
            print(f"  {i}/{len(pairs)} ({date} {place})", flush=True)

    site.close()
    compress_site_db()

    size = os.path.getsize(site_db.SITE_DB_PATH)
    gz_size = os.path.getsize(str(site_db.SITE_DB_PATH) + ".gz")
    print(f"\n完了: ok={n_ok} skip={n_skip} "
          f"site.db={size / 1024:.0f}KB / gz={gz_size / 1024:.0f}KB", flush=True)


def compress_site_db():
    """配信用にVACUUM + gzip。Netlifyはバイナリを自動圧縮しないため、
    gzip版も生成して配信する(ブラウザ側は DecompressionStream で解凍)。"""
    conn = sqlite3.connect(site_db.SITE_DB_PATH)
    conn.execute("PRAGMA page_size=1024")
    conn.execute("VACUUM")
    conn.close()

    import gzip
    gz_path = str(site_db.SITE_DB_PATH) + ".gz"
    with open(site_db.SITE_DB_PATH, "rb") as f_in, \
            gzip.open(gz_path, "wb", compresslevel=9) as f_out:
        f_out.write(f_in.read())


def rebuild_forward(target_date):
    """前日オッズスナップショット(keiba.dbのforward_*)から target_date の
    順方向予想を site.db に書き込む。スナップショットが無い日(月曜など
    開催なし・オッズ未発売)は何もせず正常終了する。"""
    raw = db.connect()
    site = site_db.connect()

    places = [r[0] for r in raw.execute(
        "SELECT DISTINCT place FROM forward_races WHERE date = ?"
        " AND surface IN ('芝','ダート') ORDER BY place", (target_date,))]
    if not places:
        print(f"{target_date}: 前日オッズスナップショットなし — 何もしない")
        return

    n = 0
    for place in places:
        _, races = build_forward_predictions(raw, place, target_date)
        if races:
            site_db.write_predictions(site, target_date, place, races, forward=True)
            n += len(races)
            print(f"  {target_date} {place}: {len(races)}R (発走前オッズ)")
    site.close()
    if n:
        compress_site_db()
    print(f"完了: 順方向予想 {n}R → site.db pred_*", flush=True)


def main():
    ap = argparse.ArgumentParser(description="site.db 全再生成")
    ap.add_argument("--from", dest="date_from", default=DEFAULT_FROM)
    ap.add_argument("--fresh", action="store_true",
                    help="site.dbを削除してから再生成")
    ap.add_argument("--forward", metavar="YYYYMMDD", default=None,
                    help="前日オッズから指定日の順方向予想だけを書き込む")
    args = ap.parse_args()
    if args.forward:
        rebuild_forward(args.forward)
    else:
        rebuild(args.date_from, fresh=args.fresh)


if __name__ == "__main__":
    main()
