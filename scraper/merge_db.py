"""
分散バックフィルの部分DBを data/keiba.db にマージする。

GHAの各fetchジョブが出力した部分SQLite(スキーマはdb.pyと同一)を
ATTACHして取り込む。race_id単位で総入れ替えするため冪等。

使い方:
  python scraper/merge_db.py parts/part-202401.db parts/part-202402.db ...
"""

import sys

import db


def merge_part(conn, part_path):
    conn.execute("ATTACH DATABASE ? AS part", (str(part_path),))
    try:
        n_races = conn.execute("SELECT COUNT(*) FROM part.races").fetchone()[0]
        with conn:
            # レース単位で総入れ替え(部分DB側を正とする)
            conn.execute(
                "DELETE FROM results WHERE race_id IN (SELECT race_id FROM part.races)")
            conn.execute("INSERT OR REPLACE INTO races SELECT * FROM part.races")
            conn.execute("INSERT INTO results SELECT * FROM part.results")
            conn.execute(
                "INSERT OR REPLACE INTO checked_dates SELECT * FROM part.checked_dates")
            # 取得成功したレースの失敗記録は消し、未解決の失敗だけ持ち込む
            conn.execute(
                "INSERT OR REPLACE INTO failures SELECT * FROM part.failures")
            conn.execute(
                "DELETE FROM failures WHERE race_id IN (SELECT race_id FROM races)")
        return n_races
    finally:
        conn.execute("DETACH DATABASE part")


def main():
    parts = sys.argv[1:]
    if not parts:
        print("usage: merge_db.py <part.db> [part.db ...]")
        sys.exit(1)

    conn = db.connect()
    total = 0
    for p in sorted(parts):
        n = merge_part(conn, p)
        total += n
        print(f"merged {p}: {n} races")

    n_races = conn.execute("SELECT COUNT(*) FROM races").fetchone()[0]
    n_results = conn.execute("SELECT COUNT(*) FROM results").fetchone()[0]
    n_fail = conn.execute("SELECT COUNT(*) FROM failures").fetchone()[0]
    span = conn.execute("SELECT MIN(date), MAX(date) FROM races").fetchone()
    print(f"\n=== マージ完了 ===\n"
          f"今回取り込み: {total}レース({len(parts)}パート)\n"
          f"DB累計: races={n_races} results={n_results} failures={n_fail} "
          f"期間={span[0]}〜{span[1]}")


if __name__ == "__main__":
    main()
