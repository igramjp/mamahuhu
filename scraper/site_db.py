"""
サイト配信用SQLite (public/data/site.db)。

集計済みデータだけを持つ小さなDB。ブラウザがsql.js(WASM)で丸ごと読む。
生データは data/keiba.db(非公開)、こちらは表示に必要なテーブルのみ。

旧 public/data/{date}_{place}.json の内容を正規化したもの:
- reports:         1ファイル ≙ 1行(date, place, source, ...)
- surface_stats:   surfaces{} の race_count / best_combo
- bias_rows:       frame_bias / style_bias の行
- combo_cells:     combo_matrix(内外×脚質の4マス)
- hot_jockeys:     好調騎手
- race_top3:       各レース上位3頭(同着があるためPKにumaban含む)
- notable_races/notable_entries: 注目レース(次開催メイン)の出馬表

旧 {date}_結果.json はここに持たない: 中身は race_top3(当日) ×
bias_rows/hot_jockeys(直近開催日) から導出できるため、フロント側で計算する。
"""

import json
import re
import sqlite3
from pathlib import Path

SITE_DB_PATH = Path(__file__).resolve().parent.parent / "public" / "data" / "site.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS reports (
    date         TEXT NOT NULL,
    place        TEXT NOT NULL,
    source       TEXT,
    generated_at TEXT,
    total_races  INTEGER,
    PRIMARY KEY (date, place)
);

CREATE TABLE IF NOT EXISTS surface_stats (
    date TEXT NOT NULL, place TEXT NOT NULL, surface TEXT NOT NULL,
    race_count INTEGER,
    best_combo_frame TEXT,   -- 内/外 (best_comboが無い日はNULL)
    best_combo_style TEXT,   -- 逃げ先行/差し追込
    best_combo_rate  REAL,
    best_combo_n     INTEGER,
    PRIMARY KEY (date, place, surface)
);

CREATE TABLE IF NOT EXISTS bias_rows (
    date TEXT NOT NULL, place TEXT NOT NULL, surface TEXT NOT NULL,
    kind TEXT NOT NULL,      -- 'frame' | 'style'
    grp  TEXT NOT NULL,      -- 内/外/逃げ先行/差し追込
    win_rate  REAL,
    show_rate REAL,
    n         INTEGER,
    PRIMARY KEY (date, place, surface, kind, grp)
);

CREATE TABLE IF NOT EXISTS combo_cells (
    date TEXT NOT NULL, place TEXT NOT NULL, surface TEXT NOT NULL,
    frame TEXT NOT NULL, style TEXT NOT NULL,
    show_rate REAL,
    n         INTEGER,
    PRIMARY KEY (date, place, surface, frame, style)
);

CREATE TABLE IF NOT EXISTS hot_jockeys (
    date TEXT NOT NULL, place TEXT NOT NULL, jockey TEXT NOT NULL,
    max_pop_diff REAL,
    rides INTEGER, wins INTEGER, shows INTEGER,
    PRIMARY KEY (date, place, jockey)
);

CREATE TABLE IF NOT EXISTS race_top3 (
    date TEXT NOT NULL, place TEXT NOT NULL, race_no INTEGER NOT NULL,
    surface TEXT, race_name TEXT,
    rank INTEGER NOT NULL,
    umaban INTEGER,
    frame_io TEXT,           -- 内/外
    style    TEXT,
    jockey   TEXT,
    PRIMARY KEY (date, place, race_no, rank, umaban)
);

CREATE TABLE IF NOT EXISTS notable_races (
    date TEXT NOT NULL, place TEXT NOT NULL,   -- 紐づくレポートのキー
    race_date TEXT, race_no INTEGER,
    race_name TEXT, grade TEXT, surface TEXT,
    PRIMARY KEY (date, place)
);

CREATE TABLE IF NOT EXISTS notable_entries (
    date TEXT NOT NULL, place TEXT NOT NULL, umaban INTEGER NOT NULL,
    waku INTEGER, horse TEXT, jockey TEXT,
    frame_io TEXT, style TEXT,
    PRIMARY KEY (date, place, umaban)
);
"""

TABLES = ["reports", "surface_stats", "bias_rows", "combo_cells",
          "hot_jockeys", "race_top3", "notable_races", "notable_entries"]


def connect(db_path=None):
    path = Path(db_path) if db_path else SITE_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    return conn


def _delete_report(conn, date, place):
    for t in TABLES:
        conn.execute(f"DELETE FROM {t} WHERE date = ? AND place = ?", (date, place))


def write_report(conn, data):
    """analyze_to_dict が返す dict(旧JSONと同形)を site.db に書き込む。
    同じ (date, place) は総入れ替えで冪等。"""
    date, place = data["date"], data["place"]
    with conn:
        _delete_report(conn, date, place)
        conn.execute(
            "INSERT INTO reports (date, place, source, generated_at, total_races)"
            " VALUES (?,?,?,?,?)",
            (date, place, data.get("source"), data.get("generated_at"),
             data.get("total_races")))

        for surface, s in (data.get("surfaces") or {}).items():
            bc = s.get("best_combo") or {}
            conn.execute(
                "INSERT INTO surface_stats VALUES (?,?,?,?,?,?,?,?)",
                (date, place, surface, s.get("race_count"),
                 bc.get("内外"), bc.get("脚質"), bc.get("複勝率"), bc.get("出走数")))
            for kind, key, rows in [("frame", "内外", s.get("frame_bias") or []),
                                    ("style", "脚質", s.get("style_bias") or [])]:
                for r in rows:
                    conn.execute(
                        "INSERT INTO bias_rows VALUES (?,?,?,?,?,?,?,?)",
                        (date, place, surface, kind, r[key],
                         r.get("勝率"), r.get("複勝率"), r.get("出走数")))
            for c in s.get("combo_matrix") or []:
                conn.execute(
                    "INSERT INTO combo_cells VALUES (?,?,?,?,?,?,?)",
                    (date, place, surface, c["内外"], c["脚質"],
                     c.get("複勝率"), c.get("出走数")))

        for j in data.get("hot_jockeys") or []:
            conn.execute(
                "INSERT INTO hot_jockeys VALUES (?,?,?,?,?,?,?)",
                (date, place, j["騎手"], j.get("最大人気差"),
                 j.get("騎乗数"), j.get("勝利"), j.get("複勝")))

        for race in data.get("races") or []:
            for h in race.get("top3") or []:
                conn.execute(
                    "INSERT INTO race_top3 VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (date, place, race["R"], race.get("surface"),
                     race.get("race_name"), h["着順"], h.get("馬番"),
                     h.get("内外"), h.get("脚質"), h.get("騎手")))

        write_notable(conn, date, place, data.get("notable_race"), _in_txn=True)


def write_notable(conn, date, place, nr, _in_txn=False):
    """notable_race だけを書き換える(金曜の埋め直し用)。"""
    def _do():
        conn.execute("DELETE FROM notable_races WHERE date=? AND place=?", (date, place))
        conn.execute("DELETE FROM notable_entries WHERE date=? AND place=?", (date, place))
        if not nr:
            return
        conn.execute(
            "INSERT INTO notable_races VALUES (?,?,?,?,?,?,?)",
            (date, place, nr.get("date"), nr.get("R"), nr.get("race_name"),
             nr.get("grade"), nr.get("surface")))
        for e in nr.get("entries") or []:
            conn.execute(
                "INSERT INTO notable_entries VALUES (?,?,?,?,?,?,?,?)",
                (date, place, e["馬番"], e.get("枠"), e.get("馬名"),
                 e.get("騎手"), e.get("内外"), e.get("脚質")))
    if _in_txn:
        _do()
    else:
        with conn:
            _do()


def touch_generated_at(conn, date, place, generated_at):
    with conn:
        conn.execute("UPDATE reports SET generated_at=? WHERE date=? AND place=?",
                     (generated_at, date, place))


# ---------- 旧JSONからの一括移行 ----------
def import_legacy_json(conn, data_dir):
    """public/data/{date}_{place}.json を全件取り込む。
    index.json / {date}_結果.json は導出可能なので対象外。"""
    n = 0
    for f in sorted(Path(data_dir).glob("*.json")):
        m = re.match(r"(\d{8})_(.+)\.json", f.name)
        if not m or m.group(2) == "結果":
            continue
        data = json.loads(f.read_text(encoding="utf-8"))
        write_report(conn, data)
        n += 1
    return n


if __name__ == "__main__":
    import sys
    data_dir = sys.argv[1] if len(sys.argv) > 1 else str(SITE_DB_PATH.parent)
    conn = connect()
    n = import_legacy_json(conn, data_dir)
    print(f"取り込み: {n}ファイル → {SITE_DB_PATH}")
    for t in TABLES:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t}: {cnt}行")
