"""
SQLite永続化レイヤ。

生データ(レース結果・オッズ)は data/keiba.db に保存する。
集計済みJSON(public/data/)とは分離する方針(bias_analysis_spec.md参照)。
race_id をキーに冪等なUPSERTを行い、同じ日に何度実行しても重複しない。
"""

import sqlite3
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "keiba.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS races (
    race_id         TEXT PRIMARY KEY,   -- netkeiba 12桁ID
    date            TEXT NOT NULL,      -- YYYYMMDD
    place           TEXT NOT NULL,      -- 東京/京都など
    kai             INTEGER,            -- 開催回次 (IDの5-6桁目)
    nichi           INTEGER,            -- 開催日次 (IDの7-8桁目)
    race_no         INTEGER,            -- レース番号 1-12
    race_name       TEXT,
    surface         TEXT,               -- 芝/ダート/障害
    distance        INTEGER,            -- m
    turn            TEXT,               -- 右/左/直線 (障害はNULLあり)
    course_note     TEXT,               -- "外"(外回り)等の付記
    weather         TEXT,               -- 晴/曇/雨など
    track_condition TEXT,               -- 良/稍重/重/不良
    n_starters      INTEGER,            -- 出走頭数(取消・除外を除く)
    scraped_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS results (
    race_id     TEXT NOT NULL,
    umaban      INTEGER NOT NULL,       -- 馬番
    wakuban     INTEGER,                -- 枠番
    finish      INTEGER,                -- 着順 (中止/除外/取消/失格はNULL)
    finish_raw  TEXT,                   -- 着順の元表記 ("1","中","除","取","失"等)
    horse       TEXT,
    sex_age     TEXT,                   -- "牡3"等
    weight_carried REAL,                -- 斤量
    jockey      TEXT,
    time        TEXT,                   -- 走破タイム "1:33.5"
    margin      TEXT,                   -- 着差
    passing     TEXT,                   -- コーナー通過順 "3-3-2-1"
    last3f      REAL,                   -- 上り3F
    win_odds    REAL,                   -- 単勝オッズ(確定)
    popularity  INTEGER,                -- 人気
    horse_weight TEXT,                  -- 馬体重 "512(+4)"
    PRIMARY KEY (race_id, umaban)
);

-- 開催チェック済みの日付。n_races=0 は「その日は開催なし」の記録で、
-- バックフィル再開時に同じ日付のレース一覧を再取得しないためのキャッシュ。
CREATE TABLE IF NOT EXISTS checked_dates (
    date       TEXT PRIMARY KEY,
    n_races    INTEGER NOT NULL,
    checked_at TEXT
);

CREATE TABLE IF NOT EXISTS failures (
    race_id   TEXT PRIMARY KEY,
    date      TEXT,
    error     TEXT,
    failed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_races_date  ON races(date);
CREATE INDEX IF NOT EXISTS idx_races_place ON races(place, date);
"""

RACE_COLS = [
    "race_id", "date", "place", "kai", "nichi", "race_no", "race_name",
    "surface", "distance", "turn", "course_note", "weather",
    "track_condition", "n_starters", "scraped_at",
]
RESULT_COLS = [
    "race_id", "umaban", "wakuban", "finish", "finish_raw", "horse",
    "sex_age", "weight_carried", "jockey", "time", "margin", "passing",
    "last3f", "win_odds", "popularity", "horse_weight",
]


def connect(db_path=None):
    path = Path(db_path) if db_path else DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(SCHEMA)
    return conn


def _upsert(conn, table, cols, row):
    placeholders = ", ".join("?" for _ in cols)
    col_list = ", ".join(cols)
    conn.execute(
        f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})",
        [row.get(c) for c in cols],
    )


def upsert_race(conn, race, results):
    """レース1件と馬別結果をまとめてUPSERT。成功したらfailuresから消す。"""
    with conn:
        _upsert(conn, "races", RACE_COLS, race)
        # 再取得時に頭数が変わった場合(除外反映等)の残骸を消す
        conn.execute("DELETE FROM results WHERE race_id = ?", (race["race_id"],))
        for r in results:
            _upsert(conn, "results", RESULT_COLS, r)
        conn.execute("DELETE FROM failures WHERE race_id = ?", (race["race_id"],))


def record_failure(conn, race_id, date, error):
    with conn:
        _upsert(conn, "failures", ["race_id", "date", "error", "failed_at"], {
            "race_id": race_id,
            "date": date,
            "error": str(error)[:300],
            "failed_at": datetime.now().isoformat(timespec="seconds"),
        })


def existing_race_ids(conn):
    """既に取得済みのrace_id集合(再開・スキップ判定用)。"""
    return {row[0] for row in conn.execute("SELECT race_id FROM races")}


def dates_with_races(conn):
    """レースが保存済みの日付集合。"""
    return {row[0] for row in conn.execute("SELECT DISTINCT date FROM races")}


def mark_date_checked(conn, date, n_races):
    with conn:
        _upsert(conn, "checked_dates", ["date", "n_races", "checked_at"], {
            "date": date,
            "n_races": n_races,
            "checked_at": datetime.now().isoformat(timespec="seconds"),
        })


def checked_dates(conn):
    """{date: n_races} 済みチェックのキャッシュ。"""
    return dict(conn.execute("SELECT date, n_races FROM checked_dates"))


def races_count_by_date(conn):
    """{date: 保存済みレース数}"""
    return dict(conn.execute("SELECT date, COUNT(*) FROM races GROUP BY date"))
