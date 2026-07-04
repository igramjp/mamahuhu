"""
サイト配信用SQLite (public/data/site.db)。

集計済みデータだけを持つ小さなDB。ブラウザがsql.js(WASM)で丸ごと読む。
生データは data/keiba.db(非公開)、こちらは表示に必要なテーブルのみ。
すべて data/keiba.db(または当日スクレイプ)から再生成できる派生データ。

- reports:         開催日×場のインデックス(date, place, source, ...)
- notable_races/notable_entries: 注目レース(次開催メイン)の出馬表
- bias3_*:         新バイアス集計(相対枠位置3分割×正規化着順、deviation)
- pred_*:          期待値ベース予想。pred_horses は表示に使う行のみ
                   (推奨馬 + レース内EV上位3頭)。rank列に確定着順を持ち、
                   結果検証はこの列だけで完結する

サイズ注意: site.db はブラウザが丸ごとダウンロードするため、
表示に使わない行は保存しない。
"""

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

CREATE TABLE IF NOT EXISTS notable_races (
    date TEXT NOT NULL, place TEXT NOT NULL,   -- 紐づくレポートのキー
    race_date TEXT, race_no INTEGER,
    race_name TEXT, grade TEXT, surface TEXT,
    PRIMARY KEY (date, place)
);

CREATE TABLE IF NOT EXISTS notable_entries (
    date TEXT NOT NULL, place TEXT NOT NULL, umaban INTEGER NOT NULL,
    waku INTEGER, horse TEXT, jockey TEXT,
    style TEXT,              -- 前走脚質。相対枠位置は表示側で頭数から計算
    PRIMARY KEY (date, place, umaban)
);

-- ===== 新バイアス集計 (bias.py / bias_analysis_spec.md) =====
-- 相対枠位置3分割(内/中/外)×正規化着順。deviation = ベースライン乖離が主役指標。
CREATE TABLE IF NOT EXISTS bias3_stats (
    date TEXT NOT NULL, place TEXT NOT NULL, surface TEXT NOT NULL,
    dist_cat TEXT NOT NULL DEFAULT 'ALL',  -- 'ALL' | 短距離 | マイル〜中距離 | 長距離
    kind TEXT NOT NULL,          -- 'frame3' | 'style'
    grp  TEXT NOT NULL,          -- 内/中/外/逃げ先行/差し追込
    recent_delta   REAL,         -- 直近開催日の正規化着順delta(負=先着傾向)
    baseline_delta REAL,         -- 長期ベースライン(2-3年)の同delta
    adjusted_delta REAL,         -- 縮小推定後の値
    deviation      REAL,         -- adjusted - baseline (負=ベースラインより有利)
    dev_se         REAL,         -- deviation の標準誤差(近似)
    n              INTEGER,      -- 直近サンプル頭数
    PRIMARY KEY (date, place, surface, dist_cat, kind, grp)
);

CREATE TABLE IF NOT EXISTS bias3_meta (
    date TEXT NOT NULL, place TEXT NOT NULL, surface TEXT NOT NULL,
    kind TEXT NOT NULL,          -- 'frame3' | 'style'
    favor_label  TEXT,           -- 確度計算の基準グループ(内/逃げ先行)
    favor_races  INTEGER,        -- 基準グループが有利だったレース数
    total_races  INTEGER,        -- 比較可能だったレース数
    race_count   INTEGER,        -- 当日の対象レース数(表示用)
    n_horses     INTEGER,        -- 当日のサンプル頭数
    baseline_n   INTEGER,        -- ベースライン頭数
    baseline_level TEXT,         -- 使用した階層の内訳(表示用文字列)
    PRIMARY KEY (date, place, surface, kind)
);

CREATE TABLE IF NOT EXISTS bias3_notes (
    date TEXT NOT NULL, place TEXT NOT NULL, seq INTEGER NOT NULL,
    note TEXT NOT NULL,          -- 例: コース替わり初日でリセットの可能性
    PRIMARY KEY (date, place, seq)
);

-- ===== 予想 (期待値ベース / Benter型) =====
CREATE TABLE IF NOT EXISTS pred_races (
    date TEXT NOT NULL, place TEXT NOT NULL, race_no INTEGER NOT NULL,
    race_name TEXT, surface TEXT, distance INTEGER,
    verdict TEXT NOT NULL,       -- '推奨' | '見送り'
    model_version TEXT,          -- 例: 'proto-0(デモ・未学習)'
    PRIMARY KEY (date, place, race_no)
);

CREATE TABLE IF NOT EXISTS pred_horses (
    date TEXT NOT NULL, place TEXT NOT NULL, race_no INTEGER NOT NULL,
    umaban INTEGER NOT NULL,
    horse TEXT, jockey TEXT,
    odds REAL,                   -- 単勝オッズ
    market_prob REAL,            -- オッズ由来の市場確率(レース内正規化)
    model_prob  REAL,            -- モデル確率
    ev          REAL,            -- 期待値 = model_prob × odds
    recommended INTEGER NOT NULL DEFAULT 0,
    rank        INTEGER,         -- 確定着順(結果判明後。未確定はNULL)
    PRIMARY KEY (date, place, race_no, umaban)
);
"""

TABLES = ["reports", "notable_races", "notable_entries",
          "bias3_stats", "bias3_meta", "bias3_notes",
          "pred_races", "pred_horses"]


def connect(db_path=None):
    path = Path(db_path) if db_path else SITE_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    return conn


# write_report が総入れ替えする対象。bias3_*/pred_* は
# それぞれの writer (write_bias3 / write_predictions) が独自に入れ替える。
REPORT_TABLES = ["reports", "notable_races", "notable_entries"]


def _delete_report(conn, date, place):
    for t in REPORT_TABLES:
        conn.execute(f"DELETE FROM {t} WHERE date = ? AND place = ?", (date, place))


def write_report(conn, data):
    """analyze_to_dict が返す dict を site.db に書き込む。
    同じ (date, place) は総入れ替えで冪等。"""
    date, place = data["date"], data["place"]
    with conn:
        _delete_report(conn, date, place)
        conn.execute(
            "INSERT INTO reports (date, place, source, generated_at, total_races)"
            " VALUES (?,?,?,?,?)",
            (date, place, data.get("source"), data.get("generated_at"),
             data.get("total_races")))

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
                "INSERT INTO notable_entries VALUES (?,?,?,?,?,?,?)",
                (date, place, e["馬番"], e.get("枠"), e.get("馬名"),
                 e.get("騎手"), e.get("脚質")))
    if _in_txn:
        _do()
    else:
        with conn:
            _do()


def touch_generated_at(conn, date, place, generated_at):
    with conn:
        conn.execute("UPDATE reports SET generated_at=? WHERE date=? AND place=?",
                     (generated_at, date, place))


def write_bias3(conn, report):
    """bias.py build_bias_report の dict を bias3_* テーブルへ書き込む(冪等)。
    dist_cat='ALL' が当日全体、距離カテゴリ別は by_distance から。"""
    date, place = report["date"], report["place"]

    def insert_groups(surface, dist_cat, block_holder):
        for kind, key in (("frame3", "frame_bias"), ("style", "style_bias")):
            block = block_holder.get(key)
            if not block:
                continue
            for g in block["groups"]:
                conn.execute(
                    "INSERT INTO bias3_stats VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (date, place, surface, dist_cat, kind, g["group"],
                     g.get("delta"), g.get("baseline_delta"),
                     g.get("adjusted_delta"), g.get("deviation"),
                     g.get("dev_se"), g.get("n")))

    with conn:
        for t in ("bias3_stats", "bias3_meta", "bias3_notes"):
            conn.execute(f"DELETE FROM {t} WHERE date = ? AND place = ?", (date, place))
        for surface, s in (report.get("surfaces") or {}).items():
            insert_groups(surface, "ALL", s)
            for cat, cs in (s.get("by_distance") or {}).items():
                insert_groups(surface, cat, cs)
            for kind, key in (("frame3", "frame_bias"), ("style", "style_bias")):
                block = s.get(key)
                if not block:
                    continue
                conf = block.get("confidence") or {}
                levels = block.get("baseline_levels") or {}
                level_str = "、".join(f"{k}:{v}R" for k, v in levels.items()) or None
                conn.execute(
                    "INSERT INTO bias3_meta VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (date, place, surface, kind,
                     conf.get("favor_label"), conf.get("favor_races"),
                     conf.get("total_races"), s.get("race_count"),
                     s.get("n_horses"), s.get("baseline_n"), level_str))
        for i, note in enumerate(report.get("notes") or []):
            conn.execute("INSERT INTO bias3_notes VALUES (?,?,?,?)",
                         (date, place, i, note))


def write_predictions(conn, date, place, races):
    """予想を pred_* テーブルへ書き込む(冪等)。
    races: [{race_no, race_name, surface, distance, verdict, model_version,
             horses: [{umaban, horse, jockey, odds, market_prob, model_prob,
                       ev, recommended}]}]"""
    with conn:
        for t in ("pred_races", "pred_horses"):
            conn.execute(f"DELETE FROM {t} WHERE date = ? AND place = ?", (date, place))
        for r in races:
            conn.execute(
                "INSERT INTO pred_races VALUES (?,?,?,?,?,?,?,?)",
                (date, place, r["race_no"], r.get("race_name"), r.get("surface"),
                 r.get("distance"), r["verdict"], r.get("model_version")))
            for h in r.get("horses") or []:
                conn.execute(
                    "INSERT INTO pred_horses VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    (date, place, r["race_no"], h["umaban"], h.get("horse"),
                     h.get("jockey"), h.get("odds"), h.get("market_prob"),
                     h.get("model_prob"), h.get("ev"),
                     1 if h.get("recommended") else 0, h.get("rank")))


if __name__ == "__main__":
    conn = connect()
    for t in TABLES:
        cnt = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"  {t}: {cnt}行")
