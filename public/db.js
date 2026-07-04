// site.db (SQLite) データ層。sql.js(WASM)で public/data/site.db を読み、
// 旧JSONと同じ形のオブジェクトを組み立てて app.js / result.js に渡す。
// 旧 {date}_結果.json は保存せず、ここで race_top3(当日) ×
// bias_rows/hot_jockeys(直近開催日) から導出する(scrape.pyの旧build_kekka移植)。
(function () {
  let db = null;

  async function open() {
    if (db) return;
    const [SQL, buf] = await Promise.all([
      initSqlJs({ locateFile: (f) => `vendor/sqljs/${f}` }),
      fetch("data/site.db").then((r) => {
        if (!r.ok) throw new Error(`${r.status} data/site.db`);
        return r.arrayBuffer();
      }),
    ]);
    db = new SQL.Database(new Uint8Array(buf));
  }

  function rows(sql, params) {
    const stmt = db.prepare(sql);
    stmt.bind(params || []);
    const out = [];
    while (stmt.step()) out.push(stmt.getAsObject());
    stmt.free();
    return out;
  }

  // ---- 一覧(旧index.json相当) ----
  function indexItems() {
    return rows(
      "SELECT date, place FROM reports ORDER BY date DESC, place ASC",
    );
  }

  // ---- 場別レポート(旧{date}_{place}.json相当) ----
  function report(date, place) {
    const rep = rows(
      "SELECT * FROM reports WHERE date = ? AND place = ?", [date, place],
    )[0];
    if (!rep) return null;

    const surfaces = {};
    for (const s of rows(
      "SELECT * FROM surface_stats WHERE date = ? AND place = ?", [date, place],
    )) {
      const bias = (kind, key) =>
        rows(
          `SELECT grp, win_rate, show_rate, n FROM bias_rows
           WHERE date = ? AND place = ? AND surface = ? AND kind = ?
           ORDER BY CASE grp WHEN '内' THEN 0 WHEN '逃げ先行' THEN 0 ELSE 1 END`,
          [date, place, s.surface, kind],
        ).map((r) => ({
          [key]: r.grp, 勝率: r.win_rate, 複勝率: r.show_rate, 出走数: r.n,
        }));
      surfaces[s.surface] = {
        race_count: s.race_count,
        frame_bias: bias("frame", "内外"),
        style_bias: bias("style", "脚質"),
        best_combo: s.best_combo_frame
          ? { 内外: s.best_combo_frame, 脚質: s.best_combo_style,
              複勝率: s.best_combo_rate, 出走数: s.best_combo_n }
          : null,
      };
    }

    const hotJockeys = rows(
      `SELECT * FROM hot_jockeys WHERE date = ? AND place = ?
       ORDER BY max_pop_diff DESC, jockey`, [date, place],
    ).map((r) => ({
      騎手: r.jockey, 最大人気差: r.max_pop_diff,
      騎乗数: r.rides, 勝利: r.wins, 複勝: r.shows,
    }));

    const races = [];
    for (const r of rows(
      `SELECT * FROM race_top3 WHERE date = ? AND place = ?
       ORDER BY race_no, rank, umaban`, [date, place],
    )) {
      let race = races[races.length - 1];
      if (!race || race.R !== r.race_no) {
        race = { R: r.race_no, surface: r.surface, race_name: r.race_name, top3: [] };
        races.push(race);
      }
      race.top3.push({
        着順: r.rank, 馬番: r.umaban, 内外: r.frame_io, 脚質: r.style, 騎手: r.jockey,
      });
    }

    const out = {
      place, date, source: rep.source, generated_at: rep.generated_at,
      total_races: rep.total_races, surfaces, hot_jockeys: hotJockeys, races,
    };

    const nr = rows(
      "SELECT * FROM notable_races WHERE date = ? AND place = ?", [date, place],
    )[0];
    if (nr) {
      out.notable_race = {
        date: nr.race_date, R: nr.race_no, race_name: nr.race_name,
        grade: nr.grade, surface: nr.surface,
        entries: rows(
          `SELECT * FROM notable_entries WHERE date = ? AND place = ?
           ORDER BY umaban`, [date, place],
        ).map((e) => ({
          枠: e.waku, 馬番: e.umaban, 馬名: e.horse, 騎手: e.jockey,
          内外: e.frame_io, 脚質: e.style,
        })),
      };
    }
    return out;
  }

  // ---- 結果ふりかえり(旧{date}_結果.json相当を導出) ----
  const FRAME_LABEL = { 内: "内枠", 外: "外枠" };

  function stripJockeyMark(name) {
    return (name || "").replace(/^[▲★△◇▽◎○☆▼]+/, "").trim();
  }

  function jockeyInHotSet(jockey, hotNames) {
    const j = stripJockeyMark(jockey);
    if (!j) return false;
    for (const hn of hotNames) {
      const hnn = stripJockeyMark(hn);
      if (!hnn) continue;
      if (hnn.startsWith(j) || j.startsWith(hnn)) return true;
    }
    return false;
  }

  function prevDate(targetDate) {
    const r = rows(
      "SELECT MAX(date) AS d FROM reports WHERE date < ?", [targetDate],
    )[0];
    return (r && r.d) || null;
  }

  function winningEntry(biasRows) {
    if (!biasRows.length) return null;
    return biasRows.sort(
      (a, b) => b.show_rate - a.show_rate || b.n - a.n,
    )[0].grp;
  }

  function kekka(targetDate) {
    const prev = prevDate(targetDate);
    if (!prev) return null;

    // 好調騎手は前開催日の全場union(騎手は土日で場を移動するため)
    const hotNames = rows(
      "SELECT DISTINCT jockey FROM hot_jockeys WHERE date = ?", [prev],
    ).map((r) => r.jockey);

    const places = [];
    for (const p of rows(
      `SELECT DISTINCT t.place FROM reports t
       JOIN reports y ON y.place = t.place AND y.date = ?
       WHERE t.date = ? ORDER BY t.place`, [prev, targetDate],
    )) {
      const place = p.place;
      const winBySurface = {};
      const surfacesCombo = {};
      for (const s of rows(
        "SELECT * FROM surface_stats WHERE date = ? AND place = ?", [prev, place],
      )) {
        winBySurface[s.surface] = {
          frame: winningEntry(rows(
            "SELECT grp, show_rate, n FROM bias_rows WHERE date=? AND place=? AND surface=? AND kind='frame'",
            [prev, place, s.surface])),
          style: winningEntry(rows(
            "SELECT grp, show_rate, n FROM bias_rows WHERE date=? AND place=? AND surface=? AND kind='style'",
            [prev, place, s.surface])),
        };
        if (s.best_combo_frame) {
          surfacesCombo[s.surface] = {
            内外: s.best_combo_frame, 脚質: s.best_combo_style,
            複勝率: s.best_combo_rate, 出走数: s.best_combo_n,
          };
        }
      }

      const races = [];
      for (const r of rows(
        `SELECT * FROM race_top3 WHERE date = ? AND place = ?
         ORDER BY race_no, rank, umaban`, [targetDate, place],
      )) {
        let race = races[races.length - 1];
        if (!race || race.R !== r.race_no) {
          race = { R: r.race_no, surface: r.surface, race_name: r.race_name, hits: [] };
          races.push(race);
        }
        const win = winBySurface[r.surface] || { frame: null, style: null };
        const labels = [];
        if (win.frame && r.frame_io === win.frame)
          labels.push(FRAME_LABEL[r.frame_io] || r.frame_io);
        if (win.style && r.style === win.style) labels.push(r.style);
        if (r.jockey && jockeyInHotSet(r.jockey, hotNames)) labels.push(r.jockey);
        race.hits.push({ 着順: r.rank, 馬番: r.umaban, labels });
      }

      places.push({ place, prev_date: prev, surfaces: surfacesCombo, races });
    }

    if (!places.length) return null;
    return { date: targetDate, prev_date: prev, places };
  }

  function kekkaDates() {
    // 前開催日が存在し、かつ同じ場のレポートが両日にある日だけが対象
    return rows(
      `SELECT DISTINCT t.date FROM reports t
       WHERE EXISTS (
         SELECT 1 FROM reports y
         WHERE y.place = t.place AND y.date < t.date
           AND y.date = (SELECT MAX(date) FROM reports WHERE date < t.date)
       )
       ORDER BY t.date DESC`,
    ).map((r) => r.date);
  }

  window.SiteDB = {
    open, indexItems, report, kekka, kekkaDates,
    stripJockeyMark, jockeyInHotSet,
  };
})();
