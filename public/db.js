// site.db (SQLite) データ層。sql.js(WASM)で public/data/site.db を読む。
// すべて新集計(bias3 = 相対枠位置3分割×正規化着順のdeviation)ベース。
// 結果検証は pred_horses.rank(確定着順)から導出する。
(function () {
  let db = null;

  // gzip版を優先して取得(Netlifyはバイナリを自動圧縮しないため)。
  // DecompressionStream非対応・取得失敗時は生のsite.dbへフォールバック。
  async function fetchDb() {
    if (typeof DecompressionStream === "function") {
      try {
        const r = await fetch("data/site.db.gz");
        if (r.ok && r.body) {
          const stream = r.body.pipeThrough(new DecompressionStream("gzip"));
          return await new Response(stream).arrayBuffer();
        }
      } catch (e) { /* フォールバックへ */ }
    }
    const r = await fetch("data/site.db");
    if (!r.ok) throw new Error(`${r.status} data/site.db`);
    return r.arrayBuffer();
  }

  async function open() {
    if (db) return;
    const [SQL, buf] = await Promise.all([
      initSqlJs({ locateFile: (f) => `vendor/sqljs/${f}` }),
      fetchDb(),
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

  // ---- 開催インデックス ----
  function indexItems() {
    return rows(
      "SELECT date, place FROM reports ORDER BY date DESC, place ASC",
    );
  }

  // ---- 場別レポート(top3・注目レース) ----
  function report(date, place) {
    const rep = rows(
      "SELECT * FROM reports WHERE date = ? AND place = ?", [date, place],
    )[0];
    if (!rep) return null;

    const out = {
      place, date, source: rep.source, generated_at: rep.generated_at,
      total_races: rep.total_races,
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
          枠: e.waku, 馬番: e.umaban, 馬名: e.horse, 騎手: e.jockey, 脚質: e.style,
        })),
      };
    }
    return out;
  }

  // ---- 新バイアス集計(bias3_*) ----
  // 戻り値: {surfaces: {芝: {frame: {groups, meta}, style: {groups, meta},
  //          byDistance: {短距離: {frame: {groups}, style: {groups}}, ...}}}, notes:[...]}
  // frame/style 直下は当日全体(dist_cat='ALL')の推定。
  const GRP_ORDER = { 内: 0, 中: 1, 外: 2, 逃げ先行: 0, 差し追込: 1 };
  const DIST_ORDER = { 短距離: 0, "マイル〜中距離": 1, 長距離: 2 };

  function bias3(date, place) {
    const stats = rows(
      `SELECT * FROM bias3_stats WHERE date = ? AND place = ?`, [date, place]);
    if (!stats.length) return null;
    const surfaces = {};
    for (const r of stats) {
      const s = (surfaces[r.surface] ||= { byDistance: {} });
      const k = r.kind === "frame3" ? "frame" : "style";
      const holder = r.dist_cat === "ALL"
        ? s
        : (s.byDistance[r.dist_cat] ||= {});
      const block = (holder[k] ||= { groups: [], meta: null });
      block.groups.push({
        grp: r.grp, recent: r.recent_delta, baseline: r.baseline_delta,
        adjusted: r.adjusted_delta, deviation: r.deviation,
        se: r.dev_se, n: r.n,
      });
    }
    for (const m of rows(
      `SELECT * FROM bias3_meta WHERE date = ? AND place = ?`, [date, place])) {
      const k = m.kind === "frame3" ? "frame" : "style";
      if (surfaces[m.surface] && surfaces[m.surface][k]) {
        surfaces[m.surface][k].meta = m;
      }
    }
    for (const s of Object.values(surfaces)) {
      for (const holder of [s, ...Object.values(s.byDistance)]) {
        for (const k of ["frame", "style"]) {
          if (holder[k]) holder[k].groups.sort((a, b) => GRP_ORDER[a.grp] - GRP_ORDER[b.grp]);
        }
      }
    }
    const notes = rows(
      `SELECT note FROM bias3_notes WHERE date = ? AND place = ? ORDER BY seq`,
      [date, place]).map((r) => r.note);
    return { surfaces, notes };
  }

  // 「来てる」グループ: deviationが最も負で、閾値を超えているもの。
  // 馬場よみのヘッドライン・結果の答え合わせ・注目レースのchipで共通に使う。
  const DEV_THRESHOLD = 0.01;
  const MIN_N = 8;

  function favoredGroup(block) {
    if (!block || !block.groups) return null;
    const cands = block.groups.filter(
      (g) => (g.n || 0) >= MIN_N && (g.deviation ?? 0) <= -DEV_THRESHOLD);
    if (!cands.length) return null;
    return cands.reduce((a, b) => (b.deviation < a.deviation ? b : a)).grp;
  }

  // ---- 結果検証(推奨馬の単勝的中と回収率を導出) ----
  // pred_horses.rank(確定着順)が入っている日が検証可能。
  // 推奨馬の的中 = rank=1(単勝ベット)。2-3着は参考情報。
  function verifyDates() {
    // 結果(確定着順)が入っている日 = 検証可能。全レース見送りの日も
    // 「見送り判断の記録」として表示対象に含める。
    return rows(
      `SELECT DISTINCT date FROM pred_horses
       WHERE rank IS NOT NULL
       ORDER BY date DESC`,
    ).map((r) => r.date);
  }

  function verify(targetDate) {
    const places = [];
    const total = { n_races: 0, n_reco_races: 0, n_reco: 0, n_hit: 0, payout: 0,
                    n_attn: 0, n_attn_hit: 0, attn_payout: 0 };

    for (const p of rows(
      `SELECT DISTINCT place FROM pred_races WHERE date = ? ORDER BY place`,
      [targetDate],
    )) {
      const place = p.place;
      const sum = { n_races: 0, n_reco_races: 0, n_reco: 0, n_hit: 0, payout: 0,
                    n_attn: 0, n_attn_hit: 0, attn_payout: 0 };
      const races = [];
      for (const r of rows(
        `SELECT * FROM pred_races WHERE date = ? AND place = ? ORDER BY race_no`,
        [targetDate, place],
      )) {
        sum.n_races++;
        const race = {
          race_no: r.race_no, race_name: r.race_name,
          surface: r.surface, distance: r.distance,
          verdict: r.verdict, horses: [],
        };
        if (r.verdict === "推奨") {
          sum.n_reco_races++;
          for (const h of rows(
            `SELECT * FROM pred_horses
             WHERE date = ? AND place = ? AND race_no = ? AND recommended = 1
             ORDER BY ev DESC, umaban`, [targetDate, place, r.race_no],
          )) {
            const hit = h.rank === 1;
            sum.n_reco++;
            if (hit) {
              sum.n_hit++;
              sum.payout += h.odds || 0;
            }
            race.horses.push({
              umaban: h.umaban, horse: h.horse, odds: h.odds,
              ev: h.ev, edge: h.edge, rank: h.rank, hit, kind: "推奨",
            });
          }
        }
        // 注目馬(推奨とは別ティア): 見送りレースにも付きうる
        for (const h of rows(
          `SELECT * FROM pred_horses
           WHERE date = ? AND place = ? AND race_no = ?
             AND attention = 1 AND recommended = 0`,
          [targetDate, place, r.race_no],
        )) {
          const hit = h.rank === 1;
          sum.n_attn++;
          if (hit) {
            sum.n_attn_hit++;
            sum.attn_payout += h.odds || 0;
          }
          race.horses.push({
            umaban: h.umaban, horse: h.horse, odds: h.odds,
            ev: h.ev, edge: h.edge, rank: h.rank, hit, kind: "注目",
          });
        }
        races.push(race);
      }
      for (const k of Object.keys(total)) total[k] += sum[k];
      places.push({ place, races, summary: sum });
    }

    if (!places.length) return null;
    // 単勝回収率(%): 100円均等買い想定 = Σ(的中オッズ)/頭数 × 100
    const withRoi = (s) => {
      s.roi = s.n_reco ? (s.payout / s.n_reco) * 100 : null;
      s.attn_roi = s.n_attn ? (s.attn_payout / s.n_attn) * 100 : null;
      return s;
    };
    withRoi(total);
    for (const pl of places) withRoi(pl.summary);
    return { date: targetDate, places, total };
  }

  // ---- 予想(pred_*) ----
  function predItems() {
    return rows(
      "SELECT DISTINCT date, place FROM pred_races ORDER BY date DESC, place ASC");
  }

  // 累計統計: 「歪みは見つかっているか」を数字で示す(現実セクション用)。
  // 確定オッズ版(forward=0)のみ対象。performance系はrank判明分のみ。
  function predStats() {
    const total = rows(
      `SELECT COUNT(*) n_races,
              COALESCE(SUM(verdict = '推奨'), 0) n_reco_races,
              MIN(date) date_from, MAX(date) date_to,
              COUNT(DISTINCT date) n_days
       FROM pred_races WHERE forward = 0`)[0];
    const attn = rows(
      `SELECT COUNT(*) n, COALESCE(SUM(rank = 1), 0) hits,
              COALESCE(SUM(CASE WHEN rank = 1 THEN odds ELSE 0 END), 0) payout
       FROM pred_horses WHERE attention = 1 AND rank IS NOT NULL`)[0];
    const reco = rows(
      `SELECT COUNT(*) n, COALESCE(SUM(rank = 1), 0) hits,
              COALESCE(SUM(CASE WHEN rank = 1 THEN odds ELSE 0 END), 0) payout
       FROM pred_horses WHERE recommended = 1 AND rank IS NOT NULL`)[0];
    const roi = (s) => (s.n ? (s.payout / s.n) * 100 : null);
    return {
      total,
      attn: { ...attn, roi: roi(attn) },
      reco: { ...reco, roi: roi(reco) },
    };
  }

  // 最新の予想日の推奨レース(バイアスページの「期待値分析との接続」用)
  function latestRecoRaces() {
    const d = rows("SELECT MAX(date) d FROM pred_races")[0];
    if (!d || !d.d) return { date: null, races: [] };
    const races = rows(
      `SELECT * FROM pred_races WHERE date = ? AND verdict = '推奨'
       ORDER BY place, race_no`, [d.d]);
    for (const r of races) {
      r.horses = rows(
        `SELECT * FROM pred_horses
         WHERE date = ? AND place = ? AND race_no = ? AND recommended = 1
         ORDER BY ev DESC`, [d.d, r.place, r.race_no]);
    }
    return { date: d.d, races };
  }

  function predictions(date, place) {
    const races = rows(
      `SELECT * FROM pred_races WHERE date = ? AND place = ? ORDER BY race_no`,
      [date, place]);
    if (!races.length) return null;
    for (const r of races) {
      r.horses = rows(
        `SELECT * FROM pred_horses WHERE date = ? AND place = ? AND race_no = ?
         ORDER BY attention DESC, recommended DESC, edge DESC, model_prob DESC,
                  umaban`, [date, place, r.race_no]);
    }
    return races;
  }

  window.SiteDB = {
    open, indexItems, report,
    bias3, favoredGroup, predItems, predictions,
    predStats, latestRecoRaces,
    verify, verifyDates,
  };
})();
