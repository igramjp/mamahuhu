// トラックバイアス分析 - frontend renderer(全場スクロール表示)
const $ = (s, r = document) => r.querySelector(s);

const SURFACE_META = {
  芝: { cls: "" },
  ダート: { cls: "dirt" },
};
const DIST_CATS = ["短距離", "マイル〜中距離", "長距離"];

// ---- 統計ヘルパー ----
const DEV_MAX = 0.08; // メーター両端のスケール(正規化着順の乖離)

// 有意判定: |Δ| > 1.96×SE(ベースライン既知とみなす近似・95%水準)
function isSignificant(g) {
  return g.se != null && Math.abs(g.deviation ?? 0) > 1.96 * g.se;
}

// 二項検定(両側, p=0.5)。favor_races/total_races の偏りの偶然性評価
function binomTestP(k, n) {
  if (!n) return null;
  const comb = (n, k) => {
    let r = 1;
    for (let i = 0; i < k; i++) r = (r * (n - i)) / (i + 1);
    return r;
  };
  let lo = 0, hi = 0;
  for (let i = 0; i <= n; i++) {
    const p = comb(n, i) * Math.pow(0.5, n);
    if (i <= k) lo += p;
    if (i >= k) hi += p;
  }
  return Math.min(1, 2 * Math.min(lo, hi));
}

function fmtDelta(v) {
  if (v == null) return "—";
  return (v < 0 ? "−" : "+") + Math.abs(v).toFixed(3);
}

// 乖離の言語化(強度は|Δ|の区分、方向は符号)
function devLabel(g) {
  const dev = g.deviation ?? 0;
  const a = Math.abs(dev);
  if (a < 0.01) return { word: "中立", side: "flat" };
  const strength = a < 0.03 ? "弱" : a < 0.06 ? "中" : "強";
  return dev < 0
    ? { word: `有利化(${strength})`, side: "plus" }
    : { word: `不利化(${strength})`, side: "minus" };
}

// ---- 描画: 乖離メーター ----
function devMeterRow(g) {
  const dev = g.deviation ?? 0;
  const adv = -dev; // 右 = ベースラインより有利
  const w = (Math.min(Math.abs(adv), DEV_MAX) / DEV_MAX) * 50;
  const { word, side } = devLabel(g);
  const sig = isSignificant(g);
  const barStyle =
    adv >= 0 ? `left:50%;width:${w}%` : `left:${50 - w}%;width:${w}%`;
  return `<div class="dev-row">
    <span class="dev-label">${g.grp}</span>
    <div class="dev-track"><span class="dev-center"></span><span class="dev-bar ${side}" style="${barStyle}"></span></div>
    <span class="dev-word ${side}">${word}<small class="dev-num">Δ${fmtDelta(dev)}${sig ? "<b>*</b>" : ""} n=${g.n}</small></span>
  </div>`;
}

// ヘッドライン: 最大|Δ|のグループの状態を文章化
function verdictHtml(frameBlock) {
  const cands = (frameBlock.groups || []).filter((g) => (g.n || 0) >= 8);
  if (!cands.length) return `<p class="yomi-verdict">標本不足(n&lt;8)</p>`;
  const top = cands.reduce((a, b) =>
    Math.abs(b.deviation ?? 0) > Math.abs(a.deviation ?? 0) ? b : a);
  const dev = top.deviation ?? 0;
  if (Math.abs(dev) < 0.01) {
    return `<p class="yomi-verdict">ベースラインからの乖離なし</p>`;
  }
  const dir = dev < 0 ? "有利化" : "不利化";
  const sig = isSignificant(top) ? "有意に" : "";
  return `<p class="yomi-verdict"><b>${top.grp}枠</b>グループが${sig}${dir}
    <small class="yomi-verdict-stat">Δ=${fmtDelta(dev)}${isSignificant(top) ? " (p&lt;0.05)" : " (n.s.)"}</small></p>`;
}

function confidenceLine(meta) {
  if (!meta || !meta.total_races) return "";
  const p = binomTestP(meta.favor_races, meta.total_races);
  return `<p class="dev-confidence">レース内比較: ${meta.total_races}R中 <b>${meta.favor_races}</b>Rで${meta.favor_label}側グループが優位(二項検定 p=${p == null ? "—" : p.toFixed(2)})</p>`;
}

// ---- 描画: 距離カテゴリ別テーブル ----
function distTable(byDistance) {
  const cats = DIST_CATS.filter((c) => byDistance[c]);
  if (!cats.length) return "";
  const cell = (holder, kind, grp) => {
    const block = holder[kind];
    const g = block && block.groups.find((x) => x.grp === grp);
    if (!g) return '<td class="num-cell">—</td>';
    const { side } = devLabel(g);
    return `<td class="num-cell dist-${side}">${fmtDelta(g.deviation)}${isSignificant(g) ? "<b>*</b>" : ""}</td>`;
  };
  let rows = "";
  for (const cat of cats) {
    const h = byDistance[cat];
    rows += `<tr>
      <td class="dist-cat-cell">${cat}<small> ${h.frame && h.frame.groups[0] ? sumN(h.frame.groups) : "-"}頭</small></td>
      ${cell(h, "frame", "内")}${cell(h, "frame", "中")}${cell(h, "frame", "外")}
      ${cell(h, "style", "逃げ先行")}${cell(h, "style", "差し追込")}
    </tr>`;
  }
  return `<h3 class="sub-head">距離カテゴリ別の乖離Δ</h3>
    <table class="data-table dist-table"><thead><tr>
      <th>距離帯</th><th>内</th><th>中</th><th>外</th><th>逃先</th><th>差追</th>
    </tr></thead><tbody>${rows}</tbody></table>
    <p class="yomi-foot-min">距離帯別は標本が小さく縮小推定が強く効くため、保守的(ベースライン寄り)な値になる。</p>`;
}

function sumN(groups) {
  return groups.reduce((a, g) => a + (g.n || 0), 0);
}

// ---- 描画: 1つの場×コース種別 ----
function surfaceBlockHtml(place, surface, b3s, notes) {
  let html = `<div class="surface-block">`;

  if (b3s.frame) {
    html += verdictHtml(b3s.frame);
    html += `<h3 class="sub-head">枠順バイアス <small class="yomi-scale">← 不利化 ｜ ベースライン ｜ 有利化 →</small></h3>
      <div class="dev-meter">${b3s.frame.groups.map(devMeterRow).join("")}</div>
      ${confidenceLine(b3s.frame.meta)}`;
  }
  if (b3s.style) {
    html += `<h3 class="sub-head">脚質バイアス</h3>
      <div class="dev-meter">${b3s.style.groups.map(devMeterRow).join("")}</div>
      ${confidenceLine(b3s.style.meta)}`;
  }

  html += distTable(b3s.byDistance || {});

  const m = b3s.frame && b3s.frame.meta;
  html += `<p class="yomi-foot">指標: 正規化着順(着順÷出走頭数)のグループ平均とレース期待値の差。
    Δは過去2〜3年のベースライン(馬場状態×距離帯で層別${m && m.baseline_n ? `、n=${m.baseline_n.toLocaleString()}` : ""})への縮小推定後の乖離。
    枠は馬番÷頭数による相対位置の3分位。* は近似95%水準で有意。
    対象: ${m && m.race_count ? m.race_count : "-"}R / ${m && m.n_horses ? m.n_horses : "-"}頭。</p>`;

  for (const note of notes || []) {
    html += `<p class="yomi-note">⚠️ ${note}</p>`;
  }
  return html + `</div>`;
}

// ---- 期待値分析との接続 ----
// このページの乖離Δは翌開催の期待値モデルの入力。推奨が出たレースは
// ここに裏付け(市場確率→モデル確率→期待値の鎖)を明示し、推奨ゼロなら
// 「歪みはオッズに織り込み済み」という検証結果をそのまま示す。
function evConnectionHtml() {
  let latest, stats;
  try {
    latest = SiteDB.latestRecoRaces();
    stats = SiteDB.predStats();
  } catch (e) {
    return "";
  }
  if (!latest.date) return "";
  const t = stats.total;

  const intro = `<p class="section-sub">上の乖離Δは、翌開催の期待値モデルの唯一の入力になる(市場確率の対数オッズに −β×Δ を加点、β=0.5)。ここの数字が「買える歪み」なら、期待値分析に推奨として現れる。</p>`;

  if (!latest.races.length) {
    const cum = t && t.n_races
      ? `2024年1月以降の累計では、${t.n_races.toLocaleString()}レースを検証して期待値1.1を超えた推奨は<b>${t.n_reco_races}件</b>。`
      : "";
    const attn = stats.attn && stats.attn.n
      ? `モデルが市場より高く評価した「注目馬」${stats.attn.n}頭の実測単勝回収率は<b>${stats.attn.roi.toFixed(0)}%</b>(損益分岐は100%)。`
      : "";
    return `<section class="section panel reality-panel">
      <h2 class="section-head">このバイアスは、儲けに変わるか</h2>
      ${intro}
      <p class="reality-lead">直近の期待値分析(${formatDateWithDow(latest.date)})の答えは<b>ノー</b> — 推奨0レース。${cum}${attn}</p>
      <p class="yomi-foot">つまり、このページで観測される枠順バイアスは実在するが、その大半はすでにオッズに織り込まれており、控除率(約20%)の壁を超える余地を残していない。バイアスの検証記録と、それが馬券のプラスに変わらないという検証結果を、両方そのまま公開している。</p>
      <div class="link-button"><a href="index.html"><span>期待値分析を見る</span></a></div>
    </section>`;
  }

  let rows = "";
  for (const r of latest.races) {
    let devStr = "";
    try {
      const a = r.analysis ? JSON.parse(r.analysis) : null;
      const devs = (a && a.deviations) || {};
      devStr = ["内", "中", "外"].filter((g) => devs[g] != null)
        .map((g) => `${g}Δ${(devs[g] < 0 ? "−" : "+") + Math.abs(devs[g]).toFixed(3)}`)
        .join(" ");
    } catch (e) { /* 内訳なし */ }
    for (const h of r.horses) {
      rows += `<tr>
        <td>${r.place}${r.race_no}R</td>
        <td class="horse-name-cell">${h.horse || ""} <small>(${h.frame3 || "?"})</small></td>
        <td class="num-cell">${h.odds ?? "-"}</td>
        <td class="num-cell">${(h.market_prob * 100).toFixed(1)}%→${(h.model_prob * 100).toFixed(1)}%</td>
        <td class="num-cell">${h.ev.toFixed(2)}</td>
        <td><small>${devStr}</small></td>
      </tr>`;
    }
  }
  return `<section class="section panel reality-panel">
    <h2 class="section-head">期待値分析の推奨 ${latest.races.length}レース(${formatDateWithDow(latest.date)})</h2>
    ${intro}
    <table class="data-table"><thead><tr>
      <th>レース</th><th>推奨馬(枠位置)</th><th>オッズ</th><th>市場→モデル</th><th>期待値</th><th>使用した乖離</th>
    </tr></thead><tbody>${rows}</tbody></table>
    <p class="yomi-foot">推奨は「前開催で観測した枠位置グループの乖離が、オッズに織り込まれていない」とモデルが判断した稀なケース。詳細な判定の内訳は期待値分析ページの各レース欄。</p>
    <div class="link-button"><a href="index.html"><span>期待値分析で詳細を見る</span></a></div>
  </section>`;
}

// ---- ページ全体: 全場をスクロールで縦に並べる ----
function renderAll(items, dataByKey, bias3ByKey, isLatest) {
  const root = $("#report");
  let html = "";

  for (const it of items) {
    const data = dataByKey[it.key];
    const b3 = bias3ByKey[it.key];
    if (!data) continue;

    for (const surface of ["芝", "ダート"]) {
      const b3s = b3 && b3.surfaces && b3.surfaces[surface];
      if (!b3s) continue;
      html += `<section class="section panel">
        <h2 class="section-head">${it.place}・${surface}</h2>
        ${surfaceBlockHtml(it.place, surface, b3s, b3.notes)}
      </section>`;
    }
  }

  // 最新日の表示にだけ、直近の期待値分析との接続を載せる
  if (html && isLatest) {
    html += evConnectionHtml();
  }

  if (!html) {
    html = '<p class="loading">この開催日の分析データはありません。</p>';
  }
  root.innerHTML = html;
}

async function init() {
  let placeItems;
  try {
    await SiteDB.open();
    placeItems = SiteDB.indexItems();
  } catch (e) {
    $("#report").innerHTML =
      '<p class="loading">データがまだ生成されていません。</p>';
    return;
  }

  if (placeItems.length === 0) {
    $("#report").innerHTML = '<p class="loading">データがありません。</p>';
    return;
  }

  // 検証可能な日があれば結果検証へのCTAを表示
  if (SiteDB.verifyDates().length > 0) {
    const cta = $("#result-cta");
    if (cta) cta.hidden = false;
  }

  // ?date=YYYYMMDD で過去日指定。未指定なら最新。
  const params = new URLSearchParams(window.location.search);
  const requestedDate = params.get("date");
  const availableDates = [...new Set(placeItems.map((it) => it.date))]
    .sort()
    .reverse();

  let targetDate;
  if (requestedDate) {
    if (!availableDates.includes(requestedDate)) {
      $("#report").innerHTML =
        `<p class="loading">${requestedDate} のデータはありません。</p>`;
      return;
    }
    targetDate = requestedDate;
  } else {
    targetDate = availableDates[0];
  }

  const items = placeItems.filter((it) => it.date === targetDate);
  const dataByKey = {};
  const bias3ByKey = {};
  for (const it of items) {
    it.key = `${it.date}_${it.place}`;
    dataByKey[it.key] = SiteDB.report(it.date, it.place);
    bias3ByKey[it.key] = SiteDB.bias3(it.date, it.place);
  }

  $("#date-display").textContent =
    `${formatDateWithDow(targetDate)} 開催のトラックバイアス分析`;

  renderAll(items, dataByKey, bias3ByKey, targetDate === availableDates[0]);
}

function formatDateWithDow(yyyymmdd) {
  const yyyy = +yyyymmdd.slice(0, 4);
  const mm = +yyyymmdd.slice(4, 6);
  const dd = +yyyymmdd.slice(6, 8);
  const d = new Date(yyyy, mm - 1, dd);
  const dow = ["日", "月", "火", "水", "木", "金", "土"][d.getDay()];
  return `${yyyy}/${String(mm).padStart(2, "0")}/${String(dd).padStart(2, "0")}(${dow})`;
}

init();
