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

// ---- 注目レース ----
function entryFrame3(entries) {
  const sorted = [...entries].sort((a, b) => a["馬番"] - b["馬番"]);
  const n = sorted.length;
  const map = {};
  sorted.forEach((e, i) => {
    const p = (i + 1) / n;
    map[e["馬番"]] = p <= 1 / 3 + 1e-9 ? "内" : p <= 2 / 3 + 1e-9 ? "中" : "外";
  });
  return map;
}

function notableRaceHtml(nr, bias3data) {
  if (!nr || !nr.entries || nr.entries.length === 0) return "";
  const b3s = bias3data && bias3data.surfaces && bias3data.surfaces[nr.surface];
  const favFrame = b3s ? SiteDB.favoredGroup(b3s.frame) : null;
  const favStyle = b3s ? SiteDB.favoredGroup(b3s.style) : null;
  const frame3Map = entryFrame3(nr.entries);
  const meta = SURFACE_META[nr.surface] || { cls: "" };
  const dateLabel = nr.date ? formatDateWithDow(nr.date) : "";
  const raceNameHtml = nr.race_name
    ? `<span class="notable-race-name">${nr.race_name}</span>`
    : "";

  let rows = "";
  for (const e of nr.entries) {
    const chips = [];
    const frameHit = favFrame && frame3Map[e["馬番"]] === favFrame;
    const styleHit = favStyle && e["脚質"] === favStyle;
    if (frameHit && styleHit) {
      chips.push(`<span class="chip chip-hit">${favFrame}枠</span>`);
      chips.push(`<span class="chip chip-hit">${e["脚質"]}</span>`);
    }
    const chipsHtml =
      chips.length > 0 ? `<div class="chips">${chips.join("")}</div>` : "";
    rows += `<tr>
      <td class="horse-num-cell"><span class="horse-num">${e["馬番"]}</span></td>
      <td class="horse-name-cell">${e["馬名"]}</td>
      <td class="horse-jockey-cell">${e["騎手"] || ""}</td>
      <td class="horse-chips-cell">${chipsHtml}</td>
    </tr>`;
  }

  return `<section class="section panel notable-section">
    <h2 class="section-head">次開催: ${nr.race_name || "メインレース"}</h2>
    <p class="section-sub">直近推定バイアス(有利化グループ)に枠・脚質の両方が該当する出走馬をマーク。</p>
    <div class="notable-race-head">
      <span class="surface-tag ${meta.cls}">${nr.surface || ""}</span>
      ${raceNameHtml}
      <span class="notable-meta">${dateLabel} 11R</span>
    </div>
    <table class="data-table notable-race-table"><tbody>${rows}</tbody></table>
  </section>`;
}

// ---- ページ全体: 全場をスクロールで縦に並べる ----
function renderAll(items, dataByKey, bias3ByKey) {
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

    html += notableRaceHtml(data.notable_race, b3);
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

  renderAll(items, dataByKey, bias3ByKey);
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
