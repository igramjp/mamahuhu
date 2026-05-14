// どうだった - frontend renderer
const $ = (s, r = document) => r.querySelector(s);

async function fetchJSON(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`${r.status} ${path}`);
  return r.json();
}

function formatDateWithDow(yyyymmdd) {
  const yyyy = +yyyymmdd.slice(0, 4);
  const mm = +yyyymmdd.slice(4, 6);
  const dd = +yyyymmdd.slice(6, 8);
  const d = new Date(yyyy, mm - 1, dd);
  const dow = ['日', '月', '火', '水', '木', '金', '土'][d.getDay()];
  return `${yyyy}.${String(mm).padStart(2, '0')}.${String(dd).padStart(2, '0')} (${dow})`;
}

const SURFACE_META = {
  "芝": { cls: "" },
  "ダート": { cls: "dirt" },
};
const FRAME_PREFIX = { "内": "内枠の", "外": "外枠の" };
const pct = v => (v * 100).toFixed(1) + "%";

function placeAnchorId(place) {
  return `place-${encodeURIComponent(place)}`;
}

function renderSurfaceHeader(place, surface, c) {
  const meta = SURFACE_META[surface] || { cls: "" };
  const combo = (FRAME_PREFIX[c["内外"]] || c["内外"]) + c["脚質"];
  return `<div class="surface-header">
    <span class="surface-tag ${meta.cls}">${surface}</span>
    <br><span class="best-combo">前日のバイアス:<br><b>${combo}</b><small>※複勝率 ${pct(c["複勝率"])}（${c["出走数"]}頭）</small></span>
  </div>`;
}

function renderHeadlines(place, surfaces) {
  if (!surfaces || Object.keys(surfaces).length === 0) {
    return '<p class="result-headline">前日データなし。</p>';
  }
  let html = '';
  for (const surface of ['芝', 'ダート']) {
    const c = surfaces[surface];
    if (!c) continue;
    html += renderSurfaceHeader(place, surface, c);
  }
  html += '<p class="result-lead">を、きょうの結果に当ててみると...</p>';
  return html;
}

function renderPlace(p) {
  const headlines = renderHeadlines(p.place, p.surfaces);

  let rows = '';
  for (const race of p.races) {
    const bias = p.surfaces && p.surfaces[race.surface];
    const frameLabel = bias ? bias["内外"] + "枠" : null;
    const styleLabel = bias ? bias["脚質"] : null;
    const hitByRank = {};
    for (const h of (race.hits || [])) hitByRank[h["着順"]] = h;
    for (const rank of [1, 2, 3]) {
      const rcell = rank === 1
        ? `<td class="race-cell" rowspan="3"><span class="race-no">${race.R}R</span><small> ${race.surface}</small></td>`
        : '';
      const h = hitByRank[rank];
      const numEl = h && h["馬番"] != null ? `<span class="horse-num">${h["馬番"]}</span>` : '';
      let labelChips;
      if (!h || !h.labels || h.labels.length === 0) {
        labelChips = '<span class="chip chip-miss">該当なし</span>';
      } else {
        const isComboHit = frameLabel && styleLabel
          && h.labels.includes(frameLabel) && h.labels.includes(styleLabel);
        labelChips = h.labels.map(l => {
          const hit = isComboHit && (l === frameLabel || l === styleLabel);
          return `<span class="chip${hit ? ' chip-hit' : ''}">${l}</span>`;
        }).join('');
      }
      rows += `<tr>${rcell}<td class="result-cell"><div class="chips">${numEl}${labelChips}</div></td><td class="rank-cell">${rank}</td></tr>`;
    }
  }

  return `<section class="section" id="${placeAnchorId(p.place)}">
    <h2 class="section-head">${p.place}</h2>
    ${headlines}
    <table class="data-table result-table"><thead><tr>
      <th>レース</th><th>結果</th><th>着順</th>
    </tr></thead><tbody>${rows}</tbody></table>
  </section>`;
}

function renderPlaceButtons(places) {
  const container = $('#place-buttons');
  if (!container) return;
  container.innerHTML = '';
  for (const p of places) {
    const a = document.createElement('a');
    a.className = 'place-btn';
    a.textContent = p.place;
    a.href = `#${placeAnchorId(p.place)}`;
    container.appendChild(a);
  }
}

function renderResult(data) {
  const root = $('#report');
  if (!data.places || data.places.length === 0) {
    root.innerHTML = '<p class="loading">対象の場がありません。</p>';
    return;
  }
  root.innerHTML = data.places.map(renderPlace).join('');
  renderPlaceButtons(data.places);
}

async function init() {
  let index;
  try {
    index = await fetchJSON('data/index.json');
  } catch (e) {
    $('#report').innerHTML = '<p class="loading">データがまだ生成されていません。</p>';
    return;
  }

  const kekkaItems = (index.items || []).filter(it => it.place === '結果');
  if (kekkaItems.length === 0) {
    $('#report').innerHTML = '<p class="loading">結果データはまだありません。<br>2日連続で開催されると、ふりかえりが出ます。</p>';
    return;
  }

  const cta = $('#index-cta');
  if (cta) cta.hidden = false;

  kekkaItems.sort((a, b) => b.date.localeCompare(a.date));
  const latest = kekkaItems[0];

  $('#date-display').textContent = formatDateWithDow(latest.date);

  try {
    const data = await fetchJSON(`data/${latest.filename}`);
    renderResult(data);
  } catch (e) {
    $('#report').innerHTML = `<p class="loading">読み込みエラー: ${e.message}</p>`;
  }
}

init();
