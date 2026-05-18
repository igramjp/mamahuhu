// 馬場読み - frontend renderer
const $ = (s, r = document) => r.querySelector(s);

const SURFACE_META = {
  "芝": { cls: "" },
  "ダート": { cls: "dirt" },
};

async function fetchJSON(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`${r.status} ${path}`);
  return r.json();
}

const pct = v => (v * 100).toFixed(1) + "%";

function biasTable(rows, firstColLabel, firstColKey) {
  if (!rows || rows.length === 0) return '<p class="muted">データなし</p>';
  let html = `<table class="data-table bias-table"><thead><tr>
    <th>${firstColLabel}</th><th>勝率</th><th>複勝率</th><th>頭数</th>
  </tr></thead><tbody>`;
  for (const r of rows) {
    html += `<tr>
      <td>${r[firstColKey]}</td>
      <td>${pct(r["勝率"])}</td>
      <td>${pct(r["複勝率"])}</td>
      <td>${r["出走数"]}</td>
    </tr>`;
  }
  return html + '</tbody></table>';
}

const FRAME_PREFIX = { "内": "内枠の", "外": "外枠の" };

function renderSurface(surface, data, place, date) {
  const meta = SURFACE_META[surface] || { cls: "" };
  const c = data.best_combo;
  const comboHtml = c
    ? `<br><span class="best-combo">${formatDateShort(date)}当日のバイアス:<br><b>${(FRAME_PREFIX[c["内外"]] || c["内外"]) + c["脚質"]}</b><small>※複勝率 ${pct(c["複勝率"])}（${c["出走数"]}頭）</small></span>`
    : '';
  return `<div class="surface-block">
    <div class="surface-header"><span class="surface-tag ${meta.cls}">${place}・${surface}</span>${comboHtml}</div>
    <h3 class="sub-head">枠バイアス</h3>
    ${biasTable(data.frame_bias, "区分", "内外")}
    <h3 class="sub-head">脚質バイアス</h3>
    ${biasTable(data.style_bias, "脚質", "脚質")}
  </div>`;
}

function renderJockeys(jockeys) {
  if (!jockeys || jockeys.length === 0) return '<p class="muted">データなし</p>';
  let html = `<table class="data-table"><thead><tr>
    <th>騎手</th><th>最大人気差</th><th>騎乗</th><th>勝</th><th>複</th>
  </tr></thead><tbody>`;
  for (const j of jockeys) {
    const v = j["最大人気差"];
    const cls = v > 0 ? 'plus' : (v < 0 ? 'minus' : '');
    const sign = v > 0 ? '+' : '';
    html += `<tr>
      <td>${j["騎手"]}</td>
      <td class="${cls}">${sign}${v.toFixed(0)}</td>
      <td>${j["騎乗数"]}</td>
      <td>${j["勝利"]}</td>
      <td>${j["複勝"]}</td>
    </tr>`;
  }
  return html + '</tbody></table>';
}

function updateComboImage(c) {
  const img = $('#combo-image');
  if (!img) return;
  if (c && c["内外"] && c["脚質"]) {
    img.src = `/images/${c["内外"]}枠${c["脚質"]}.gif?t=${Date.now()}`;
    img.alt = `${(FRAME_PREFIX[c["内外"]] || c["内外"]) + c["脚質"]}`;
    img.hidden = false;
  } else {
    img.removeAttribute('src');
    img.alt = '';
    img.hidden = true;
  }
}

function winningEntry(rows, key) {
  if (!rows || rows.length === 0) return null;
  return [...rows].sort((a, b) =>
    (b["複勝率"] - a["複勝率"]) || (b["出走数"] - a["出走数"]))[0][key];
}

function stripJockeyMark(name) {
  return (name || '').replace(/^[▲★△◇▽◎○☆▼]+/, '').trim();
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

function notableRaceHtml(nr, surfaces, hotJockeysAllPlaces) {
  if (!nr || !nr.entries || nr.entries.length === 0) return '';
  const sData = surfaces && surfaces[nr.surface];
  const winFrame = sData ? winningEntry(sData.frame_bias, "内外") : null;
  const winStyle = sData ? winningEntry(sData.style_bias, "脚質") : null;
  const hotNames = (hotJockeysAllPlaces || []).map(j => j["騎手"]).filter(Boolean);
  const meta = SURFACE_META[nr.surface] || { cls: "" };
  const dateLabel = nr.date ? formatDateWithDow(nr.date) : '';
  const raceNameHtml = nr.race_name
    ? `<span class="notable-race-name">${nr.race_name}</span>`
    : '';

  let rows = '';
  for (const e of nr.entries) {
    const chips = [];
    const frameHit = winFrame && e["内外"] === winFrame;
    const styleHit = winStyle && e["脚質"] === winStyle;
    if (frameHit && styleHit) {
      chips.push(`<span class="chip chip-hit">${e["内外"]}枠</span>`);
      chips.push(`<span class="chip chip-hit">${e["脚質"]}</span>`);
    }
    if (e["騎手"] && jockeyInHotSet(e["騎手"], hotNames)) {
      chips.push(`<span class="chip">${e["騎手"]}</span>`);
    }
    const chipsHtml = chips.length > 0 ? `<div class="chips">${chips.join('')}</div>` : '';
    rows += `<tr>
      <td class="horse-num-cell"><span class="horse-num">${e["馬番"]}</span></td>
      <td class="horse-name-cell">${e["馬名"]}</td>
      <td class="horse-jockey-cell">${e["騎手"] || ''}</td>
      <td class="horse-chips-cell">${chipsHtml}</td>
    </tr>`;
  }

  return `<section class="section" id="notable-race">
    <h2 class="section-head">注目レース</h2>
    <p class="section-sub">翌開催日、${formatDateShort(nr.date)}のメインレース「${nr.race_name}」の出馬表。<br>当日のバイアスに該当する馬を探してみると...</p>
    <div class="notable-race-head">
      <span class="surface-tag ${meta.cls}">${nr.surface || ''}</span>
      ${raceNameHtml}
      <span class="notable-meta">${dateLabel} 11R</span>
    </div>
    <table class="data-table notable-race-table"><tbody>${rows}</tbody></table>
  </section>`;
}

function renderReport(data, surface, place, hotJockeysAllPlaces) {
  const root = $('#report');
  let html = '';

  const surfaces = data.surfaces || {};
  const surfaceData = surfaces[surface];
  updateComboImage(surfaceData && surfaceData.best_combo);

  // バイアス
  html += `<section class="section">
    <h2 class="section-head">トラックバイアス</h2>
    <p class="section-sub">枠順を2区分（内1-4 / 外5-8）と脚質ごとに集計。差が大きいほど偏りが強い。</p>`;
  if (surfaceData) {
    html += renderSurface(surface, surfaceData, place, data.date);
  } else {
    html += `<p class="muted">${surface}のレースはありませんでした。</p>`;
  }
  html += `</section>`;

  // 騎手 (surface関係なく1日合算、その場の集計)
  html += `<section class="section">
    <h2 class="section-head">好調騎手</h2>
    <p class="section-sub">複勝圏内（1〜3着）で人気差+5以上のサプライズを記録した騎手。人気差 = 人気 − 着順。1鞍でも大駆けがあれば拾える指標。</p>
    ${renderJockeys(data.hot_jockeys || [])}
  </section>`;

  // 注目レース (騎手は当日全競馬場の好調騎手とマッチ)
  html += notableRaceHtml(data.notable_race, surfaces, hotJockeysAllPlaces);

  root.innerHTML = html;
}

async function init() {
  let index;
  try {
    index = await fetchJSON('data/index.json');
  } catch (e) {
    $('#report').innerHTML = '<p class="loading">データがまだ生成されていません。<br>GitHub Actionsの初回実行（または手動実行）後に表示されます。</p>';
    return;
  }

  if (!index.items || index.items.length === 0) {
    $('#report').innerHTML = '<p class="loading">データがありません。</p>';
    return;
  }

  // 結果データがあればCTAボタンを表示
  if (index.items.some(it => it.place === '結果')) {
    const cta = $('#result-cta');
    if (cta) cta.hidden = false;
  }

  // ?date=YYYYMMDD で過去日指定。未指定なら最新。
  const params = new URLSearchParams(window.location.search);
  const requestedDate = params.get('date');

  // 結果ファイルは別ページなので場ボタンから除外
  const placeItems = index.items.filter(it => it.place !== '結果');
  const availableDates = [...new Set(placeItems.map(it => it.date))].sort().reverse();

  let targetDate;
  if (requestedDate) {
    if (!availableDates.includes(requestedDate)) {
      $('#report').innerHTML = `<p class="loading">${requestedDate} のデータはありません。</p>`;
      return;
    }
    targetDate = requestedDate;
  } else {
    targetDate = availableDates[0];
  }

  const latestItems = placeItems.filter(it => it.date === targetDate);

  // 日付表示（曜日付き）
  $('#date-display').textContent = formatDateWithDow(targetDate);

  // 当日全場のデータを並列fetch。注目レースの騎手chipを当日全競馬場の
  // 好調騎手とマッチさせるため(土曜は東京、日曜は京都など移動するため
  // 単一場の好調騎手リストでは取りこぼす)。各場のJSONはここで一度だけ
  // 取得して dataByFilename にキャッシュし、ボタン押下時に再利用する。
  $('#report').innerHTML = '<p class="loading">読み込み中...</p>';
  const dataByFilename = {};
  let hotJockeysAllPlaces = [];
  try {
    const all = await Promise.all(latestItems.map(async it => {
      const d = await fetchJSON(`data/${it.filename}`);
      dataByFilename[it.filename] = d;
      return d;
    }));
    hotJockeysAllPlaces = all.flatMap(d => d.hot_jockeys || []);
  } catch (e) {
    $('#report').innerHTML = `<p class="loading">読み込みエラー: ${e.message}</p>`;
    return;
  }

  // 競馬場・コース選択の状態
  const btnContainer = $('#place-buttons');
  const surfBtnContainer = $('#surface-buttons');
  let currentFilename = latestItems[0].filename;
  let currentPlace = latestItems[0].place;
  let currentSurface = '芝';

  function loadAndRender(filename) {
    const data = dataByFilename[filename];
    if (!data) {
      $('#report').innerHTML = `<p class="loading">読み込みエラー: ${filename}</p>`;
      return;
    }
    renderReport(data, currentSurface, currentPlace, hotJockeysAllPlaces);
  }

  for (const it of latestItems) {
    const btn = document.createElement('button');
    btn.className = 'place-btn';
    btn.textContent = it.place;
    btn.dataset.filename = it.filename;
    if (it.filename === currentFilename) btn.classList.add('active');
    btn.addEventListener('click', () => {
      btnContainer.querySelectorAll('.place-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      currentFilename = it.filename;
      currentPlace = it.place;
      loadAndRender(it.filename);
    });
    btnContainer.appendChild(btn);
  }

  for (const s of ['芝', 'ダート']) {
    const btn = document.createElement('button');
    btn.className = 'place-btn';
    btn.textContent = s;
    if (s === currentSurface) btn.classList.add('active');
    btn.addEventListener('click', () => {
      surfBtnContainer.querySelectorAll('.place-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      currentSurface = s;
      loadAndRender(currentFilename);
    });
    surfBtnContainer.appendChild(btn);
  }

  loadAndRender(currentFilename);
}

function formatDateWithDow(yyyymmdd) {
  const yyyy = +yyyymmdd.slice(0, 4);
  const mm = +yyyymmdd.slice(4, 6);
  const dd = +yyyymmdd.slice(6, 8);
  const d = new Date(yyyy, mm - 1, dd);
  const dow = ['日', '月', '火', '水', '木', '金', '土'][d.getDay()];
  return `${yyyy}/${String(mm).padStart(2, '0')}/${String(dd).padStart(2, '0')}(${dow})`;
}

function formatDateShort(yyyymmdd) {
  const yyyy = +yyyymmdd.slice(0, 4);
  const mm = +yyyymmdd.slice(4, 6);
  const dd = +yyyymmdd.slice(6, 8);
  const d = new Date(yyyy, mm - 1, dd);
  const dow = ['日', '月', '火', '水', '木', '金', '土'][d.getDay()];
  return `${mm}/${dd}(${dow})`;
}

init();
