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

function renderSurface(surface, data, place) {
  const meta = SURFACE_META[surface] || { cls: "" };
  const c = data.best_combo;
  const comboHtml = c
    ? `<br><span class="best-combo">いちばん走ったのは:<br><b>${(FRAME_PREFIX[c["内外"]] || c["内外"]) + c["脚質"]}</b><small>※複勝率 ${pct(c["複勝率"])}（${c["出走数"]}頭）</small></span>`
    : '';
  return `<div class="surface-block">
    <div class="surface-header"><span class="surface-tag ${meta.cls}">${place}・${surface}</span>${comboHtml}</div>
    <h3 class="sub-head">枠バイアス（内・外）</h3>
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

function renderReport(data, surface, place) {
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
    html += renderSurface(surface, surfaceData, place);
  } else {
    html += `<p class="muted">${surface}のレースはありませんでした。</p>`;
  }
  html += `</section>`;

  // 騎手 (surface関係なく1日合算)
  html += `<section class="section">
    <h2 class="section-head">好調騎手</h2>
    <p class="section-sub">複勝圏内（1〜3着）で人気差+5以上のサプライズを記録した騎手。人気差 = 人気 − 着順。1鞍でも大駆けがあれば拾える指標。</p>
    ${renderJockeys(data.hot_jockeys || [])}
  </section>`;

  root.innerHTML = html;
}

async function init() {
  const yearEl = $('#year');
  if (yearEl) yearEl.textContent = new Date().getFullYear();

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

  // 最新の日付を取得（indexはdesc順だが念のためソート）
  // 結果ファイルは別ページなので場ボタンから除外
  const placeItems = index.items.filter(it => it.place !== '結果');
  const latestDate = placeItems
    .map(it => it.date)
    .sort()
    .reverse()[0];

  const latestItems = placeItems.filter(it => it.date === latestDate);

  // 日付表示（曜日付き）
  $('#date-display').textContent = formatDateWithDow(latestDate);

  // 競馬場・コース選択の状態
  const btnContainer = $('#place-buttons');
  const surfBtnContainer = $('#surface-buttons');
  let currentFilename = latestItems[0].filename;
  let currentPlace = latestItems[0].place;
  let currentSurface = '芝';

  async function loadAndRender(filename) {
    $('#report').innerHTML = '<p class="loading">読み込み中...</p>';
    try {
      const data = await fetchJSON(`data/${filename}`);
      renderReport(data, currentSurface, currentPlace);
    } catch (e) {
      $('#report').innerHTML = `<p class="loading">読み込みエラー: ${e.message}</p>`;
    }
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
  return `${yyyy}.${String(mm).padStart(2, '0')}.${String(dd).padStart(2, '0')} (${dow})`;
}

init();
