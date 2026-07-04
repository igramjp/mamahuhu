// 馬場読み - frontend renderer
const $ = (s, r = document) => r.querySelector(s);

const SURFACE_META = {
  芝: { cls: "" },
  ダート: { cls: "dirt" },
};

const pct = (v) => (v * 100).toFixed(1) + "%";

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
  return html + "</tbody></table>";
}

const FRAME_PREFIX = { 内: "内枠の", 外: "外枠の" };

function renderSurface(surface, data, place, date) {
  const meta = SURFACE_META[surface] || { cls: "" };
  const c = data.best_combo;
  const comboHtml = c
    ? `<br><span class="best-combo">最も好走した枠×脚質:<br><b>${(FRAME_PREFIX[c["内外"]] || c["内外"]) + c["脚質"]}</b><small>※集計日 ${formatDateShort(date)}</small></span>`
    : "";
  return `<div class="surface-block">
    <div class="surface-header"><span class="surface-tag ${meta.cls}">${place}・${surface}</span>${comboHtml}</div>
    <h3 class="sub-head">枠バイアス</h3>
    ${biasTable(data.frame_bias, "区分", "内外")}
    <h3 class="sub-head">脚質バイアス</h3>
    ${biasTable(data.style_bias, "脚質", "脚質")}
  </div>`;
}

function renderJockeys(jockeys) {
  if (!jockeys || jockeys.length === 0)
    return '<p class="muted">データなし</p>';
  let html = `<table class="data-table"><thead><tr>
    <th>騎手</th><th>最大人気差</th><th>騎乗</th><th>勝</th><th>複</th>
  </tr></thead><tbody>`;
  for (const j of jockeys) {
    const v = j["最大人気差"];
    const cls = v > 0 ? "plus" : v < 0 ? "minus" : "";
    const sign = v > 0 ? "+" : "";
    html += `<tr>
      <td>${j["騎手"]}</td>
      <td class="${cls}">${sign}${v.toFixed(0)}</td>
      <td>${j["騎乗数"]}</td>
      <td>${j["勝利"]}</td>
      <td>${j["複勝"]}</td>
    </tr>`;
  }
  return html + "</tbody></table>";
}

function updateComboImage(c) {
  const img = $("#combo-image");
  if (!img) return;
  if (c && c["内外"] && c["脚質"]) {
    img.src = `/images/${c["内外"]}枠${c["脚質"]}.gif?t=${Date.now()}`;
    img.alt = `${(FRAME_PREFIX[c["内外"]] || c["内外"]) + c["脚質"]}`;
    img.hidden = false;
  } else {
    img.removeAttribute("src");
    img.alt = "";
    img.hidden = true;
  }
}

function winningEntry(rows, key) {
  if (!rows || rows.length === 0) return null;
  return [...rows].sort(
    (a, b) => b["複勝率"] - a["複勝率"] || b["出走数"] - a["出走数"],
  )[0][key];
}

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

function notableRaceHtml(nr, surfaces, hotJockeysAllPlaces) {
  if (!nr || !nr.entries || nr.entries.length === 0) return "";
  const sData = surfaces && surfaces[nr.surface];
  const winFrame = sData ? winningEntry(sData.frame_bias, "内外") : null;
  const winStyle = sData ? winningEntry(sData.style_bias, "脚質") : null;
  const hotNames = (hotJockeysAllPlaces || [])
    .map((j) => j["騎手"])
    .filter(Boolean);
  const meta = SURFACE_META[nr.surface] || { cls: "" };
  const dateLabel = nr.date ? formatDateWithDow(nr.date) : "";
  const raceNameHtml = nr.race_name
    ? `<span class="notable-race-name">${nr.race_name}</span>`
    : "";

  let rows = "";
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
    const chipsHtml =
      chips.length > 0 ? `<div class="chips">${chips.join("")}</div>` : "";
    rows += `<tr>
      <td class="horse-num-cell"><span class="horse-num">${e["馬番"]}</span></td>
      <td class="horse-name-cell">${e["馬名"]}</td>
      <td class="horse-jockey-cell">${e["騎手"] || ""}</td>
      <td class="horse-chips-cell">${chipsHtml}</td>
    </tr>`;
  }

  return `<section class="section" id="notable-race">
    <h2 class="section-head">注目レース</h2>
    <p class="section-sub">次開催のメインレース「${nr.race_name}」の出馬表。<br>直近のバイアスに当てはまる馬は？</p>
    <div class="notable-race-head">
      <span class="surface-tag ${meta.cls}">${nr.surface || ""}</span>
      ${raceNameHtml}
      <span class="notable-meta">${dateLabel} 11R</span>
    </div>
    <table class="data-table notable-race-table"><tbody>${rows}</tbody></table>
  </section>`;
}

function renderReport(data, surface, place, hotJockeysAllPlaces) {
  const root = $("#report");
  let html = "";

  const surfaces = data.surfaces || {};
  const surfaceData = surfaces[surface];
  updateComboImage(surfaceData && surfaceData.best_combo);

  // バイアス
  html += `<section class="section">
    <h2 class="section-head">トラックバイアス</h2>
    <p class="section-sub">直近の開催結果をもとに、<br>枠順（内1-4 / 外5-8）と脚質ごとの成績を集計。</p>`;
  if (surfaceData) {
    html += renderSurface(surface, surfaceData, place, data.date);
  } else {
    html += `<p class="muted">${surface}のレースはありませんでした。</p>`;
  }
  html += `</section>`;

  // 騎手 (surface関係なく1日合算、その場の集計)
  html += `<section class="section">
    <h2 class="section-head">好調騎手</h2>
    <p class="section-sub">人気差 = 人気 − 着順。<br>複勝圏内で大駆けを決めた騎手。</p>
    ${renderJockeys(data.hot_jockeys || [])}
  </section>`;

  // 注目レース (騎手は当日全競馬場の好調騎手とマッチ)
  html += notableRaceHtml(data.notable_race, surfaces, hotJockeysAllPlaces);

  root.innerHTML = html;
}

async function init() {
  let placeItems;
  try {
    await SiteDB.open();
    placeItems = SiteDB.indexItems();
  } catch (e) {
    $("#report").innerHTML =
      '<p class="loading">データがまだ生成されていません。<br>GitHub Actionsの初回実行（または手動実行）後に表示されます。</p>';
    return;
  }

  if (placeItems.length === 0) {
    $("#report").innerHTML = '<p class="loading">データがありません。</p>';
    return;
  }

  // 結果(ふりかえり)が導出できる日があればCTAボタンを表示
  if (SiteDB.kekkaDates().length > 0) {
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

  const latestItems = placeItems.filter((it) => it.date === targetDate);
  for (const it of latestItems) it.key = `${it.date}_${it.place}`;

  // 当日全場のデータをsite.dbから組み立て。注目レースの騎手chipを当日
  // 全競馬場の好調騎手とマッチさせるため(土曜は東京、日曜は京都など
  // 移動するため単一場の好調騎手リストでは取りこぼす)。
  const dataByKey = {};
  let hotJockeysAllPlaces = [];
  for (const it of latestItems) {
    const d = SiteDB.report(it.date, it.place);
    if (!d) continue;
    dataByKey[it.key] = d;
    hotJockeysAllPlaces.push(...(d.hot_jockeys || []));
  }

  // 見出し: 集計日(過去日)だと古く見えるので、当週の注目レース名を出す。
  // 当週にG3以上のJRA重賞があればその名前、無ければ「きょう」。
  // 重賞が複数なら格上(G1>G2>G3)を優先。
  const gradeRank = { G1: 1, G2: 2, G3: 3 };
  const topGraded = latestItems
    .map((it) => ({ it, nr: (dataByKey[it.key] || {}).notable_race }))
    .filter((x) => x.nr && x.nr.race_name && gradeRank[x.nr.grade])
    .sort((a, b) => gradeRank[a.nr.grade] - gradeRank[b.nr.grade])[0];
  $("#date-display").textContent = topGraded
    ? `${topGraded.nr.race_name}のバイアスを見てみよう！`
    : "最新のバイアスを見てみよう！";

  // 競馬場・コース選択の状態。重賞があればその開催場とコースを初期選択、
  // 無ければ先頭の場・芝。
  const btnContainer = $("#place-buttons");
  const surfBtnContainer = $("#surface-buttons");
  let currentKey = topGraded ? topGraded.it.key : latestItems[0].key;
  let currentPlace = topGraded ? topGraded.it.place : latestItems[0].place;
  let currentSurface = (topGraded && topGraded.nr.surface) || "芝";

  function loadAndRender(key) {
    const data = dataByKey[key];
    if (!data) {
      $("#report").innerHTML =
        `<p class="loading">読み込みエラー: ${key}</p>`;
      return;
    }
    renderReport(data, currentSurface, currentPlace, hotJockeysAllPlaces);
  }

  for (const it of latestItems) {
    const btn = document.createElement("button");
    btn.className = "place-btn";
    btn.textContent = it.place;
    btn.dataset.key = it.key;
    if (it.key === currentKey) btn.classList.add("active");
    btn.addEventListener("click", () => {
      btnContainer
        .querySelectorAll(".place-btn")
        .forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      currentKey = it.key;
      currentPlace = it.place;
      loadAndRender(it.key);
    });
    btnContainer.appendChild(btn);
  }

  for (const s of ["芝", "ダート"]) {
    const btn = document.createElement("button");
    btn.className = "place-btn";
    btn.textContent = s;
    if (s === currentSurface) btn.classList.add("active");
    btn.addEventListener("click", () => {
      surfBtnContainer
        .querySelectorAll(".place-btn")
        .forEach((b) => b.classList.remove("active"));
      btn.classList.add("active");
      currentSurface = s;
      loadAndRender(currentKey);
    });
    surfBtnContainer.appendChild(btn);
  }

  loadAndRender(currentKey);
}

function formatDateWithDow(yyyymmdd) {
  const yyyy = +yyyymmdd.slice(0, 4);
  const mm = +yyyymmdd.slice(4, 6);
  const dd = +yyyymmdd.slice(6, 8);
  const d = new Date(yyyy, mm - 1, dd);
  const dow = ["日", "月", "火", "水", "木", "金", "土"][d.getDay()];
  return `${yyyy}/${String(mm).padStart(2, "0")}/${String(dd).padStart(2, "0")}(${dow})`;
}

function formatDateShort(yyyymmdd) {
  const yyyy = +yyyymmdd.slice(0, 4);
  const mm = +yyyymmdd.slice(4, 6);
  const dd = +yyyymmdd.slice(6, 8);
  const d = new Date(yyyy, mm - 1, dd);
  const dow = ["日", "月", "火", "水", "木", "金", "土"][d.getDay()];
  return `${mm}/${dd}(${dow})`;
}

init();
