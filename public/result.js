// 結果検証 - frontend renderer
// 推奨馬(単勝)の的中と回収率を検証する。基準(期待値1.1)に届く馬がいない
// レースは、最も基準に近かった馬の期待値と着順を記録する。
const EV_THRESHOLD = 1.1;
const $ = (s, r = document) => r.querySelector(s);

const SURFACE_META = {
  芝: { cls: "" },
  ダート: { cls: "dirt" },
};

function formatDateShort(yyyymmdd) {
  const yyyy = +yyyymmdd.slice(0, 4);
  const mm = +yyyymmdd.slice(4, 6);
  const dd = +yyyymmdd.slice(6, 8);
  const d = new Date(yyyy, mm - 1, dd);
  const dow = ["日", "月", "火", "水", "木", "金", "土"][d.getDay()];
  return `${mm}/${dd}(${dow})`;
}

function fmtRoi(roi) {
  return roi == null ? "—" : Math.round(roi) + "%";
}

function outcomeChip(h) {
  if (h.hit) return `<span class="chip chip-hit">1着 的中</span>`;
  if (h.rank === 2 || h.rank === 3)
    return `<span class="chip">${h.rank}着</span>`;
  return `<span class="chip chip-miss">4着以下</span>`;
}

function horseRow(h) {
  const kindChip = h.kind === "推奨"
    ? '<span class="chip chip-hit">推奨</span>'
    : `<span class="chip chip-attn">注目${h.edge != null ? ` +${(h.edge * 100).toFixed(1)}%` : ""}</span>`;
  return `<tr class="${h.hit ? "verify-hit-row" : ""}">
    <td class="horse-num-cell"><span class="horse-num">${h.umaban}</span></td>
    <td class="horse-name-cell verify-name-cell">${h.horse || ""}</td>
    <td class="num-cell">${h.odds ?? "—"}</td>
    <td class="verify-kind-cell">${kindChip}</td>
    <td class="verify-outcome-cell">${outcomeChip(h)}</td>
  </tr>`;
}

// 基準に届かなかったレース: 「見送り」の一語ではなく、最も基準に近かった馬が
// どこまで届き、何着だったかを出す
function bestChip(b) {
  if (!b) return '<span class="verdict-chip verdict-pass">基準未満</span>';
  const rank = b.rank ? `・${b.rank}着` : "";
  const who = `${b.umaban}番${b.horse ? " " + b.horse : ""}`;
  return `<span class="verdict-chip verdict-pass" title="最も基準(期待値${EV_THRESHOLD})に近かった馬: ${who}">最大期待値 ${b.ev.toFixed(2)}${rank}</span>`;
}

function raceRow(r) {
  const meta = SURFACE_META[r.surface] || { cls: "" };
  const hasAttn = r.horses.some((h) => h.kind === "注目");
  const head = `<div class="pred-race-head">
      <span class="race-no-tag">${r.race_no}R</span>
      <span class="pred-race-name">${r.race_name || ""}</span>
      <span class="surface-tag-mini ${meta.cls}">${r.surface}${r.distance || ""}m</span>
      ${r.verdict === "推奨"
        ? '<span class="verdict-chip verdict-reco">推奨</span>'
        : bestChip(r.best)}
      ${hasAttn ? '<span class="verdict-chip verdict-attn">注目</span>' : ""}
    </div>`;

  if (!r.horses.length) {
    return `<div class="pred-race verify-pass-race">${head}</div>`;
  }

  const table = `<table class="data-table pred-table"><thead><tr>
      <th>馬番</th><th>馬名</th><th>単勝</th><th>区分</th><th>結果</th>
    </tr></thead><tbody>${r.horses.map(horseRow).join("")}</tbody></table>`;
  return `<div class="pred-race pred-race-open">${head}${table}</div>`;
}

function summaryHtml(s, label) {
  return `<div class="verify-summary">
    <div class="verify-stat"><span class="verify-stat-num">${s.n_races}</span><span class="verify-stat-label">対象R</span></div>
    <div class="verify-stat"><span class="verify-stat-num">${s.n_reco}<small>頭</small></span><span class="verify-stat-label">推奨(EV1.1超)</span></div>
    <div class="verify-stat"><span class="verify-stat-num">${s.n_attn_hit}<small>/${s.n_attn}</small></span><span class="verify-stat-label">注目馬 的中</span></div>
    <div class="verify-stat"><span class="verify-stat-num ${s.attn_roi != null && s.attn_roi >= 100 ? "roi-plus" : ""}">${fmtRoi(s.attn_roi)}</span><span class="verify-stat-label">注目馬 回収率</span></div>
  </div>`;
}

function subText(data) {
  const b = data.best;
  const bestText = b
    ? `最も基準に近かったのは${b.place}${b.race_no}R ${b.umaban}番${b.horse ? " " + b.horse : ""}の期待値<b>${b.ev.toFixed(2)}</b>${b.rank ? `(結果${b.rank}着)` : ""}。`
    : "";
  if (data.total.n_reco === 0) {
    return `この日、期待値が基準${EV_THRESHOLD}に届いた馬はいません(推奨0頭のため、推奨の回収率はありません)。<br>${bestText}各レースには、最も基準に近かった馬の期待値と着順を記録しています。`;
  }
  return `基準(期待値${EV_THRESHOLD})を超えた推奨馬を単勝100円で均等買いした場合の検証。<br>基準に届かなかったレースは賭けていないため回収率に影響しません。${bestText}`;
}

function renderVerify(data) {
  const root = $("#report");
  let html = `<section class="section panel">
    <h2 class="section-head">検証サマリ</h2>
    <p class="section-sub">${subText(data)}</p>
    ${summaryHtml(data.total)}
  </section>`;

  for (const pl of data.places) {
    html += `<section class="section panel">
      <h2 class="section-head">${pl.place}</h2>
      ${summaryHtml(pl.summary)}
      ${pl.races.map(raceRow).join("")}
    </section>`;
  }

  html += `<section class="section">
    <h2 class="section-head">読み方</h2>
    <p class="yomi-foot">的中 = 推奨馬の1着(単勝)。回収率 = Σ(的中馬のオッズ)÷推奨頭数×100。
    1開催日の回収率は分散が大きく、単日の結果でモデルの優劣は判断できません。
    評価は長期のROIとキャリブレーションで行います(分析手法参照)。</p>
    <p class="yomi-note">⚠️ 本検証は過去の推奨の記録であり、将来の成績を保証しません。</p>
  </section>`;

  root.innerHTML = html;
}

async function init() {
  let dates;
  try {
    await SiteDB.open();
    dates = SiteDB.verifyDates(); // 新しい順
  } catch (e) {
    $("#report").innerHTML = '<p class="loading">データがまだ生成されていません。</p>';
    return;
  }

  if (dates.length === 0) {
    $("#report").innerHTML =
      '<p class="loading">検証可能なデータはまだありません。<br>推奨とレース結果が揃うと表示されます。</p>';
    return;
  }

  const cta = $("#index-cta");
  if (cta) cta.hidden = false;

  // ?date=YYYYMMDD で過去日指定。未指定なら最新。
  const params = new URLSearchParams(window.location.search);
  const requestedDate = params.get("date");
  let targetDate;
  if (requestedDate) {
    if (!dates.includes(requestedDate)) {
      $("#report").innerHTML =
        `<p class="loading">${requestedDate} の検証データはありません。</p>`;
      return;
    }
    targetDate = requestedDate;
  } else {
    targetDate = dates[0];
  }

  const ledeDate = $("#lede-date");
  if (ledeDate) ledeDate.textContent = formatDateShort(targetDate);

  const data = SiteDB.verify(targetDate);
  if (!data) {
    $("#report").innerHTML =
      `<p class="loading">${targetDate} の検証データはありません。</p>`;
    return;
  }
  renderVerify(data);
}

init();
