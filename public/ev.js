// 期待値分析 - frontend renderer
const $ = (s, r = document) => r.querySelector(s);

const SURFACE_META = {
  芝: { cls: "" },
  ダート: { cls: "dirt" },
};

const pctP = (v) => (v * 100).toFixed(1) + "%";

function formatDateWithDow(yyyymmdd) {
  const yyyy = +yyyymmdd.slice(0, 4);
  const mm = +yyyymmdd.slice(4, 6);
  const dd = +yyyymmdd.slice(6, 8);
  const d = new Date(yyyy, mm - 1, dd);
  const dow = ["日", "月", "火", "水", "木", "金", "土"][d.getDay()];
  return `${yyyy}/${String(mm).padStart(2, "0")}/${String(dd).padStart(2, "0")}(${dow})`;
}

function horseRows(horses, expanded) {
  // 展開時: 推奨・注目+上位3頭。折りたたみ: 上位3頭。
  const shown = expanded
    ? horses.filter((h) => h.recommended || h.attention || horses.indexOf(h) < 3)
    : horses.slice(0, 3);
  let rows = "";
  for (const h of shown) {
    const evCls = h.ev >= 1.1 ? "ev-hot" : h.ev >= 0.9 ? "ev-warm" : "";
    const chips =
      (h.recommended ? '<span class="chip chip-hit reco-chip">推奨</span>' : "") +
      (h.attention ? `<span class="chip chip-attn reco-chip">注目 市場比+${(h.edge * 100).toFixed(1)}%</span>` : "");
    rows += `<tr class="${h.recommended ? "reco-row" : h.attention ? "attn-row" : ""}">
      <td class="horse-num-cell"><span class="horse-num">${h.umaban}</span></td>
      <td class="horse-name-cell">${h.horse || ""}${chips}</td>
      <td class="num-cell">${h.odds ?? "-"}</td>
      <td class="num-cell">${pctP(h.market_prob)}</td>
      <td class="num-cell">${pctP(h.model_prob)}</td>
      <td class="num-cell ${evCls}">${h.ev.toFixed(2)}</td>
    </tr>`;
  }
  return rows;
}

function raceCard(r) {
  const meta = SURFACE_META[r.surface] || { cls: "" };
  const isReco = r.verdict === "推奨";
  const hasAttn = r.horses.some((h) => h.attention);
  const verdictChip = isReco
    ? '<span class="verdict-chip verdict-reco">推奨あり</span>'
    : '<span class="verdict-chip verdict-pass">見送り</span>';
  const attnChip = hasAttn
    ? '<span class="verdict-chip verdict-attn">注目</span>' : "";

  const head = `<div class="pred-race-head">
      <span class="race-no-tag">${r.race_no}R</span>
      <span class="pred-race-name">${r.race_name || ""}</span>
      <span class="surface-tag-mini ${meta.cls}">${r.surface}${r.distance || ""}m</span>
      ${verdictChip}${attnChip}
    </div>`;

  const expanded = isReco || hasAttn;
  const table = `<table class="data-table pred-table"><thead><tr>
      <th>馬番</th><th>馬名</th><th>単勝オッズ</th><th>市場確率</th><th>モデル確率</th><th>期待値</th>
    </tr></thead><tbody>${horseRows(r.horses, expanded)}</tbody></table>`;

  if (expanded) {
    return `<div class="pred-race pred-race-open">${head}${table}</div>`;
  }
  return `<details class="pred-race"><summary>${head}</summary>${table}</details>`;
}

function placeSectionHtml(place, races) {
  const nReco = races.filter((r) => r.verdict === "推奨").length;
  const nAttn = races.filter((r) => r.horses.some((h) => h.attention)).length;
  return `<section class="section panel">
    <h2 class="section-head">${place}</h2>
    <p class="pred-summary">${races.length}レース中 推奨<b>${nReco}</b>R・注目<b>${nAttn}</b>R・見送り<b>${races.length - nReco}</b>R。</p>
    ${races.map(raceCard).join("")}
  </section>`;
}

function renderAll(date, items) {
  let html = "";
  let version = null;
  let anyForward = false;
  let snappedAt = null;
  for (const it of items) {
    const races = SiteDB.predictions(date, it.place);
    if (!races) continue;
    if (!version && races[0]) version = races[0].model_version;
    for (const r of races) {
      if (!r.forward) continue;
      anyForward = true;
      if (r.snapped_at && (!snappedAt || r.snapped_at > snappedAt)) {
        snappedAt = r.snapped_at;
      }
    }
    html += placeSectionHtml(it.place, races);
  }
  if (anyForward) {
    // snapped_at: "2026-07-05T08:31:02" → "07/05 08:31"
    const m = snappedAt && snappedAt.match(/\d{4}-(\d{2})-(\d{2})T(\d{2}):(\d{2})/);
    const when = m ? `${m[1]}/${m[2]} ${m[3]}:${m[4]}時点の発売中オッズ` : "発売中オッズのスナップショット";
    html = `<p class="yomi-note forward-note">⏱ <b>発走前の分析です。</b>単勝オッズは${when}で、以降のオッズ変動により市場確率・期待値は変わります(前日夜に公開し、当日朝に更新)。結果確定後、このページは確定オッズ版に更新されます。</p>` + html;
  }
  html += `<section class="section">
    <h2 class="section-head">定義</h2>
    <p class="yomi-foot">市場確率 = 単勝オッズの逆数をレース内で正規化した勝率推定。モデル確率 = 市場確率の対数オッズに、当該馬の相対枠位置グループのバイアス乖離Δを加点して再正規化した値。期待値 = モデル確率 × 単勝オッズ。市場と同一の確率なら期待値は控除率相当(約0.8)に収束するため、閾値1.1超は市場との明確な見解差を意味する。
    現行モデル(v1)のバックテストでは、枠順バイアス由来の歪みはオッズに織り込み済みで有意な優位性は検出されなかった。したがって「推奨」(期待値1.1超)は稀で、見送りが標準的な結論となる。特徴量の拡張は検証を通過したものだけを順次追加する。</p>
    <p class="yomi-foot">「注目」= モデルが市場より+2%以上高く評価したレースの最上位馬。バイアスの追い風が明確な馬を示す相対的なシグナルであり、<b>期待値がプラスであることを意味しない</b>(控除率の壁は超えていない)。注目馬の成績は結果検証ページで継続的に開示する。</p>
    ${version ? `<p class="model-badge">model: ${version}</p>` : ""}
    <p class="yomi-note">⚠️ 本分析は統計的情報の提供であり、的中・収益を保証しません。馬券の購入は20歳以上・自己責任で。</p>
  </section>`;
  $("#report").innerHTML = html || '<p class="loading">この開催日のデータがありません。</p>';
}

async function init() {
  let items;
  try {
    await SiteDB.open();
    items = SiteDB.predItems();
  } catch (e) {
    $("#report").innerHTML = '<p class="loading">データがまだ生成されていません。</p>';
    return;
  }

  if (!items.length) {
    $("#report").innerHTML =
      '<p class="loading">期待値データはまだありません。</p>';
    return;
  }

  if (SiteDB.verifyDates().length > 0) {
    const cta = $("#result-cta");
    if (cta) cta.hidden = false;
  }

  // ?date=YYYYMMDD 指定可。未指定は最新
  const params = new URLSearchParams(window.location.search);
  const requestedDate = params.get("date");
  const dates = [...new Set(items.map((it) => it.date))].sort().reverse();
  const targetDate = requestedDate && dates.includes(requestedDate)
    ? requestedDate
    : dates[0];

  $("#date-display").textContent =
    `${formatDateWithDow(targetDate)} 開催の期待値分析`;

  renderAll(targetDate, items.filter((it) => it.date === targetDate));
}

init();
