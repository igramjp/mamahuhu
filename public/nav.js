// link-button と フッターナビの href に現URLのクエリを引き継ぐ。
// ?date=YYYYMMDD 等のパラメータをページ間で保持するため。
(function () {
  const qs = window.location.search;
  if (!qs || qs === '?') return;

  const links = document.querySelectorAll('.link-button a, .nav-links a');
  for (const a of links) {
    const href = a.getAttribute('href');
    if (!href) continue;
    if (/^(https?:|mailto:|tel:|#)/i.test(href)) continue;
    if (href.includes('?')) continue;
    a.setAttribute('href', href + qs);
  }
})();
