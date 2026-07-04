# mamahuhu X auto-poster

毎週金・土の20時(JST)に、翌日のG3以上のメインレースを netkeiba から取得し、
mamahuhu の直近開催結果データ (バイアス + 好調騎手) を組み合わせて X に投稿する bot。

(バイアスデータ site.db は土日18時に生成・コミットされるため、その後の20時に走らせる)

メイン投稿のリプライで [mamahuhu](https://mamahuhu.app/) の URL を貼ります。

## 投稿例

```
#ヴィクトリアマイル のバイアスは...
「外枠の差し追込」

Ｃ．ルメール騎手、川田将雅騎手が好調なので要注意!

詳しい集計データはツリーから
```

(リプライ) `https://mamahuhu.app/`

## セットアップ

### 1. X Developer 登録 ($5チャージ必要)

1. https://developer.x.com にアクセス → コンソールへ
2. 利用目的を英文で記入(自分のアカウントへの定期投稿である旨)
3. アプリ作成 → **User authentication settings** で `Read and Write` 権限に
4. API Key / Secret と Access Token / Secret を取得
5. クレジットを $5 チャージ (Auto Recharge は OFF推奨)

### 2. GitHub Secrets 設定

リポジトリの Settings → Secrets and variables → Actions で以下を登録：

- `X_API_KEY`
- `X_API_SECRET`
- `X_ACCESS_TOKEN`
- `X_ACCESS_SECRET`

### 3. テスト実行

GitHub の Actions タブ → `post-to-x` → Run workflow → `dry_run = true` で実行。
ログで投稿内容を確認できます。問題なければ実投稿へ。

## 動作の流れ

1. cron で金・土 20:00 JST に起動 (`.github/workflows/post.yml`)
2. netkeiba の翌日レース一覧から G3 以上をフィルタ
3. 開催場ごとのバイアスデータを同一リポジトリの `public/data/site.db` (SQLite) から読む
   - **土曜20時投稿** (=日曜レース): 当日(土)のデータ。当日18時生成・コミット済みのもの
   - **金曜20時投稿** (=土曜レース): 先週日曜(5日前)のデータ
   - データが無い開催場はスキップ。フォールバックなし
4. 各レースの出馬表から騎手と芝/ダートを取得
5. その芝/ダートの `best_combo` (内外+脚質) からバイアス文字列を生成
6. site.db の `hot_jockeys` 全員と照合し、出走中の好調騎手をピックアップ
7. テンプレに沿って投稿 → リプライで mamahuhu URL

## コスト試算

X API Pay-Per-Use:
- メイン投稿: $0.015
- URLリプライ: $0.01
- 合計: 約$0.025/レース

週末G3以上が3レースある場合: 月 $0.30 程度。$5チャージで1年以上もちます。

## スクレイピングの注意

- netkeibaの構造が変わると `parse_graded_races` / `fetch_race_meta` の selector
  を調整する必要があります
- リクエスト間に2秒sleepを入れています
- 出馬表は前日夕方くらいに確定するので、金土20時の実行で問題ないはず

## 手動でログを見たいとき

```bash
DRY_RUN=1 python post.py
```

(X認証情報の環境変数は不要 - dry_run時は投稿しないため)

### 任意の日付でテストする

`--date YYYYMMDD` で実行日を上書きできます。曜日判定とデータ参照日もそれに合わせて変わります。

```bash
# 金曜投稿(=土曜レース)を再現
DRY_RUN=1 python post.py --date 20260522

# 土曜投稿(=日曜レース)を再現
DRY_RUN=1 python post.py --date 20260523
```

GitHub Actions の `Run workflow` でも `date` 欄に `20260522` のように入れればOK。
`dry_run` のチェックも合わせてONにしておくとX投稿はされません。
