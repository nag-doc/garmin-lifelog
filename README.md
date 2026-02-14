# Garmin Health Data Sync

Garminの健康データを毎日Google Sheetsに自動同期するシステムです。

## 仕組み
- 毎日 **朝 6:00 (JST)** に GitHub Actions が実行されます。
- 前日と当日のデータを取得し、Spreadsheetに追記・更新します。

## 設定 (Secrets)
このリポジトリの Settings > Secrets and variables > Actions に以下を設定しています。
- \`GARMIN_TOKENS\`: ログインセッション情報 (export_tokens.pyで生成)
- \`SERVICE_ACCOUNT_JSON\`: Google Sheets接続用キー
- \`SPREADSHEET_ID\`: 書き込み先のシートID

## トークンの更新方法 (半年後など)
もし自動実行がエラー(Login failed)で止まった場合は、以下の手順でトークンを更新してください。

1. このリポジトリを手元のPCやCloud Shellにクローンする。
2. \`python3 export_tokens.py\` を実行し、メール/パスワード/MFAを入力。
3. 表示された長い文字列をコピーする。
4. GitHubの Secrets (\`GARMIN_TOKENS\`) を、その文字列で上書き更新する。
5. Actionsタブから手動実行して直ったか確認する。
