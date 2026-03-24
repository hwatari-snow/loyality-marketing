# Snowflake Streamlitデモ

顧客・取引・ポイントデータを Snowflake に構築し、Streamlit ダッシュボードで顧客抽出を行うデモ環境です。

## セットアップ手順

### 1. setup.sql を実行

Snowflake の SQL Worksheet を開き、`setup.sql` の内容を **すべて** 貼り付けて実行してください。

以下が自動で行われます：

- Warehouse / Database / Schema の作成
- GitHub リポジトリとの連携（Git Integration）
- CSV データの取得・テーブル作成・データロード
- セマンティックビューの作成（Cortex Analyst 用）

### 2. Streamlit アプリを作成

Snowflake の左メニューから **Streamlit** を開き、新しい Streamlit アプリを作成します。

- データベース: `DEMO`
- スキーマ: `LM`
- Warehouse: `LM_WH`

エディタが開いたら、`streamlit_app.py` の内容を **すべてコピー＆ペースト** してください。

### 3. パッケージの追加

Streamlit エディタ左側のパッケージ管理から **plotly** を追加してください。

### 4. 完了

アプリが起動し、顧客抽出ダッシュボードが表示されます。

## データセット

| テーブル | 件数 | 内容 |
|---|---|---|
| CUSTOMER_MASTER | 10,000 | 会員マスタ（属性・ランク・ポイント等） |
| ID_POS_TRANSACTION | 100,000 | ID-POS 取引データ |
| POINT_HISTORY | 119,003 | ポイント付与・利用・失効履歴 |
