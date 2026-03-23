-- ============================================================
-- ロイヤルティマーケティング デモ - 一括セットアップスクリプト
-- ============================================================
-- GitHub リポジトリから Git Integration 経由で CSV を取得し、
-- DB・スキーマ・テーブル作成・データロード・セマンティックビュー作成まで一括実行
-- ============================================================

-- ============================================================
-- Step 1: 初期設定
-- ============================================================

USE ROLE ACCOUNTADMIN;

ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'ANY_REGION';

CREATE WAREHOUSE IF NOT EXISTS LM_WH
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for Loyalty Marketing Demo';

USE WAREHOUSE LM_WH;

-- ============================================================
-- Step 2: データベース・スキーマ作成
-- ============================================================

CREATE DATABASE IF NOT EXISTS DEMO;
CREATE SCHEMA IF NOT EXISTS DEMO.LM;

USE DATABASE DEMO;
USE SCHEMA LM;

-- ============================================================
-- Step 3: Git Integration によるデータ取得
-- ============================================================

CREATE OR REPLACE STAGE LM_DATA_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for Loyalty Marketing Demo data';

CREATE OR REPLACE API INTEGRATION lm_git_api_integration
    API_PROVIDER = git_https_api
    API_ALLOWED_PREFIXES = ('https://github.com/')
    ENABLED = TRUE;

CREATE OR REPLACE GIT REPOSITORY LM_GIT_REPO
    API_INTEGRATION = lm_git_api_integration
    ORIGIN = 'https://github.com/hwatari-snow/loyality-marketing.git';

ALTER GIT REPOSITORY LM_GIT_REPO FETCH;

LIST @LM_GIT_REPO/branches/main/csv/;

COPY FILES INTO @LM_DATA_STAGE/csv/
    FROM @LM_GIT_REPO/branches/main/csv/
    PATTERN = '.*\.csv$';

LIST @LM_DATA_STAGE/csv/;

-- ============================================================
-- Step 4: ファイルフォーマット作成
-- ============================================================

CREATE OR REPLACE FILE FORMAT LM_CSV_FORMAT
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL')
    ENCODING = 'UTF8';

-- ============================================================
-- Step 5: テーブル作成
-- ============================================================

-- 顧客マスタ（10,000件）
CREATE OR REPLACE TABLE CUSTOMER_MASTER (
    CUSTOMER_ID        VARCHAR(20)   NOT NULL PRIMARY KEY,
    CUSTOMER_NAME      VARCHAR(100)  NOT NULL,
    GENDER             VARCHAR(10),
    AGE                NUMBER(3,0),
    BIRTH_DATE         DATE,
    PREFECTURE         VARCHAR(20),
    CITY               VARCHAR(50),
    ENROLLMENT_DATE    DATE,
    TOTAL_POINTS       NUMBER(10,0),
    MEMBERSHIP_RANK    VARCHAR(20),
    OCCUPATION         VARCHAR(30),
    INCOME_RANGE       VARCHAR(30),
    EMAIL              VARCHAR(100),
    PHONE_NUMBER       VARCHAR(20),
    ENROLLMENT_CHANNEL VARCHAR(20),
    LAST_PURCHASE_DATE DATE,
    MEMBERSHIP_STATUS  VARCHAR(20),
    DM_CONSENT_FLAG    NUMBER(1,0),
    APP_USAGE_FLAG     NUMBER(1,0)
);

-- ID-POSトランザクション（100,000件）
CREATE OR REPLACE TABLE ID_POS_TRANSACTION (
    TRANSACTION_ID    VARCHAR(20)   NOT NULL PRIMARY KEY,
    RECEIPT_ID        VARCHAR(20)   NOT NULL,
    CUSTOMER_ID       VARCHAR(20)   NOT NULL,
    TRANSACTION_DATE  DATE          NOT NULL,
    TRANSACTION_TIME  TIME,
    CHANNEL_TYPE      VARCHAR(20),
    PARTNER_NAME      VARCHAR(50),
    STORE_ID          VARCHAR(20),
    STORE_NAME        VARCHAR(100),
    PRODUCT_CATEGORY  VARCHAR(50),
    PRODUCT_NAME      VARCHAR(100),
    QUANTITY          NUMBER(5,0)   DEFAULT 1,
    UNIT_PRICE        NUMBER(10,0),
    TOTAL_AMOUNT      NUMBER(10,0),
    POINTS_EARNED     NUMBER(10,0)  DEFAULT 0,
    POINTS_USED       NUMBER(10,0)  DEFAULT 0,
    PAYMENT_METHOD    VARCHAR(30),
    FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMER_MASTER(CUSTOMER_ID)
);

-- ポイント履歴（119,003件）
CREATE OR REPLACE TABLE POINT_HISTORY (
    POINT_ID          VARCHAR(20)   NOT NULL PRIMARY KEY,
    CUSTOMER_ID       VARCHAR(20)   NOT NULL,
    EVENT_DATE        DATE          NOT NULL,
    EVENT_TYPE        VARCHAR(20),
    POINTS            NUMBER(10,0),
    BALANCE_AFTER     NUMBER(10,0),
    SOURCE_TYPE       VARCHAR(30),
    SOURCE_DETAIL     VARCHAR(50),
    TRANSACTION_ID    VARCHAR(20),
    EXPIRATION_DATE   DATE,
    NOTE              VARCHAR(200),
    FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMER_MASTER(CUSTOMER_ID)
);

-- ============================================================
-- Step 6: データロード
-- ============================================================

-- 顧客マスタ（FK参照元のため先にロード）
COPY INTO CUSTOMER_MASTER
FROM @LM_DATA_STAGE/csv/
FILE_FORMAT = (FORMAT_NAME = 'LM_CSV_FORMAT')
PATTERN = '.*customer_master.*\.csv.*'
ON_ERROR = 'CONTINUE';

-- ID-POSトランザクション
COPY INTO ID_POS_TRANSACTION
FROM @LM_DATA_STAGE/csv/
FILE_FORMAT = (FORMAT_NAME = 'LM_CSV_FORMAT')
PATTERN = '.*transactions.*\.csv.*'
ON_ERROR = 'CONTINUE';

-- ポイント履歴
COPY INTO POINT_HISTORY
FROM @LM_DATA_STAGE/csv/
FILE_FORMAT = (FORMAT_NAME = 'LM_CSV_FORMAT')
PATTERN = '.*point_history.*\.csv.*'
ON_ERROR = 'CONTINUE';

-- ============================================================
-- Step 7: セマンティックビュー作成（Cortex Analyst 用）
-- ============================================================

create or replace semantic view DEMO.LM.LOYALTY_PROGRAM_SV
	tables (
		DEMO.LM.CUSTOMER_MASTER primary key (CUSTOMER_ID) comment='会員マスタ情報',
		DEMO.LM.ID_POS_TRANSACTION primary key (TRANSACTION_ID) comment='ID-POS取引データ',
		DEMO.LM.POINT_HISTORY primary key (POINT_ID) comment='ポイント履歴'
	)
	relationships (
		TRANSACTION_TO_CUSTOMER as ID_POS_TRANSACTION(CUSTOMER_ID) references CUSTOMER_MASTER(CUSTOMER_ID),
		POINT_HISTORY_TO_CUSTOMER as POINT_HISTORY(CUSTOMER_ID) references CUSTOMER_MASTER(CUSTOMER_ID),
		POINT_HISTORY_TO_TRANSACTION as POINT_HISTORY(TRANSACTION_ID) references ID_POS_TRANSACTION(TRANSACTION_ID)
	)
	facts (
		CUSTOMER_MASTER.APP_USAGE_FLAG as APP_USAGE_FLAG comment='アプリ利用フラグ',
		CUSTOMER_MASTER.DM_CONSENT_FLAG as DM_CONSENT_FLAG comment='DM同意フラグ',
		CUSTOMER_MASTER.TOTAL_POINTS as TOTAL_POINTS comment='保有ポイント',
		ID_POS_TRANSACTION.POINTS_EARNED as POINTS_EARNED comment='獲得ポイント',
		ID_POS_TRANSACTION.POINTS_USED as POINTS_USED comment='利用ポイント',
		ID_POS_TRANSACTION.QUANTITY as QUANTITY comment='数量',
		ID_POS_TRANSACTION.TOTAL_AMOUNT as TOTAL_AMOUNT comment='合計金額',
		ID_POS_TRANSACTION.UNIT_PRICE as UNIT_PRICE comment='単価',
		POINT_HISTORY.BALANCE_AFTER as BALANCE_AFTER comment='変更後残高',
		POINT_HISTORY.POINTS as POINTS comment='ポイント数'
	)
	dimensions (
		CUSTOMER_MASTER.AGE as AGE comment='年齢',
		CUSTOMER_MASTER.BIRTH_DATE as BIRTH_DATE comment='生年月日',
		CUSTOMER_MASTER.CITY as CITY comment='市区町村',
		CUSTOMER_MASTER.CUSTOMER_ID as CUSTOMER_ID comment='顧客ID',
		CUSTOMER_MASTER.CUSTOMER_NAME as CUSTOMER_NAME comment='顧客名',
		CUSTOMER_MASTER.ENROLLMENT_CHANNEL as ENROLLMENT_CHANNEL comment='入会チャネル',
		CUSTOMER_MASTER.ENROLLMENT_DATE as ENROLLMENT_DATE comment='入会日',
		CUSTOMER_MASTER.GENDER as GENDER comment='性別',
		CUSTOMER_MASTER.INCOME_RANGE as INCOME_RANGE comment='年収帯',
		CUSTOMER_MASTER.MEMBERSHIP_RANK as MEMBERSHIP_RANK comment='会員ランク',
		CUSTOMER_MASTER.MEMBERSHIP_STATUS as MEMBERSHIP_STATUS comment='会員ステータス',
		CUSTOMER_MASTER.OCCUPATION as OCCUPATION comment='職業',
		CUSTOMER_MASTER.PREFECTURE as PREFECTURE comment='都道府県',
		ID_POS_TRANSACTION.CHANNEL_TYPE as CHANNEL_TYPE comment='チャネル種別',
		ID_POS_TRANSACTION.CUSTOMER_ID as CUSTOMER_ID comment='顧客ID',
		ID_POS_TRANSACTION.PARTNER_NAME as PARTNER_NAME comment='提携先名',
		ID_POS_TRANSACTION.PAYMENT_METHOD as PAYMENT_METHOD comment='支払方法',
		ID_POS_TRANSACTION.PRODUCT_CATEGORY as PRODUCT_CATEGORY comment='商品カテゴリ',
		ID_POS_TRANSACTION.PRODUCT_NAME as PRODUCT_NAME comment='商品名',
		ID_POS_TRANSACTION.RECEIPT_ID as RECEIPT_ID comment='レシートID',
		ID_POS_TRANSACTION.STORE_ID as STORE_ID comment='店舗ID',
		ID_POS_TRANSACTION.STORE_NAME as STORE_NAME comment='店舗名',
		ID_POS_TRANSACTION.TRANSACTION_DATE as TRANSACTION_DATE comment='取引日',
		ID_POS_TRANSACTION.TRANSACTION_ID as TRANSACTION_ID comment='取引ID',
		ID_POS_TRANSACTION.TRANSACTION_TIME as TRANSACTION_TIME comment='取引時刻',
		POINT_HISTORY.CUSTOMER_ID as CUSTOMER_ID comment='顧客ID',
		POINT_HISTORY.EVENT_DATE as EVENT_DATE comment='イベント日',
		POINT_HISTORY.EVENT_TYPE as EVENT_TYPE comment='イベント種別（付与/利用/失効）',
		POINT_HISTORY.EXPIRATION_DATE as EXPIRATION_DATE comment='有効期限',
		POINT_HISTORY.NOTE as NOTE comment='備考',
		POINT_HISTORY.POINT_ID as POINT_ID comment='ポイントID',
		POINT_HISTORY.SOURCE_DETAIL as SOURCE_DETAIL comment='ソース詳細',
		POINT_HISTORY.SOURCE_TYPE as SOURCE_TYPE comment='ソース種別',
		POINT_HISTORY.TRANSACTION_ID as TRANSACTION_ID comment='取引ID'
	)
	comment='ロイヤルティプログラムの分析用セマンティックモデル。
顧客マスタ、POS取引、ポイント履歴を統合し、売上・ポイント分析が可能。
'
	with extension (CA='{"tables":[{"name":"CUSTOMER_MASTER","dimensions":[{"name":"AGE"},{"name":"BIRTH_DATE"},{"name":"CITY"},{"name":"CUSTOMER_ID"},{"name":"CUSTOMER_NAME"},{"name":"ENROLLMENT_CHANNEL"},{"name":"ENROLLMENT_DATE"},{"name":"GENDER"},{"name":"INCOME_RANGE"},{"name":"MEMBERSHIP_RANK"},{"name":"MEMBERSHIP_STATUS"},{"name":"OCCUPATION"},{"name":"PREFECTURE"}],"facts":[{"name":"APP_USAGE_FLAG"},{"name":"DM_CONSENT_FLAG"},{"name":"TOTAL_POINTS"}]},{"name":"ID_POS_TRANSACTION","dimensions":[{"name":"CHANNEL_TYPE"},{"name":"CUSTOMER_ID"},{"name":"PARTNER_NAME"},{"name":"PAYMENT_METHOD"},{"name":"PRODUCT_CATEGORY"},{"name":"PRODUCT_NAME"},{"name":"RECEIPT_ID"},{"name":"STORE_ID"},{"name":"STORE_NAME"},{"name":"TRANSACTION_DATE"},{"name":"TRANSACTION_ID"},{"name":"TRANSACTION_TIME"}],"facts":[{"name":"POINTS_EARNED"},{"name":"POINTS_USED"},{"name":"QUANTITY"},{"name":"TOTAL_AMOUNT"},{"name":"UNIT_PRICE"}]},{"name":"POINT_HISTORY","dimensions":[{"name":"CUSTOMER_ID"},{"name":"EVENT_DATE"},{"name":"EVENT_TYPE"},{"name":"EXPIRATION_DATE"},{"name":"NOTE"},{"name":"POINT_ID"},{"name":"SOURCE_DETAIL"},{"name":"SOURCE_TYPE"},{"name":"TRANSACTION_ID"}],"facts":[{"name":"BALANCE_AFTER"},{"name":"POINTS"}]}],"relationships":[{"name":"TRANSACTION_TO_CUSTOMER"},{"name":"POINT_HISTORY_TO_CUSTOMER"},{"name":"POINT_HISTORY_TO_TRANSACTION"}],"verified_queries":[{"name":"\\"月別売上\\"","sql":"SELECT \\n  DATE_TRUNC(''MONTH'', t.TRANSACTION_DATE) AS \\"月\\",\\n  SUM(t.TOTAL_AMOUNT) AS \\"売上金額\\"\\nFROM DEMO.LM.ID_POS_TRANSACTION t\\nGROUP BY 1\\nORDER BY 1\\n","question":"月別の売上金額を教えてください","verified_at":1710489600,"verified_by":"admin"},{"name":"\\"会員ランク別売上\\"","sql":"SELECT \\n  c.MEMBERSHIP_RANK AS \\"会員ランク\\",\\n  COUNT(DISTINCT c.CUSTOMER_ID) AS \\"会員数\\",\\n  SUM(t.TOTAL_AMOUNT) AS \\"売上金額\\"\\nFROM DEMO.LM.CUSTOMER_MASTER c\\nLEFT JOIN DEMO.LM.ID_POS_TRANSACTION t ON c.CUSTOMER_ID = t.CUSTOMER_ID\\nGROUP BY 1\\nORDER BY 3 DESC\\n","question":"会員ランク別の売上を教えてください","verified_at":1710489600,"verified_by":"admin"}]}');

-- ============================================================
-- Step 8: Streamlit アプリ デプロイ
-- ============================================================

CREATE OR REPLACE STAGE LM_STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_FULL')
    COMMENT = 'Stage for Loyalty Marketing Streamlit app';

ALTER GIT REPOSITORY LM_GIT_REPO FETCH;

COPY FILES INTO @LM_STREAMLIT_STAGE/
    FROM @LM_GIT_REPO/branches/main/
    FILES = ('streamlit_app.py', 'environment.yml');

COPY FILES INTO @LM_STREAMLIT_STAGE/.streamlit/
    FROM @LM_GIT_REPO/branches/main/.streamlit/
    FILES = ('config.toml');

LIST @LM_STREAMLIT_STAGE;

CREATE OR REPLACE STREAMLIT DEMO.LM.CUSTOMER_DASHBOARD
    ROOT_LOCATION = '@DEMO.LM.LM_STREAMLIT_STAGE'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = LM_WH
    COMMENT = 'ロイヤルティマーケティング 顧客抽出ダッシュボード';

-- ============================================================
-- Step 9: データ確認
-- ============================================================

SELECT 'CUSTOMER_MASTER'    AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM CUSTOMER_MASTER
UNION ALL
SELECT 'ID_POS_TRANSACTION', COUNT(*) FROM ID_POS_TRANSACTION
UNION ALL
SELECT 'POINT_HISTORY',      COUNT(*) FROM POINT_HISTORY;
