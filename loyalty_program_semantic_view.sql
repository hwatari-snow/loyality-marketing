-- セマンティックビュー作成SQL (demo.LMスキーマ)
CREATE OR REPLACE SEMANTIC VIEW demo.LM.LOYALTY_PROGRAM_SV
  COMMENT = 'ロイヤルティプログラム分析用セマンティックレイヤー'
AS SEMANTIC MODEL
$$
name: loyalty_program
description: |
  ロイヤルティプログラムの分析用セマンティックモデル。
  顧客マスタ、POS取引、ポイント履歴を統合し、売上・ポイント分析が可能。

tables:
  - name: customer_master
    description: 会員マスタ情報
    base_table:
      database: DEMO
      schema: LM
      table: CUSTOMER_MASTER
    primary_key:
      columns:
        - CUSTOMER_ID
    dimensions:
      - name: customer_id
        description: 顧客ID
        expr: CUSTOMER_ID
        data_type: VARCHAR
      - name: customer_name
        description: 顧客名
        expr: CUSTOMER_NAME
        data_type: VARCHAR
      - name: gender
        description: 性別
        expr: GENDER
        data_type: VARCHAR
      - name: age
        description: 年齢
        expr: AGE
        data_type: NUMBER
      - name: birth_date
        description: 生年月日
        expr: BIRTH_DATE
        data_type: DATE
      - name: prefecture
        description: 都道府県
        expr: PREFECTURE
        data_type: VARCHAR
      - name: city
        description: 市区町村
        expr: CITY
        data_type: VARCHAR
      - name: enrollment_date
        description: 入会日
        expr: ENROLLMENT_DATE
        data_type: DATE
      - name: membership_rank
        description: 会員ランク
        expr: MEMBERSHIP_RANK
        data_type: VARCHAR
      - name: occupation
        description: 職業
        expr: OCCUPATION
        data_type: VARCHAR
      - name: income_range
        description: 年収帯
        expr: INCOME_RANGE
        data_type: VARCHAR
      - name: enrollment_channel
        description: 入会チャネル
        expr: ENROLLMENT_CHANNEL
        data_type: VARCHAR
      - name: membership_status
        description: 会員ステータス
        expr: MEMBERSHIP_STATUS
        data_type: VARCHAR
    facts:
      - name: total_points
        description: 保有ポイント
        expr: TOTAL_POINTS
        data_type: NUMBER
      - name: dm_consent_flag
        description: DM同意フラグ
        expr: DM_CONSENT_FLAG
        data_type: NUMBER
      - name: app_usage_flag
        description: アプリ利用フラグ
        expr: APP_USAGE_FLAG
        data_type: NUMBER

  - name: id_pos_transaction
    description: ID-POS取引データ
    base_table:
      database: DEMO
      schema: LM
      table: ID_POS_TRANSACTION
    primary_key:
      columns:
        - TRANSACTION_ID
    dimensions:
      - name: transaction_id
        description: 取引ID
        expr: TRANSACTION_ID
        data_type: VARCHAR
      - name: receipt_id
        description: レシートID
        expr: RECEIPT_ID
        data_type: VARCHAR
      - name: customer_id
        description: 顧客ID
        expr: CUSTOMER_ID
        data_type: VARCHAR
      - name: transaction_date
        description: 取引日
        expr: TRANSACTION_DATE
        data_type: DATE
      - name: transaction_time
        description: 取引時刻
        expr: TRANSACTION_TIME
        data_type: TIME
      - name: channel_type
        description: チャネル種別
        expr: CHANNEL_TYPE
        data_type: VARCHAR
      - name: partner_name
        description: 提携先名
        expr: PARTNER_NAME
        data_type: VARCHAR
      - name: store_id
        description: 店舗ID
        expr: STORE_ID
        data_type: VARCHAR
      - name: store_name
        description: 店舗名
        expr: STORE_NAME
        data_type: VARCHAR
      - name: product_category
        description: 商品カテゴリ
        expr: PRODUCT_CATEGORY
        data_type: VARCHAR
      - name: product_name
        description: 商品名
        expr: PRODUCT_NAME
        data_type: VARCHAR
      - name: payment_method
        description: 支払方法
        expr: PAYMENT_METHOD
        data_type: VARCHAR
    facts:
      - name: quantity
        description: 数量
        expr: QUANTITY
        data_type: NUMBER
      - name: unit_price
        description: 単価
        expr: UNIT_PRICE
        data_type: NUMBER
      - name: total_amount
        description: 合計金額
        expr: TOTAL_AMOUNT
        data_type: NUMBER
      - name: points_earned
        description: 獲得ポイント
        expr: POINTS_EARNED
        data_type: NUMBER
      - name: points_used
        description: 利用ポイント
        expr: POINTS_USED
        data_type: NUMBER

  - name: point_history
    description: ポイント履歴
    base_table:
      database: DEMO
      schema: LM
      table: POINT_HISTORY
    primary_key:
      columns:
        - POINT_ID
    dimensions:
      - name: point_id
        description: ポイントID
        expr: POINT_ID
        data_type: VARCHAR
      - name: customer_id
        description: 顧客ID
        expr: CUSTOMER_ID
        data_type: VARCHAR
      - name: event_date
        description: イベント日
        expr: EVENT_DATE
        data_type: DATE
      - name: event_type
        description: イベント種別（付与/利用/失効）
        expr: EVENT_TYPE
        data_type: VARCHAR
      - name: source_type
        description: ソース種別
        expr: SOURCE_TYPE
        data_type: VARCHAR
      - name: source_detail
        description: ソース詳細
        expr: SOURCE_DETAIL
        data_type: VARCHAR
      - name: transaction_id
        description: 取引ID
        expr: TRANSACTION_ID
        data_type: VARCHAR
      - name: expiration_date
        description: 有効期限
        expr: EXPIRATION_DATE
        data_type: DATE
      - name: note
        description: 備考
        expr: NOTE
        data_type: VARCHAR
    facts:
      - name: points
        description: ポイント数
        expr: POINTS
        data_type: NUMBER
      - name: balance_after
        description: 変更後残高
        expr: BALANCE_AFTER
        data_type: NUMBER

relationships:
  - name: customer_to_transaction
    left_table: customer_master
    right_table: id_pos_transaction
    relationship_columns:
      - left_column: CUSTOMER_ID
        right_column: CUSTOMER_ID
    join_type: left_outer
    relationship_type: one_to_many
  - name: customer_to_point_history
    left_table: customer_master
    right_table: point_history
    relationship_columns:
      - left_column: CUSTOMER_ID
        right_column: CUSTOMER_ID
    join_type: left_outer
    relationship_type: one_to_many
  - name: transaction_to_point_history
    left_table: id_pos_transaction
    right_table: point_history
    relationship_columns:
      - left_column: TRANSACTION_ID
        right_column: TRANSACTION_ID
    join_type: left_outer
    relationship_type: one_to_many

verified_queries:
  - name: 月別売上
    question: 月別の売上金額を教えてください
    verified_at: 1710489600
    verified_by: admin
    sql: |
      SELECT 
        DATE_TRUNC('MONTH', t.TRANSACTION_DATE) AS 月,
        SUM(t.TOTAL_AMOUNT) AS 売上金額
      FROM DEMO.LM.ID_POS_TRANSACTION t
      GROUP BY 1
      ORDER BY 1
  - name: 会員ランク別売上
    question: 会員ランク別の売上を教えてください
    verified_at: 1710489600
    verified_by: admin
    sql: |
      SELECT 
        c.MEMBERSHIP_RANK AS 会員ランク,
        COUNT(DISTINCT c.CUSTOMER_ID) AS 会員数,
        SUM(t.TOTAL_AMOUNT) AS 売上金額
      FROM DEMO.LM.CUSTOMER_MASTER c
      LEFT JOIN DEMO.LM.ID_POS_TRANSACTION t ON c.CUSTOMER_ID = t.CUSTOMER_ID
      GROUP BY 1
      ORDER BY 3 DESC
$$;
