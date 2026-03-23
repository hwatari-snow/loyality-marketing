import streamlit as st
import pandas as pd
import plotly.graph_objects as go

st.set_page_config(
    page_title="顧客抽出ダッシュボード",
    page_icon=":material/target:",
    layout="wide"
)

conn = st.connection("snowflake")

def get_filtered_values(table, column, existing_filters):
    conditions = []
    for col, config in existing_filters.items():
        if col == column:
            continue
        if config["type"] == "multi" and config["value"]:
            escaped = "', '".join([str(v).replace("'", "''") for v in config["value"]])
            conditions.append(f"{col} IN ('{escaped}')")
        elif config["type"] == "range" and config["value"]:
            min_v, max_v = config["value"]
            conditions.append(f"{col} BETWEEN {min_v} AND {max_v}")
        elif config["type"] == "date_range" and config["value"]:
            min_d, max_d = config["value"]
            conditions.append(f"{col} BETWEEN '{min_d}' AND '{max_d}'")
    
    where = " AND ".join(conditions) if conditions else "1=1"
    df = conn.query(f"SELECT DISTINCT {column} FROM DEMO.LM.{table} WHERE {column} IS NOT NULL AND {where} ORDER BY {column}")
    return df[column].tolist()

def get_filtered_range(table, column, existing_filters):
    conditions = []
    for col, config in existing_filters.items():
        if col == column:
            continue
        if config["type"] == "multi" and config["value"]:
            escaped = "', '".join([str(v).replace("'", "''") for v in config["value"]])
            conditions.append(f"{col} IN ('{escaped}')")
        elif config["type"] == "range" and config["value"]:
            min_v, max_v = config["value"]
            conditions.append(f"{col} BETWEEN {min_v} AND {max_v}")
    
    where = " AND ".join(conditions) if conditions else "1=1"
    df = conn.query(f"SELECT MIN({column}) as min_val, MAX({column}) as max_val FROM DEMO.LM.{table} WHERE {where}")
    return int(df["MIN_VAL"].iloc[0] or 0), int(df["MAX_VAL"].iloc[0] or 0)

def get_filtered_date_range(table, column, existing_filters):
    conditions = []
    for col, config in existing_filters.items():
        if col == column:
            continue
        if config["type"] == "multi" and config["value"]:
            escaped = "', '".join([str(v).replace("'", "''") for v in config["value"]])
            conditions.append(f"{col} IN ('{escaped}')")
    
    where = " AND ".join(conditions) if conditions else "1=1"
    df = conn.query(f"SELECT MIN({column}) as min_val, MAX({column}) as max_val FROM DEMO.LM.{table} WHERE {where}")
    return df["MIN_VAL"].iloc[0], df["MAX_VAL"].iloc[0]

def build_conditions(filters):
    conditions = []
    for col, config in filters.items():
        if config["type"] == "multi" and config["value"]:
            escaped = "', '".join([str(v).replace("'", "''") for v in config["value"]])
            conditions.append(f"{col} IN ('{escaped}')")
        elif config["type"] == "range" and config["value"]:
            min_v, max_v = config["value"]
            conditions.append(f"{col} BETWEEN {min_v} AND {max_v}")
        elif config["type"] == "date_range" and config["value"]:
            min_d, max_d = config["value"]
            conditions.append(f"{col} BETWEEN '{min_d}' AND '{max_d}'")
    return conditions

def get_customer_ids(table, conditions):
    where = " AND ".join(conditions) if conditions else "1=1"
    df = conn.query(f"SELECT DISTINCT CUSTOMER_ID FROM DEMO.LM.{table} WHERE {where}")
    return set(df["CUSTOMER_ID"].tolist())

@st.cache_data(ttl=600)
def get_total_customers():
    df = conn.query("SELECT COUNT(DISTINCT CUSTOMER_ID) as cnt FROM DEMO.LM.CUSTOMER_MASTER")
    return int(df["CNT"].iloc[0])

for key in ["cm_filters", "pos_filters", "ph_filters"]:
    if key not in st.session_state:
        st.session_state[key] = {}

def update_filter(filter_dict, key, filter_type, value, default_value=None):
    if filter_type == "multi":
        if value:
            filter_dict[key] = {"type": "multi", "value": value}
        elif key in filter_dict:
            del filter_dict[key]
    elif filter_type == "range":
        if value != default_value:
            filter_dict[key] = {"type": "range", "value": value}
        elif key in filter_dict:
            del filter_dict[key]
    elif filter_type == "date_range":
        if len(value) == 2 and value != default_value:
            filter_dict[key] = {"type": "date_range", "value": value}
        elif key in filter_dict:
            del filter_dict[key]

with st.sidebar:
    st.title(":material/filter_list: フィルター")
    st.caption(":material/info: 選択に応じて選択肢が絞り込まれます")
    
    with st.expander(":material/person: 顧客マスタ", expanded=True, icon=":material/person:"):
        vals = get_filtered_values("CUSTOMER_MASTER", "MEMBERSHIP_RANK", st.session_state.cm_filters)
        sel = st.multiselect("会員ランク", vals, key="cm_rank")
        update_filter(st.session_state.cm_filters, "MEMBERSHIP_RANK", "multi", sel)
        
        vals = get_filtered_values("CUSTOMER_MASTER", "MEMBERSHIP_STATUS", st.session_state.cm_filters)
        sel = st.multiselect("会員ステータス", vals, key="cm_status")
        update_filter(st.session_state.cm_filters, "MEMBERSHIP_STATUS", "multi", sel)
        
        vals = get_filtered_values("CUSTOMER_MASTER", "GENDER", st.session_state.cm_filters)
        sel = st.multiselect("性別", vals, key="cm_gender")
        update_filter(st.session_state.cm_filters, "GENDER", "multi", sel)
        
        min_age, max_age = get_filtered_range("CUSTOMER_MASTER", "AGE", st.session_state.cm_filters)
        if min_age < max_age:
            age_range = st.slider("年齢", min_age, max_age, (min_age, max_age), key="cm_age")
            update_filter(st.session_state.cm_filters, "AGE", "range", age_range, (min_age, max_age))
        
        vals = get_filtered_values("CUSTOMER_MASTER", "PREFECTURE", st.session_state.cm_filters)
        sel = st.multiselect("都道府県", vals, key="cm_pref")
        update_filter(st.session_state.cm_filters, "PREFECTURE", "multi", sel)
        
        vals = get_filtered_values("CUSTOMER_MASTER", "CITY", st.session_state.cm_filters)
        sel = st.multiselect("市区町村", vals, key="cm_city")
        update_filter(st.session_state.cm_filters, "CITY", "multi", sel)
        
        vals = get_filtered_values("CUSTOMER_MASTER", "OCCUPATION", st.session_state.cm_filters)
        sel = st.multiselect("職業", vals, key="cm_occ")
        update_filter(st.session_state.cm_filters, "OCCUPATION", "multi", sel)
        
        vals = get_filtered_values("CUSTOMER_MASTER", "INCOME_RANGE", st.session_state.cm_filters)
        sel = st.multiselect("収入帯", vals, key="cm_income")
        update_filter(st.session_state.cm_filters, "INCOME_RANGE", "multi", sel)
        
        vals = get_filtered_values("CUSTOMER_MASTER", "ENROLLMENT_CHANNEL", st.session_state.cm_filters)
        sel = st.multiselect("入会チャネル", vals, key="cm_channel")
        update_filter(st.session_state.cm_filters, "ENROLLMENT_CHANNEL", "multi", sel)
        
        min_pts, max_pts = get_filtered_range("CUSTOMER_MASTER", "TOTAL_POINTS", st.session_state.cm_filters)
        if min_pts < max_pts:
            pts_range = st.slider("累計ポイント", min_pts, max_pts, (min_pts, max_pts), key="cm_pts")
            update_filter(st.session_state.cm_filters, "TOTAL_POINTS", "range", pts_range, (min_pts, max_pts))
        
        vals = get_filtered_values("CUSTOMER_MASTER", "DM_CONSENT_FLAG", st.session_state.cm_filters)
        sel = st.multiselect("DM同意", [str(v) for v in vals], key="cm_dm")
        update_filter(st.session_state.cm_filters, "DM_CONSENT_FLAG", "multi", sel)
        
        vals = get_filtered_values("CUSTOMER_MASTER", "APP_USAGE_FLAG", st.session_state.cm_filters)
        sel = st.multiselect("アプリ利用", [str(v) for v in vals], key="cm_app")
        update_filter(st.session_state.cm_filters, "APP_USAGE_FLAG", "multi", sel)
    
    with st.expander(":material/shopping_cart: 購買履歴", expanded=True, icon=":material/shopping_cart:"):
        vals = get_filtered_values("ID_POS_TRANSACTION", "CHANNEL_TYPE", st.session_state.pos_filters)
        sel = st.multiselect("チャネル", vals, key="pos_channel")
        update_filter(st.session_state.pos_filters, "CHANNEL_TYPE", "multi", sel)
        
        vals = get_filtered_values("ID_POS_TRANSACTION", "PARTNER_NAME", st.session_state.pos_filters)
        sel = st.multiselect("パートナー", vals, key="pos_partner")
        update_filter(st.session_state.pos_filters, "PARTNER_NAME", "multi", sel)
        
        vals = get_filtered_values("ID_POS_TRANSACTION", "STORE_NAME", st.session_state.pos_filters)
        sel = st.multiselect("店舗名", vals, key="pos_store")
        update_filter(st.session_state.pos_filters, "STORE_NAME", "multi", sel)
        
        vals = get_filtered_values("ID_POS_TRANSACTION", "PRODUCT_CATEGORY", st.session_state.pos_filters)
        sel = st.multiselect("商品カテゴリ", vals, key="pos_cat")
        update_filter(st.session_state.pos_filters, "PRODUCT_CATEGORY", "multi", sel)
        
        vals = get_filtered_values("ID_POS_TRANSACTION", "PRODUCT_NAME", st.session_state.pos_filters)
        sel = st.multiselect("商品名", vals, key="pos_product")
        update_filter(st.session_state.pos_filters, "PRODUCT_NAME", "multi", sel)
        
        vals = get_filtered_values("ID_POS_TRANSACTION", "PAYMENT_METHOD", st.session_state.pos_filters)
        sel = st.multiselect("支払方法", vals, key="pos_pay")
        update_filter(st.session_state.pos_filters, "PAYMENT_METHOD", "multi", sel)
        
        min_amt, max_amt = get_filtered_range("ID_POS_TRANSACTION", "TOTAL_AMOUNT", st.session_state.pos_filters)
        if min_amt < max_amt:
            amt_range = st.slider("購入金額", min_amt, max_amt, (min_amt, max_amt), key="pos_amt")
            update_filter(st.session_state.pos_filters, "TOTAL_AMOUNT", "range", amt_range, (min_amt, max_amt))
        
        min_qty, max_qty = get_filtered_range("ID_POS_TRANSACTION", "QUANTITY", st.session_state.pos_filters)
        if min_qty < max_qty:
            qty_range = st.slider("数量", min_qty, max_qty, (min_qty, max_qty), key="pos_qty")
            update_filter(st.session_state.pos_filters, "QUANTITY", "range", qty_range, (min_qty, max_qty))
        
        min_d, max_d = get_filtered_date_range("ID_POS_TRANSACTION", "TRANSACTION_DATE", st.session_state.pos_filters)
        if min_d and max_d:
            date_range = st.date_input("取引日", (min_d, max_d), min_value=min_d, max_value=max_d, key="pos_date")
            update_filter(st.session_state.pos_filters, "TRANSACTION_DATE", "date_range", date_range, (min_d, max_d))
    
    with st.expander(":material/toll: ポイント履歴", expanded=True, icon=":material/toll:"):
        vals = get_filtered_values("POINT_HISTORY", "EVENT_TYPE", st.session_state.ph_filters)
        sel = st.multiselect("イベント種別", vals, key="ph_event")
        update_filter(st.session_state.ph_filters, "EVENT_TYPE", "multi", sel)
        
        vals = get_filtered_values("POINT_HISTORY", "SOURCE_TYPE", st.session_state.ph_filters)
        sel = st.multiselect("ソース種別", vals, key="ph_source")
        update_filter(st.session_state.ph_filters, "SOURCE_TYPE", "multi", sel)
        
        vals = get_filtered_values("POINT_HISTORY", "SOURCE_DETAIL", st.session_state.ph_filters)
        sel = st.multiselect("ソース詳細", vals, key="ph_source_detail")
        update_filter(st.session_state.ph_filters, "SOURCE_DETAIL", "multi", sel)
        
        min_pts, max_pts = get_filtered_range("POINT_HISTORY", "POINTS", st.session_state.ph_filters)
        if min_pts < max_pts:
            pts_range = st.slider("ポイント数", min_pts, max_pts, (min_pts, max_pts), key="ph_pts")
            update_filter(st.session_state.ph_filters, "POINTS", "range", pts_range, (min_pts, max_pts))
        
        min_bal, max_bal = get_filtered_range("POINT_HISTORY", "BALANCE_AFTER", st.session_state.ph_filters)
        if min_bal < max_bal:
            bal_range = st.slider("残高", min_bal, max_bal, (min_bal, max_bal), key="ph_bal")
            update_filter(st.session_state.ph_filters, "BALANCE_AFTER", "range", bal_range, (min_bal, max_bal))
        
        min_d, max_d = get_filtered_date_range("POINT_HISTORY", "EVENT_DATE", st.session_state.ph_filters)
        if min_d and max_d:
            date_range = st.date_input("イベント日", (min_d, max_d), min_value=min_d, max_value=max_d, key="ph_date")
            update_filter(st.session_state.ph_filters, "EVENT_DATE", "date_range", date_range, (min_d, max_d))
    
    st.space("medium")
    if st.button(":material/delete: フィルターをクリア", use_container_width=True):
        widget_keys = [
            "cm_rank", "cm_status", "cm_gender", "cm_age", "cm_pref", "cm_city", 
            "cm_occ", "cm_income", "cm_channel", "cm_pts", "cm_dm", "cm_app",
            "pos_channel", "pos_partner", "pos_store", "pos_cat", "pos_product",
            "pos_pay", "pos_amt", "pos_qty", "pos_date",
            "ph_event", "ph_source", "ph_source_detail", "ph_pts", "ph_bal", "ph_date"
        ]
        for k in widget_keys:
            if k in st.session_state:
                del st.session_state[k]
        st.session_state.cm_filters = {}
        st.session_state.pos_filters = {}
        st.session_state.ph_filters = {}
        st.rerun()

total_customers = get_total_customers()

cm_conditions = build_conditions(st.session_state.cm_filters)
pos_conditions = build_conditions(st.session_state.pos_filters)
ph_conditions = build_conditions(st.session_state.ph_filters)

cm_ids = get_customer_ids("CUSTOMER_MASTER", cm_conditions) if cm_conditions else None
pos_ids = get_customer_ids("ID_POS_TRANSACTION", pos_conditions) if pos_conditions else None
ph_ids = get_customer_ids("POINT_HISTORY", ph_conditions) if ph_conditions else None

sets_to_intersect = [s for s in [cm_ids, pos_ids, ph_ids] if s is not None]
if sets_to_intersect:
    final_ids = set.intersection(*sets_to_intersect)
else:
    final_ids = get_customer_ids("CUSTOMER_MASTER", [])

cm_count = len(cm_ids) if cm_ids else total_customers
after_cm_pos = len(cm_ids & pos_ids) if cm_ids and pos_ids else (len(pos_ids) if pos_ids else (len(cm_ids) if cm_ids else total_customers))

st.title("顧客抽出ダッシュボード")
st.caption("3テーブルの条件から顧客IDを抽出し、抽出過程を可視化")

st.subheader(":material/account_tree: データリネージ", divider="blue")

cm_filter_text = ", ".join(st.session_state.cm_filters.keys()) if st.session_state.cm_filters else "なし"
pos_filter_text = ", ".join(st.session_state.pos_filters.keys()) if st.session_state.pos_filters else "なし"
ph_filter_text = ", ".join(st.session_state.ph_filters.keys()) if st.session_state.ph_filters else "なし"

excluded_cm = total_customers - cm_count
excluded_pos = cm_count - after_cm_pos
excluded_ph = after_cm_pos - len(final_ids)

col_funnel, col_waterfall = st.columns(2)

with col_funnel:
    with st.container(border=True):
        st.markdown("**ファンネル（絞り込み過程）**")
        fig_funnel = go.Figure(go.Funnel(
            y=["全顧客", "顧客マスタ条件後", "購買履歴条件後", "最終抽出"],
            x=[total_customers, cm_count, after_cm_pos, len(final_ids)],
            textposition="inside",
            textinfo="value+percent initial",
            marker=dict(color=["#0068C9", "#1E88E5", "#42A5F5", "#4CAF50"]),
            connector=dict(line=dict(color="#E0E0E0", width=2))
        ))
        fig_funnel.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            font=dict(size=14)
        )
        st.plotly_chart(fig_funnel, use_container_width=True)

with col_waterfall:
    with st.container(border=True):
        st.markdown("**ウォーターフォール（増減内訳）**")
        fig_waterfall = go.Figure(go.Waterfall(
            orientation="v",
            measure=["absolute", "relative", "relative", "relative", "total"],
            x=["全顧客", "顧客マスタ", "購買履歴", "ポイント履歴", "最終"],
            y=[total_customers, -excluded_cm, -excluded_pos, -excluded_ph, 0],
            text=[f"{total_customers:,}", f"-{excluded_cm:,}", f"-{excluded_pos:,}", f"-{excluded_ph:,}", f"{len(final_ids):,}"],
            textposition="outside",
            connector=dict(line=dict(color="#E0E0E0", width=2)),
            decreasing=dict(marker=dict(color="#EF5350")),
            increasing=dict(marker=dict(color="#4CAF50")),
            totals=dict(marker=dict(color="#4CAF50"))
        ))
        fig_waterfall.update_layout(
            height=350,
            margin=dict(l=20, r=20, t=20, b=20),
            showlegend=False,
            font=dict(size=12)
        )
        st.plotly_chart(fig_waterfall, use_container_width=True)

with st.container(border=True):
    st.markdown("**フィルター適用状況**")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown(f"""
        <div style="text-align:center; padding:10px; background:linear-gradient(135deg, #0068C9, #1E88E5); border-radius:10px; color:white;">
            <div style="font-size:12px;">STEP 0</div>
            <div style="font-size:24px; font-weight:bold;">{total_customers:,}</div>
            <div style="font-size:11px;">全顧客</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        pct1 = cm_count / total_customers * 100 if total_customers > 0 else 100
        st.markdown(f"""
        <div style="text-align:center; padding:10px; background:linear-gradient(135deg, #1E88E5, #42A5F5); border-radius:10px; color:white;">
            <div style="font-size:12px;">STEP 1: 顧客マスタ</div>
            <div style="font-size:24px; font-weight:bold;">{cm_count:,}</div>
            <div style="font-size:11px;">{pct1:.1f}% 残存 / {len(st.session_state.cm_filters)}条件</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        pct2 = after_cm_pos / total_customers * 100 if total_customers > 0 else 100
        st.markdown(f"""
        <div style="text-align:center; padding:10px; background:linear-gradient(135deg, #42A5F5, #64B5F6); border-radius:10px; color:white;">
            <div style="font-size:12px;">STEP 2: 購買履歴</div>
            <div style="font-size:24px; font-weight:bold;">{after_cm_pos:,}</div>
            <div style="font-size:11px;">{pct2:.1f}% 残存 / {len(st.session_state.pos_filters)}条件</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        pct3 = len(final_ids) / total_customers * 100 if total_customers > 0 else 100
        st.markdown(f"""
        <div style="text-align:center; padding:10px; background:linear-gradient(135deg, #4CAF50, #66BB6A); border-radius:10px; color:white;">
            <div style="font-size:12px;">STEP 3: ポイント履歴</div>
            <div style="font-size:24px; font-weight:bold;">{len(final_ids):,}</div>
            <div style="font-size:11px;">{pct3:.1f}% 残存 / {len(st.session_state.ph_filters)}条件</div>
        </div>
        """, unsafe_allow_html=True)

st.subheader(":material/group: 抽出顧客一覧", divider="blue")

if final_ids:
    final_list = sorted(list(final_ids))[:500]
    ids_str = "', '".join(final_list)
    df_result = conn.query(f"""
        SELECT CUSTOMER_ID, CUSTOMER_NAME, GENDER, AGE, PREFECTURE, 
               MEMBERSHIP_RANK, TOTAL_POINTS, LAST_PURCHASE_DATE, MEMBERSHIP_STATUS
        FROM DEMO.LM.CUSTOMER_MASTER
        WHERE CUSTOMER_ID IN ('{ids_str}')
        ORDER BY TOTAL_POINTS DESC
    """)
    
    c1, c2, c3 = st.columns([1, 1, 4])
    c1.metric("抽出件数", f"{len(final_ids):,}")
    c2.metric("表示件数", f"{len(df_result):,}")
    if len(final_ids) > 500:
        c3.caption(":material/info: 上位500件を表示しています")
    
    st.dataframe(
        df_result,
        use_container_width=True,
        hide_index=True,
        height=350,
        column_config={
            "CUSTOMER_ID": st.column_config.TextColumn("顧客ID", width="small"),
            "CUSTOMER_NAME": st.column_config.TextColumn("氏名", width="medium"),
            "GENDER": st.column_config.TextColumn("性別", width="small"),
            "AGE": st.column_config.NumberColumn("年齢", width="small"),
            "PREFECTURE": st.column_config.TextColumn("都道府県", width="small"),
            "MEMBERSHIP_RANK": st.column_config.TextColumn("ランク", width="small"),
            "TOTAL_POINTS": st.column_config.NumberColumn("累計ポイント", format="%d"),
            "LAST_PURCHASE_DATE": st.column_config.DateColumn("最終購入日"),
            "MEMBERSHIP_STATUS": st.column_config.TextColumn("ステータス", width="small"),
        }
    )
    
    st.session_state.final_ids = final_list
    
    st.subheader(":material/person_search: 顧客詳細", divider="blue")
    
    selected_id = st.selectbox("顧客を選択", final_list, label_visibility="collapsed")
    
    if selected_id:
        tab1, tab2, tab3 = st.tabs([
            ":material/badge: 基本情報",
            ":material/receipt_long: 購買履歴",
            ":material/toll: ポイント履歴"
        ])
        
        with tab1:
            df_cm = conn.query(f"SELECT * FROM DEMO.LM.CUSTOMER_MASTER WHERE CUSTOMER_ID = '{selected_id}'")
            if not df_cm.empty:
                row = df_cm.iloc[0]
                c1, c2, c3 = st.columns(3)
                
                with c1:
                    with st.container(border=True):
                        st.caption(":material/person: 基本情報")
                        st.markdown(f"**{row['CUSTOMER_NAME']}**")
                        st.text(f"ID: {row['CUSTOMER_ID']}")
                        st.text(f"性別: {row['GENDER']} / 年齢: {row['AGE']}")
                        st.text(f"生年月日: {row['BIRTH_DATE']}")
                
                with c2:
                    with st.container(border=True):
                        st.caption(":material/card_membership: 会員情報")
                        st.markdown(f"**{row['MEMBERSHIP_RANK']}**")
                        st.badge(row['MEMBERSHIP_STATUS'], color="green" if row['MEMBERSHIP_STATUS'] == "アクティブ" else "gray")
                        st.text(f"累計ポイント: {row['TOTAL_POINTS']:,}")
                        st.text(f"入会日: {row['ENROLLMENT_DATE']}")
                
                with c3:
                    with st.container(border=True):
                        st.caption(":material/location_on: その他")
                        st.text(f"都道府県: {row['PREFECTURE']}")
                        st.text(f"市区町村: {row['CITY']}")
                        st.text(f"職業: {row['OCCUPATION']}")
                        st.text(f"最終購入日: {row['LAST_PURCHASE_DATE']}")
        
        with tab2:
            df_pos = conn.query(f"""
                SELECT TRANSACTION_DATE, CHANNEL_TYPE, STORE_NAME, PRODUCT_CATEGORY, 
                       PRODUCT_NAME, QUANTITY, TOTAL_AMOUNT, POINTS_EARNED, PAYMENT_METHOD
                FROM DEMO.LM.ID_POS_TRANSACTION
                WHERE CUSTOMER_ID = '{selected_id}'
                ORDER BY TRANSACTION_DATE DESC
            """)
            
            if not df_pos.empty:
                m1, m2, m3, m4 = st.columns(4)
                m1.metric("購入回数", f"{len(df_pos):,}")
                m2.metric("総購入金額", f"¥{df_pos['TOTAL_AMOUNT'].sum():,.0f}")
                m3.metric("獲得ポイント", f"{df_pos['POINTS_EARNED'].sum():,.0f}")
                m4.metric("平均単価", f"¥{df_pos['TOTAL_AMOUNT'].mean():,.0f}")
                
                st.dataframe(
                    df_pos,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "TRANSACTION_DATE": st.column_config.DateColumn("日付"),
                        "TOTAL_AMOUNT": st.column_config.NumberColumn("金額", format="¥%d"),
                        "POINTS_EARNED": st.column_config.NumberColumn("獲得PT"),
                    }
                )
            else:
                st.info("購買履歴がありません", icon=":material/info:")
        
        with tab3:
            df_ph = conn.query(f"""
                SELECT EVENT_DATE, EVENT_TYPE, POINTS, BALANCE_AFTER, SOURCE_TYPE, SOURCE_DETAIL
                FROM DEMO.LM.POINT_HISTORY
                WHERE CUSTOMER_ID = '{selected_id}'
                ORDER BY EVENT_DATE DESC
            """)
            
            if not df_ph.empty:
                st.line_chart(df_ph.set_index("EVENT_DATE")["BALANCE_AFTER"], height=200, color="#0068C9")
                st.dataframe(
                    df_ph,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "EVENT_DATE": st.column_config.DateColumn("日付"),
                        "POINTS": st.column_config.NumberColumn("ポイント", format="%+d"),
                        "BALANCE_AFTER": st.column_config.NumberColumn("残高"),
                    }
                )
            else:
                st.info("ポイント履歴がありません", icon=":material/info:")
else:
    st.warning("条件に該当する顧客がいません", icon=":material/warning:")
