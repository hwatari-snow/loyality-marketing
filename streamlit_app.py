import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import json
import _snowflake

st.set_page_config(
    page_title="顧客抽出ダッシュボード",
    page_icon=":material/favorite:",
    layout="wide"
)

conn = st.connection("snowflake")

SEMANTIC_VIEW = "DEMO.LM.LOYALTY_PROGRAM_SV"

COLORS = {
    "primary": "#E87A00",
    "primary_light": "#F59E0B",
    "primary_lighter": "#FBBF24",
    "primary_lightest": "#FDE68A",
    "accent": "#C2610A",
    "accent_light": "#E87A00",
    "success": "#059669",
    "success_light": "#10B981",
    "text_dark": "#1F2937",
    "text_light": "#FFFFFF",
    "bg_warm": "#FFF5EB",
    "border": "#FED7AA"
}

def call_analyst(question):
    messages = [
        {"role": "user", "content": [{"type": "text", "text": question}]}
    ]
    
    request_body = {
        "messages": messages,
        "semantic_view": SEMANTIC_VIEW
    }
    
    response = _snowflake.send_snow_api_request(
        "POST",
        "/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000
    )
    
    if response["status"] < 400:
        response_content = json.loads(response["content"])
        return response_content
    else:
        raise Exception(f"API Error: {response['status']} - {response.get('content', 'Unknown error')}")

def extract_analyst_response(response):
    result = {"text": "", "sql": "", "suggestions": []}
    
    if "message" in response:
        for content in response["message"].get("content", []):
            if content["type"] == "text":
                result["text"] += content.get("text", "")
            elif content["type"] == "sql":
                result["sql"] = content.get("statement", "")
            elif content["type"] == "suggestions":
                result["suggestions"] = content.get("suggestions", [])
    
    return result

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

if "analyst_messages" not in st.session_state:
    st.session_state.analyst_messages = []

if "analyst_extracted_ids" not in st.session_state:
    st.session_state.analyst_extracted_ids = []

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

st.markdown("""
<style>
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding: 10px 20px;
    }
    div[data-testid="stMetric"] {
        background: linear-gradient(135deg, #FFF5EB, #FFFFFF);
        border: 1px solid #FED7AA;
        border-radius: 10px;
        padding: 15px;
    }
</style>
""", unsafe_allow_html=True)

st.markdown(f"""
<div style="margin-bottom: 1rem; padding-bottom: 0.8rem; border-bottom: 3px solid {COLORS['primary']};">
    <h1 style="margin: 0; color: {COLORS['text_dark']}; font-size: 2rem;">顧客抽出ダッシュボード</h1>
    <p style="margin: 5px 0 0 0; color: #6B7280; font-size: 0.9rem;">Loyalty Marketing Customer Extraction</p>
</div>
""", unsafe_allow_html=True)

with st.sidebar:
    st.markdown(f"""
    <div style="margin-bottom: 15px;">
        <h3 style="color: {COLORS['text_dark']}; margin: 0; font-size: 1.1rem;">フィルター</h3>
    </div>
    """, unsafe_allow_html=True)
    st.caption(":material/info: 選択に応じて選択肢が絞り込まれます")
    
    with st.expander("👤 顧客マスタ", expanded=True):
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
    
    with st.expander("🛒 購買履歴", expanded=True):
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
    
    with st.expander("💎 ポイント履歴", expanded=True):
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
    
    st.divider()
    if st.button("🗑️ フィルターをクリア", use_container_width=True, type="primary"):
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

main_tab, analyst_tab, csv_tab = st.tabs([
    "📊 フィルター抽出",
    "🤖 AI抽出 (Cortex Analyst)",
    "📁 CSV抽出"
])

with main_tab:
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

    st.caption("3テーブルの条件から顧客IDを抽出し、抽出過程を可視化")

    st.markdown(f"""
    <div style="display: flex; align-items: center; margin: 1.5rem 0 1rem 0; padding-bottom: 0.5rem; border-bottom: 3px solid {COLORS['primary']};">
        <span style="font-size: 1.3rem; margin-right: 10px;">📈</span>
        <h3 style="margin: 0; color: {COLORS['text_dark']};">データリネージ</h3>
    </div>
    """, unsafe_allow_html=True)

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
                marker=dict(color=[COLORS["primary"], COLORS["primary_light"], COLORS["primary_lighter"], COLORS["success"]]),
                connector=dict(line=dict(color=COLORS["border"], width=2))
            ))
            fig_funnel.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=20, b=20),
                font=dict(size=14),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)"
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
                connector=dict(line=dict(color=COLORS["border"], width=2)),
                decreasing=dict(marker=dict(color=COLORS["primary_light"])),
                increasing=dict(marker=dict(color=COLORS["success"])),
                totals=dict(marker=dict(color=COLORS["success"]))
            ))
            fig_waterfall.update_layout(
                height=350,
                margin=dict(l=20, r=20, t=20, b=20),
                showlegend=False,
                font=dict(size=12),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)"
            )
            st.plotly_chart(fig_waterfall, use_container_width=True)

    with st.container(border=True):
        st.markdown("**フィルター適用状況**")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(f"""
            <div style="text-align:center; padding:15px; background:linear-gradient(135deg, {COLORS['primary']}, {COLORS['primary_light']}); 
                        border-radius:12px; color:white; box-shadow: 0 4px 6px rgba(232, 122, 0, 0.25);">
                <div style="font-size:11px; opacity:0.9;">STEP 0</div>
                <div style="font-size:28px; font-weight:bold;">{total_customers:,}</div>
                <div style="font-size:12px; opacity:0.9;">全顧客</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            pct1 = cm_count / total_customers * 100 if total_customers > 0 else 100
            st.markdown(f"""
            <div style="text-align:center; padding:15px; background:linear-gradient(135deg, {COLORS['primary_light']}, {COLORS['primary_lighter']}); 
                        border-radius:12px; color:white; box-shadow: 0 4px 6px rgba(245, 158, 11, 0.25);">
                <div style="font-size:11px; opacity:0.9;">STEP 1: 顧客マスタ</div>
                <div style="font-size:28px; font-weight:bold;">{cm_count:,}</div>
                <div style="font-size:12px; opacity:0.9;">{pct1:.1f}% / {len(st.session_state.cm_filters)}条件</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            pct2 = after_cm_pos / total_customers * 100 if total_customers > 0 else 100
            st.markdown(f"""
            <div style="text-align:center; padding:15px; background:linear-gradient(135deg, {COLORS['primary_lighter']}, {COLORS['primary_lightest']}); 
                        border-radius:12px; color:white; box-shadow: 0 4px 6px rgba(251, 191, 36, 0.25);">
                <div style="font-size:11px; opacity:0.9;">STEP 2: 購買履歴</div>
                <div style="font-size:28px; font-weight:bold;">{after_cm_pos:,}</div>
                <div style="font-size:12px; opacity:0.9;">{pct2:.1f}% / {len(st.session_state.pos_filters)}条件</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            pct3 = len(final_ids) / total_customers * 100 if total_customers > 0 else 100
            st.markdown(f"""
            <div style="text-align:center; padding:15px; background:linear-gradient(135deg, {COLORS['success']}, {COLORS['success_light']}); 
                        border-radius:12px; color:white; box-shadow: 0 4px 6px rgba(5, 150, 105, 0.25);">
                <div style="font-size:11px; opacity:0.9;">STEP 3: 最終抽出</div>
                <div style="font-size:28px; font-weight:bold;">{len(final_ids):,}</div>
                <div style="font-size:12px; opacity:0.9;">{pct3:.1f}% / {len(st.session_state.ph_filters)}条件</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown(f"""
    <div style="display: flex; align-items: center; margin: 2rem 0 1rem 0; padding-bottom: 0.5rem; border-bottom: 3px solid {COLORS['primary']};">
        <span style="font-size: 1.3rem; margin-right: 10px;">👥</span>
        <h3 style="margin: 0; color: {COLORS['text_dark']};">抽出顧客一覧</h3>
    </div>
    """, unsafe_allow_html=True)

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
            c3.caption("ℹ️ 上位500件を表示しています")
        
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
    else:
        st.warning("条件に該当する顧客がいません", icon="⚠️")

with analyst_tab:
    st.markdown(f"""
    <div style="display: flex; align-items: center; margin-bottom: 1rem;">
        <div style="background: linear-gradient(135deg, {COLORS['accent']}, {COLORS['accent_light']}); 
                    padding: 10px; border-radius: 10px; margin-right: 12px;">
            <span style="font-size: 22px;">🤖</span>
        </div>
        <div>
            <h3 style="margin: 0; color: {COLORS['text_dark']};">自然言語で顧客を抽出</h3>
            <p style="margin: 0; color: #6B7280; font-size: 0.85rem;">セマンティックビュー: {SEMANTIC_VIEW}</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    with st.container(border=True):
        st.markdown("**💡 質問例:**")
        col1, col2 = st.columns(2)
        with col1:
            st.caption("• 東京都在住のゴールド会員の顧客を抽出して")
            st.caption("• 過去3ヶ月で10万円以上購入した顧客は？")
            st.caption("• アプリを利用していてDM同意済みの顧客リスト")
        with col2:
            st.caption("• ポイント残高が5000以上の顧客を教えて")
            st.caption("• 30代女性で化粧品カテゴリを購入した顧客")
            st.caption("• 最終購入日が1年以上前の休眠顧客を抽出")
    
    for msg in st.session_state.analyst_messages:
        with st.chat_message(msg["role"], avatar="👤" if msg["role"] == "user" else "🤖"):
            st.markdown(msg["content"])
            if "sql" in msg and msg["sql"]:
                with st.expander("🔍 生成されたSQL"):
                    st.code(msg["sql"], language="sql")
            if "data" in msg and msg["data"] is not None:
                st.dataframe(msg["data"], use_container_width=True, hide_index=True)
                if "CUSTOMER_ID" in msg["data"].columns:
                    customer_ids = msg["data"]["CUSTOMER_ID"].tolist()
                    st.session_state.analyst_extracted_ids = customer_ids
                    st.success(f"✅ {len(customer_ids)}件の顧客IDを抽出しました")
    
    if prompt := st.chat_input("顧客抽出の条件を自然言語で入力してください..."):
        st.session_state.analyst_messages.append({"role": "user", "content": prompt})
        
        with st.chat_message("user", avatar="👤"):
            st.markdown(prompt)
        
        with st.chat_message("assistant", avatar="🤖"):
            with st.spinner("Cortex Analystが分析中..."):
                try:
                    response = call_analyst(prompt)
                    result = extract_analyst_response(response)
                    
                    assistant_msg = {"role": "assistant", "content": result["text"], "sql": result["sql"], "data": None}
                    
                    if result["text"]:
                        st.markdown(result["text"])
                    
                    if result["sql"]:
                        with st.expander("🔍 生成されたSQL"):
                            st.code(result["sql"], language="sql")
                        
                        try:
                            df = conn.query(result["sql"])
                            assistant_msg["data"] = df
                            st.dataframe(df, use_container_width=True, hide_index=True)
                            
                            if "CUSTOMER_ID" in df.columns:
                                customer_ids = df["CUSTOMER_ID"].tolist()
                                st.session_state.analyst_extracted_ids = customer_ids
                                st.success(f"✅ {len(customer_ids)}件の顧客IDを抽出しました")
                        except Exception as e:
                            st.error(f"SQLの実行に失敗しました: {e}")
                    
                    if result["suggestions"]:
                        st.markdown("**こちらの質問はいかがですか？**")
                        for suggestion in result["suggestions"]:
                            st.caption(f"• {suggestion}")
                    
                    st.session_state.analyst_messages.append(assistant_msg)
                    
                except Exception as e:
                    st.error(f"エラーが発生しました: {e}")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("🗑️ 会話をクリア", use_container_width=True):
            st.session_state.analyst_messages = []
            st.session_state.analyst_extracted_ids = []
            st.rerun()
    
    with col2:
        if st.session_state.analyst_extracted_ids:
            st.download_button(
                "📥 抽出顧客IDをダウンロード",
                data="\n".join(st.session_state.analyst_extracted_ids),
                file_name="extracted_customer_ids.txt",
                mime="text/plain",
                use_container_width=True
            )

with csv_tab:
    st.markdown(f"""
    <div style="display: flex; align-items: center; margin-bottom: 1rem;">
        <div>
            <h3 style="margin: 0; color: {COLORS['text_dark']};">CSVから顧客データを抽出</h3>
            <p style="margin: 0; color: #6B7280; font-size: 0.85rem;">顧客IDリストをアップロードして顧客マスタを取得</p>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    with st.container(border=True):
        st.markdown("**📋 使い方:**")
        st.caption("1. CUSTOMER_ID列を含むCSVファイルをアップロード")
        st.caption("2. アップロードされたIDと顧客マスタを照合")
        st.caption("3. マッチした顧客データをCSVでダウンロード")
    
    uploaded_file = st.file_uploader("CSVファイルをアップロード", type=["csv"], key="csv_upload")
    
    if uploaded_file is not None:
        try:
            df_uploaded = pd.read_csv(uploaded_file)
            
            id_column = None
            possible_columns = ["CUSTOMER_ID", "customer_id", "CustomerId", "顧客ID", "ID", "id"]
            for col in possible_columns:
                if col in df_uploaded.columns:
                    id_column = col
                    break
            
            if id_column is None:
                st.warning("CUSTOMER_ID列が見つかりません。列を選択してください。", icon="⚠️")
                id_column = st.selectbox("顧客IDの列を選択", df_uploaded.columns.tolist())
            
            if id_column:
                uploaded_ids = df_uploaded[id_column].dropna().astype(str).tolist()
                uploaded_ids = [id.strip() for id in uploaded_ids if id.strip()]
                
                st.info(f"📄 アップロードされたID数: **{len(uploaded_ids)}**件")
                
                with st.expander("アップロードされたIDをプレビュー"):
                    st.dataframe(df_uploaded[[id_column]].head(100), use_container_width=True, hide_index=True)
                
                if st.button("🔍 顧客マスタと照合", type="primary", use_container_width=True):
                    with st.spinner("照合中..."):
                        batch_size = 500
                        all_results = []
                        
                        for i in range(0, len(uploaded_ids), batch_size):
                            batch_ids = uploaded_ids[i:i+batch_size]
                            ids_str = "', '".join([id.replace("'", "''") for id in batch_ids])
                            
                            df_batch = conn.query(f"""
                                SELECT CUSTOMER_ID, CUSTOMER_NAME, GENDER, AGE, PREFECTURE, CITY,
                                       MEMBERSHIP_RANK, MEMBERSHIP_STATUS, TOTAL_POINTS, 
                                       LAST_PURCHASE_DATE, ENROLLMENT_DATE,
                                       OCCUPATION, INCOME_RANGE, ENROLLMENT_CHANNEL,
                                       DM_CONSENT_FLAG, APP_USAGE_FLAG
                                FROM DEMO.LM.CUSTOMER_MASTER
                                WHERE CUSTOMER_ID IN ('{ids_str}')
                            """)
                            all_results.append(df_batch)
                        
                        if all_results:
                            df_result = pd.concat(all_results, ignore_index=True)
                            st.session_state.csv_result = df_result
                            st.session_state.csv_uploaded_count = len(uploaded_ids)
                
                if "csv_result" in st.session_state and st.session_state.csv_result is not None:
                    df_result = st.session_state.csv_result
                    uploaded_count = st.session_state.csv_uploaded_count
                    matched_count = len(df_result)
                    
                    col1, col2, col3 = st.columns(3)
                    col1.metric("アップロード件数", f"{uploaded_count:,}")
                    col2.metric("マッチ件数", f"{matched_count:,}")
                    col3.metric("マッチ率", f"{matched_count/uploaded_count*100:.1f}%" if uploaded_count > 0 else "0%")
                    
                    st.markdown(f"""
                    <div style="display: flex; align-items: center; margin: 1.5rem 0 1rem 0; padding-bottom: 0.5rem; border-bottom: 3px solid {COLORS['primary']};">
                        <h4 style="margin: 0; color: {COLORS['text_dark']};">抽出結果</h4>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    st.dataframe(
                        df_result,
                        use_container_width=True,
                        hide_index=True,
                        height=400,
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
                    
                    csv_data = df_result.to_csv(index=False).encode('utf-8-sig')
                    st.download_button(
                        "📥 顧客データをCSVでダウンロード",
                        data=csv_data,
                        file_name="extracted_customers.csv",
                        mime="text/csv",
                        use_container_width=True,
                        type="primary"
                    )
                    
        except Exception as e:
            st.error(f"ファイルの読み込みに失敗しました: {e}")
