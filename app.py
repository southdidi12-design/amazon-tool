import streamlit as st
from datetime import datetime, timedelta

from amazon_tool.automation import HAS_OPENAI
from amazon_tool.config import (
    AUTO_SYNC_REFRESH_DAYS,
    AUTO_SYNC_INTERVAL_SECONDS,
    AUTO_SYNC_TS_KEY,
    SYNC_DAYS_KEY,
    SYNC_ERROR_KEY,
    SYNC_STATUS_KEY,
    VERSION,
    get_real_today,
)
from amazon_tool.db import (
    get_asin_dashboard_data,
    get_dashboard_data,
    get_latest_report_date,
    get_product_ads_data,
    get_system_value,
    get_trend_data,
    init_db,
    set_system_value,
)
from amazon_tool.sync import run_sync_task_guarded, start_auto_sync
from amazon_tool.ui.asin import render_asin_tab
from amazon_tool.ui.autopilot import render_autopilot_tab
from amazon_tool.ui.dashboard import render_dashboard_tab
from amazon_tool.ui.manage import render_manage_tab
from amazon_tool.ui.product_ads import render_product_ads_tab
from amazon_tool.ui.negatives import render_negative_keywords_tab

st.set_page_config(layout="wide", page_title=f"HNV ERP - {VERSION}", initial_sidebar_state="expanded")

# --- 1. 初始化 ---
init_db()
start_auto_sync()

with st.sidebar:
    st.title("🎛️ 终极控制台")
    st.caption(f"版本: {VERSION}")

    if HAS_OPENAI:
        st.success("✅ AI 引擎在线")
    else:
        st.warning("⚠️ AI 引擎离线")
    deepseek_key = st.text_input("DeepSeek Key", type="password")

    st.caption("自动同步在后台运行，首次启动会自动回补缺失天数。")

    last_sync_ts = get_system_value(AUTO_SYNC_TS_KEY)
    if last_sync_ts:
        st.caption(f"自动同步: {last_sync_ts}")
    else:
        st.caption("自动同步: 未执行")
    hours = max(1, int(AUTO_SYNC_INTERVAL_SECONDS / 3600))
    st.caption(f"自动同步频率: 每 {hours} 小时回补最近 {AUTO_SYNC_REFRESH_DAYS} 天")
    st.caption("提示: 使用本地电脑需保持开机和程序运行")
    latest_report = get_latest_report_date()
    if latest_report:
        st.caption(f"最新数据: {latest_report}")
    sync_status = get_system_value(SYNC_STATUS_KEY)
    sync_days = get_system_value(SYNC_DAYS_KEY)
    if sync_status:
        label = f"同步状态: {sync_status}"
        if sync_days:
            label += f" (天数: {sync_days})"
        st.caption(label)
    sync_error = get_system_value(SYNC_ERROR_KEY)
    if sync_error:
        st.caption(f"同步提示: {sync_error}")

    st.divider()
    if st.button("🚀 强制刷新数据", type="primary"):
        with st.status("正在同步...", expanded=True) as s:
            ok = run_sync_task_guarded(7, s)
            if not ok:
                s.update(label="同步未启动 (可能正在同步或无配置)", state="error")
            else:
                set_system_value(AUTO_SYNC_TS_KEY, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            st.rerun()

# 登录 & 主页
show_advanced = st.sidebar.checkbox("显示高级功能", value=False)

if "logged_in" not in st.session_state:
    st.session_state.logged_in = True
today = get_real_today()
st.title("🚀 亚马逊全托管中心")

# 日期选择
c1, c2 = st.columns([1, 3])
with c1:
    dr = st.date_input("📅 分析周期", value=(today - timedelta(days=7), today))
    if isinstance(dr, tuple) and len(dr) == 2:
        s_d, e_d = dr
    else:
        s_d = e_d = today

# 获取数据（核心）
df = get_dashboard_data(s_d.strftime("%Y-%m-%d"), e_d.strftime("%Y-%m-%d"))
trend = get_trend_data(s_d.strftime("%Y-%m-%d"), e_d.strftime("%Y-%m-%d"))

asin_df = None
product_ads_df = None
if show_advanced:
    asin_df = get_asin_dashboard_data(s_d.strftime("%Y-%m-%d"), e_d.strftime("%Y-%m-%d"))
    product_ads_df = get_product_ads_data()

# 如果没数据，显示引导
if df.empty and (asin_df is None or asin_df.empty) and (product_ads_df is None or product_ads_df.empty):
    st.info("数据库暂无所选日期的数据。请尝试调整日期，或点击左侧 **强制刷新数据**。")
else:
    if not df.empty:
        # 核心指标
        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("总花费", f"${df['cost'].sum():.2f}")
        k2.metric("总销售", f"${df['sales'].sum():.2f}")
        total_acos = df["cost"].sum() / df["sales"].sum() if df["sales"].sum() > 0 else 0
        k3.metric("ACOS", f"{total_acos*100:.1f}%", delta_color="inverse")
        k4.metric("点击量", int(df["clicks"].sum()))
        k5.metric(
            "转化率",
            f"{(df['orders'].sum()/df['clicks'].sum() if df['clicks'].sum()>0 else 0)*100:.1f}%",
        )
    else:
        st.info("本期暂无 Campaign 维度数据，先展示 ASIN 维度。")

    st.divider()

    # 五大标签页
    base_tabs = ["📊 数据看板", "🤖 自动驾驶", "📌 操作中心"]
    adv_tabs = ["🧩 ASIN 维度", "🧾 商品广告", "🚫 否词管理"] if show_advanced else []
    tabs = st.tabs(base_tabs + adv_tabs)

    with tabs[0]:
        render_dashboard_tab(df, trend)

    with tabs[1]:
        render_autopilot_tab(deepseek_key)

    with tabs[2]:
        render_manage_tab(df)

    if show_advanced:
        with tabs[3]:
            render_asin_tab(asin_df)

        with tabs[4]:
            render_product_ads_tab(product_ads_df)

        with tabs[5]:
            render_negative_keywords_tab()
