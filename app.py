import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from amazon_tool.automation import HAS_OPENAI
from amazon_tool.config import (
    AUTO_SYNC_REFRESH_DAYS,
    AUTO_SYNC_INTERVAL_SECONDS,
    AUTO_SYNC_TS_KEY,
    AUTO_AI_ENABLED_KEY,
    AUTO_AI_LIVE_KEY,
    AUTO_AI_LAST_RUN_KEY,
    AUTO_NEGATIVE_ENABLED_KEY,
    SYNC_DAYS_KEY,
    SYNC_ERROR_KEY,
    SYNC_STATUS_KEY,
    VERSION,
    get_auto_ai_campaign_whitelist,
    get_real_today,
)
from amazon_tool.db import (
    get_asin_dashboard_data,
    get_dashboard_data,
    get_latest_report_date,
    get_product_ads_data,
    get_db_connection,
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

    def _get_bool_setting(key, default=False):
        val = get_system_value(key)
        if val is None:
            return default
        try:
            return str(val).strip() in ["1", "true", "True", "yes", "YES", "on", "ON"]
        except Exception:
            return default

    st.divider()
    if st.button("🚀 强制刷新数据", type="primary"):
        with st.status("正在同步...", expanded=True) as s:
            ok = run_sync_task_guarded(7, s)
            if not ok:
                s.update(label="同步未启动 (可能正在同步或无配置)", state="error")
            else:
                set_system_value(AUTO_SYNC_TS_KEY, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            st.rerun()

    st.divider()
    st.markdown("#### 🤖 AI 托管状态")
    whitelist = [w for w in get_auto_ai_campaign_whitelist() if str(w).strip()]
    campaign_df = None
    id_to_name = {}
    name_to_id = {}
    if whitelist:
        conn = get_db_connection()
        try:
            campaign_df = pd.read_sql_query(
                "SELECT campaign_id, campaign_name FROM campaign_settings",
                conn,
            )
        except Exception:
            campaign_df = None
        finally:
            conn.close()
        if campaign_df is not None and not campaign_df.empty:
            campaign_df["campaign_id"] = campaign_df["campaign_id"].fillna("").astype(str)
            campaign_df["campaign_name"] = campaign_df["campaign_name"].fillna("").astype(str)
            id_to_name = dict(zip(campaign_df["campaign_id"], campaign_df["campaign_name"]))
            name_to_id = dict(zip(campaign_df["campaign_name"], campaign_df["campaign_id"]))
    display_whitelist = []
    resolved_names = []
    for w in whitelist:
        if w in id_to_name:
            name = id_to_name.get(w) or ""
            display_whitelist.append(f"{name} ({w})" if name else w)
            resolved_names.append(name or w)
        elif w in name_to_id:
            cid = name_to_id.get(w) or ""
            display_whitelist.append(f"{w} ({cid})" if cid else w)
            resolved_names.append(w)
        else:
            display_whitelist.append(w)
            resolved_names.append(w)
    target_name = resolved_names[0] if resolved_names else ""
    if whitelist:
        st.caption(f"托管活动: {', '.join(display_whitelist)}")
    else:
        st.caption("托管活动: 未配置")

    ai_enabled = _get_bool_setting(AUTO_AI_ENABLED_KEY, False)
    ai_live = _get_bool_setting(AUTO_AI_LIVE_KEY, False)
    auto_neg = _get_bool_setting(AUTO_NEGATIVE_ENABLED_KEY, False)

    campaign_found = None
    if whitelist:
        if campaign_df is None:
            campaign_found = None
        elif campaign_df.empty:
            campaign_found = 0
        else:
            known_ids = set(campaign_df["campaign_id"])
            known_names = set(campaign_df["campaign_name"])
            campaign_found = sum(1 for w in whitelist if w in known_ids or w in known_names)

    if not ai_enabled:
        st.warning("AI 托管未开启")
    elif not target_name:
        st.warning("未配置托管活动")
    elif campaign_found is None:
        st.info("活动未同步，状态未知")
    elif campaign_found == 0:
        st.warning("未找到托管活动（请先同步广告配置）")
    else:
        st.success("托管中（实盘）" if ai_live else "托管中（模拟）")

    st.caption(f"自动否词: {'开启' if auto_neg else '关闭'}")

    def _render_ai_explain_panel():
        st.markdown("##### 功能说明")
        st.write(
            "AI托管会对**白名单活动**执行自动化操作：预算锁定、竞价调整、广告位调整、自动否词。"
            "对照组活动不会被修改。"
        )
        st.markdown("**当前托管规则摘要**")
        st.markdown(
            "- 仅托管白名单活动（当前为 B 组）\n"
            "- 预算固定为 $10/天（仅对 B 生效）\n"
            "- 竞价与广告位按 ACOS/止损规则自动调整\n"
            "- 自动否词：基于花费/点击/ACOS 触发\n"
            "- 频率：每 30 分钟执行一次（实盘模式）"
        )
        last_run = get_system_value(AUTO_AI_LAST_RUN_KEY)
        if last_run:
            st.caption(f"最近一次自动驾驶: {last_run}")

        st.markdown("##### 今日动作（详细）")
        today_str = datetime.now().strftime("%Y-%m-%d")
        st.caption(f"统计日期: {today_str}")
        conn = get_db_connection()
        try:
            logs_df = pd.read_sql_query(
                "SELECT * FROM automation_logs WHERE timestamp LIKE ? ORDER BY timestamp DESC",
                conn,
                params=(f"{today_str}%",),
            )
            campaign_df = pd.read_sql_query(
                "SELECT campaign_id, campaign_name FROM campaign_settings",
                conn,
            )
        except Exception:
            logs_df = pd.DataFrame()
            campaign_df = pd.DataFrame()
        finally:
            conn.close()

        if logs_df.empty:
            st.info("今日暂无托管动作记录（或日志尚未生成）。")
            return

        id_to_name = {}
        if not campaign_df.empty:
            campaign_df["campaign_id"] = campaign_df["campaign_id"].fillna("").astype(str)
            campaign_df["campaign_name"] = campaign_df["campaign_name"].fillna("").astype(str)
            id_to_name = dict(zip(campaign_df["campaign_id"], campaign_df["campaign_name"]))

        def _display_object(value):
            raw = str(value or "")
            if raw.startswith("活动:"):
                cid = raw.split(":", 1)[-1]
                name = id_to_name.get(cid)
                return f"{name} ({cid})" if name else raw
            return raw

        logs_df = logs_df.copy()
        logs_df["object_display"] = logs_df["campaign_name"].apply(_display_object)

        st.markdown("**筛选**")
        filter_mode = st.selectbox("对象筛选", ["全部", "仅托管活动", "仅系统"])
        status_filter = st.multiselect(
            "状态筛选",
            ["已执行", "部分失败", "失败", "模拟"],
            default=["已执行", "部分失败", "失败", "模拟"],
        )
        keyword_default = target_name if target_name else ""
        keyword = st.text_input("对象关键字（可输入活动名/ASIN/系统）", value=keyword_default)

        filtered = logs_df
        if filter_mode == "仅系统":
            filtered = filtered[filtered["campaign_name"] == "系统"]
        elif filter_mode == "仅托管活动" and target_name:
            filtered = filtered[filtered["object_display"].str.contains(target_name, na=False)]

        if keyword:
            filtered = filtered[filtered["object_display"].str.contains(keyword, na=False)]

        if status_filter:
            filtered = filtered[filtered["status"].isin(status_filter)]

        if filtered.empty:
            st.info("筛选后无匹配记录。")
            return

        summary = (
            filtered.groupby(["action_type", "status"], as_index=False)
            .size()
            .sort_values(["action_type", "status"])
        )
        if not summary.empty:
            st.markdown("**动作分布**")
            st.dataframe(summary, use_container_width=True, hide_index=True)

        status_series = logs_df["status"].fillna("")
        total = len(logs_df)
        ok_count = (status_series == "已执行").sum()
        partial_count = (status_series == "部分失败").sum()
        fail_count = (status_series == "失败").sum()
        sim_count = (status_series == "模拟").sum()
        st.caption(
            f"今日共 {total} 条动作记录；已执行 {ok_count}，部分失败 {partial_count}，失败 {fail_count}，模拟 {sim_count}"
        )

        display_df = filtered.rename(
            columns={
                "timestamp": "时间",
                "object_display": "对象",
                "action_type": "动作",
                "old_value": "原值",
                "new_value": "新值",
                "reason": "原因",
                "status": "状态",
            }
        )
        st.dataframe(display_df, use_container_width=True, hide_index=True)

        st.markdown("**字段说明**")
        st.markdown(
            "- 对象：可能是活动名、ASIN 或系统\n"
            "- 动作：如预算调整、竞价调整、否词创建等\n"
            "- 原值/新值：调整前后数值（若为 0 代表非价格类动作）\n"
            "- 原因：触发动作的规则说明\n"
            "- 状态：已执行/部分失败/失败/模拟"
        )

    if hasattr(st, "dialog"):
        @st.dialog("AI托管说明与今日动作")
        def _show_ai_dialog():
            _render_ai_explain_panel()

        if st.button("🛈 查看托管说明与今日动作"):
            _show_ai_dialog()
    else:
        with st.expander("🛈 托管说明与今日动作", expanded=False):
            _render_ai_explain_panel()

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
