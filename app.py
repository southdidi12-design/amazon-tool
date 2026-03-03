import pandas as pd
import streamlit as st
from datetime import date, datetime, timedelta

from amazon_tool.automation import HAS_OPENAI
from amazon_tool.config import (
    AUTO_AI_ENABLED_KEY,
    AUTO_AI_LAST_RUN_KEY,
    AUTO_AI_LEARNING_NOTE_KEY,
    AUTO_AI_LIVE_KEY,
    AUTO_NEGATIVE_ENABLED_KEY,
    AUTO_SYNC_INTERVAL_SECONDS,
    AUTO_SYNC_REFRESH_DAYS,
    AUTO_SYNC_TS_KEY,
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
    get_db_connection,
    get_latest_report_date,
    get_product_ads_data,
    get_system_values,
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
from amazon_tool.ui.negatives import render_negative_keywords_tab
from amazon_tool.ui.product_ads import render_product_ads_tab

st.set_page_config(layout="wide", page_title=f"HNV ERP - {VERSION}", initial_sidebar_state="expanded")


def _get_bool_setting(key, default=False):
    val = get_system_value(key)
    if val is None:
        return default
    try:
        return str(val).strip().lower() in ["1", "true", "yes", "on"]
    except Exception:
        return default


def _inject_global_styles():
    st.markdown(
        """
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700;800&family=Noto+Sans+SC:wght@400;500;700&display=swap');
            :root {
                --brand-ink: #1f2a37;
                --muted-ink: #5b6574;
                --brand-primary: #0d9488;
                --brand-accent: #f97316;
                --panel-bg: rgba(255, 255, 255, 0.86);
                --panel-border: #d7e2ea;
                --ok: #0f766e;
                --warn: #b45309;
                --bad: #b91c1c;
            }
            html, body, [class*="css"] {
                font-family: 'Noto Sans SC', 'Manrope', sans-serif;
                color: var(--brand-ink);
            }
            [data-testid="stAppViewContainer"] {
                background:
                    radial-gradient(1200px 360px at 20% 0%, rgba(13, 148, 136, 0.10), transparent 55%),
                    radial-gradient(1000px 300px at 90% 0%, rgba(249, 115, 22, 0.11), transparent 55%),
                    linear-gradient(180deg, #fcfefe 0%, #f6fafb 100%);
            }
            [data-testid="stSidebar"] {
                background: linear-gradient(180deg, rgba(247, 251, 253, 0.96) 0%, rgba(241, 247, 250, 0.96) 100%);
                border-right: 1px solid var(--panel-border);
            }
            .hero-wrap {
                border: 1px solid var(--panel-border);
                border-radius: 18px;
                padding: 18px 20px 14px 20px;
                margin-bottom: 12px;
                background: linear-gradient(135deg, rgba(255,255,255,0.92), rgba(255,255,255,0.75));
            }
            .hero-title {
                margin: 0;
                font-size: 1.45rem;
                font-weight: 800;
                letter-spacing: 0.2px;
            }
            .hero-sub {
                margin-top: 8px;
                color: var(--muted-ink);
                font-size: 0.95rem;
            }
            .kpi-grid {
                display: grid;
                grid-template-columns: repeat(6, minmax(120px, 1fr));
                gap: 10px;
                margin: 12px 0 14px 0;
            }
            .kpi-card {
                border: 1px solid var(--panel-border);
                border-radius: 14px;
                padding: 10px 12px;
                background: var(--panel-bg);
                backdrop-filter: blur(3px);
                min-height: 76px;
            }
            .kpi-label {
                font-size: 0.77rem;
                color: var(--muted-ink);
                margin-bottom: 5px;
            }
            .kpi-value {
                font-size: 1.06rem;
                font-weight: 800;
                line-height: 1.2;
            }
            .workflow-grid {
                display: grid;
                grid-template-columns: repeat(4, minmax(120px, 1fr));
                gap: 10px;
                margin: 4px 0 14px 0;
            }
            .flow-step {
                border: 1px dashed #bdd1da;
                border-radius: 12px;
                padding: 10px 12px;
                background: rgba(255,255,255,0.70);
            }
            .flow-no {
                font-size: 0.72rem;
                font-weight: 700;
                color: var(--brand-primary);
                letter-spacing: 0.5px;
            }
            .flow-title {
                margin-top: 2px;
                font-size: 0.93rem;
                font-weight: 700;
            }
            .flow-tip {
                margin-top: 4px;
                color: var(--muted-ink);
                font-size: 0.8rem;
                line-height: 1.35;
            }
            .status-chip {
                display: inline-block;
                border-radius: 999px;
                padding: 3px 10px;
                font-size: 0.75rem;
                font-weight: 700;
                margin: 0 6px 6px 0;
            }
            .chip-ok { background: rgba(15, 118, 110, 0.14); color: var(--ok); }
            .chip-warn { background: rgba(180, 83, 9, 0.14); color: var(--warn); }
            .chip-bad { background: rgba(185, 28, 28, 0.14); color: var(--bad); }
            .chip-neutral { background: rgba(71, 85, 105, 0.12); color: #334155; }
            div[data-testid="stMetric"] {
                border: 1px solid var(--panel-border);
                border-radius: 12px;
                padding: 6px 8px;
                background: rgba(255, 255, 255, 0.82);
            }
            @media (max-width: 1100px) {
                .kpi-grid { grid-template-columns: repeat(3, minmax(120px, 1fr)); }
                .workflow-grid { grid-template-columns: repeat(2, minmax(120px, 1fr)); }
            }
            @media (max-width: 760px) {
                .kpi-grid, .workflow-grid { grid-template-columns: 1fr; }
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _load_campaign_mappings():
    conn = get_db_connection()
    try:
        campaign_df = pd.read_sql_query("SELECT campaign_id, campaign_name FROM campaign_settings", conn)
    except Exception:
        campaign_df = pd.DataFrame()
    finally:
        conn.close()
    if campaign_df.empty:
        return campaign_df, {}, {}
    campaign_df["campaign_id"] = campaign_df["campaign_id"].fillna("").astype(str)
    campaign_df["campaign_name"] = campaign_df["campaign_name"].fillna("").astype(str)
    id_to_name = dict(zip(campaign_df["campaign_id"], campaign_df["campaign_name"]))
    name_to_id = dict(zip(campaign_df["campaign_name"], campaign_df["campaign_id"]))
    return campaign_df, id_to_name, name_to_id


def _resolve_whitelist_display(whitelist, id_to_name, name_to_id):
    display_whitelist = []
    resolved_names = []
    for item in whitelist:
        val = str(item).strip()
        if not val:
            continue
        if val in id_to_name:
            cname = id_to_name.get(val) or ""
            display_whitelist.append(f"{cname} ({val})" if cname else val)
            resolved_names.append(cname or val)
        elif val in name_to_id:
            cid = name_to_id.get(val) or ""
            display_whitelist.append(f"{val} ({cid})" if cid else val)
            resolved_names.append(val)
        else:
            display_whitelist.append(val)
            resolved_names.append(val)
    return display_whitelist, resolved_names


def _render_sidebar_panel():
    with st.sidebar:
        st.markdown("### 🎛️ 运营控制台")
        st.caption(f"版本: {VERSION}")

        deepseek_key = st.text_input("DeepSeek Key", type="password", placeholder="输入后可用 AI 语义能力")

        engine_chip = "chip-ok" if HAS_OPENAI else "chip-warn"
        engine_text = "AI 引擎在线" if HAS_OPENAI else "AI 引擎离线"
        st.markdown(
            f"<span class='status-chip {engine_chip}'>{engine_text}</span>",
            unsafe_allow_html=True,
        )

        st.markdown("#### 🔄 数据同步")
        if st.button("🚀 立即同步最近 7 天", type="primary", use_container_width=True):
            with st.status("正在同步数据...", expanded=True) as sync_status_ui:
                ok = run_sync_task_guarded(7, sync_status_ui)
                if ok:
                    set_system_value(AUTO_SYNC_TS_KEY, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                    sync_status_ui.update(label="同步完成", state="complete")
                else:
                    sync_status_ui.update(label="同步未启动（可能已有任务在运行）", state="error")
            st.rerun()

        system_vals = get_system_values(
            [
                AUTO_SYNC_TS_KEY,
                SYNC_STATUS_KEY,
                SYNC_DAYS_KEY,
                SYNC_ERROR_KEY,
                AUTO_AI_LAST_RUN_KEY,
                AUTO_AI_LEARNING_NOTE_KEY,
            ]
        )
        last_sync_ts = system_vals.get(AUTO_SYNC_TS_KEY) or "未执行"
        sync_status = system_vals.get(SYNC_STATUS_KEY) or "未知"
        sync_days = system_vals.get(SYNC_DAYS_KEY) or "-"
        sync_error = system_vals.get(SYNC_ERROR_KEY)
        latest_report = get_latest_report_date() or "暂无"
        hours = max(1, int(AUTO_SYNC_INTERVAL_SECONDS / 3600))
        st.caption(f"自动同步频率: 每 {hours} 小时回补最近 {AUTO_SYNC_REFRESH_DAYS} 天")
        st.caption(f"最近同步时间: {last_sync_ts}")
        st.caption(f"同步状态: {sync_status} (天数: {sync_days})")
        st.caption(f"最新数据日期: {latest_report}")
        if sync_error:
            st.warning(sync_error)

        st.divider()
        st.markdown("#### 🤖 托管状态")

        whitelist = [w for w in get_auto_ai_campaign_whitelist() if str(w).strip()]
        campaign_df, id_to_name, name_to_id = _load_campaign_mappings()
        display_whitelist, resolved_names = _resolve_whitelist_display(whitelist, id_to_name, name_to_id)
        ai_enabled = _get_bool_setting(AUTO_AI_ENABLED_KEY, False)
        ai_live = _get_bool_setting(AUTO_AI_LIVE_KEY, False)
        auto_neg_enabled = _get_bool_setting(AUTO_NEGATIVE_ENABLED_KEY, False)
        target_name = resolved_names[0] if resolved_names else ""

        if display_whitelist:
            st.caption(f"托管活动: {len(display_whitelist)} 个")
            st.caption("；".join(display_whitelist[:3]) + ("；..." if len(display_whitelist) > 3 else ""))
        else:
            st.caption("托管活动: 未配置")

        if not ai_enabled:
            st.markdown("<span class='status-chip chip-warn'>托管未开启</span>", unsafe_allow_html=True)
        elif ai_live:
            st.markdown("<span class='status-chip chip-ok'>托管实盘中</span>", unsafe_allow_html=True)
        else:
            st.markdown("<span class='status-chip chip-neutral'>托管模拟中</span>", unsafe_allow_html=True)

        neg_chip = "chip-ok" if auto_neg_enabled else "chip-neutral"
        neg_text = "自动否词开启" if auto_neg_enabled else "自动否词关闭"
        st.markdown(f"<span class='status-chip {neg_chip}'>{neg_text}</span>", unsafe_allow_html=True)

        last_auto = system_vals.get(AUTO_AI_LAST_RUN_KEY)
        if last_auto:
            st.caption(f"最近自动驾驶: {last_auto}")

        learning_note = system_vals.get(AUTO_AI_LEARNING_NOTE_KEY)
        if learning_note:
            st.caption(f"最近学习: {learning_note}")

        st.divider()
        show_advanced = st.checkbox("显示高级功能", value=False)

    return {
        "deepseek_key": deepseek_key,
        "show_advanced": show_advanced,
        "target_name": target_name,
        "latest_report": latest_report,
        "sync_status": sync_status,
        "ai_enabled": ai_enabled,
        "ai_live": ai_live,
    }


def _classify_acos(acos_pct):
    if acos_pct is None:
        return "暂无", "chip-neutral"
    if acos_pct <= 25:
        return "健康", "chip-ok"
    if acos_pct <= 35:
        return "偏高", "chip-warn"
    return "风险", "chip-bad"


def _render_overview_cards(df, latest_report, sync_status, ai_enabled, ai_live):
    if df.empty:
        total_cost = 0.0
        total_sales = 0.0
        total_clicks = 0
        total_orders = 0
        acos_pct = None
        cvr_pct = None
    else:
        total_cost = float(df["cost"].sum())
        total_sales = float(df["sales"].sum())
        total_clicks = int(df["clicks"].sum())
        total_orders = int(df["orders"].sum())
        acos_pct = (total_cost / total_sales * 100) if total_sales > 0 else None
        cvr_pct = (total_orders / total_clicks * 100) if total_clicks > 0 else None

    health_label, health_class = _classify_acos(acos_pct)
    acos_text = f"{acos_pct:.1f}%" if acos_pct is not None else "-"
    cvr_text = f"{cvr_pct:.1f}%" if cvr_pct is not None else "-"
    ai_mode = "实盘" if ai_live else "模拟"
    ai_text = f"已开启 ({ai_mode})" if ai_enabled else "未开启"

    st.markdown(
        f"""
        <div class="kpi-grid">
            <div class="kpi-card"><div class="kpi-label">总花费</div><div class="kpi-value">${total_cost:.2f}</div></div>
            <div class="kpi-card"><div class="kpi-label">总销售</div><div class="kpi-value">${total_sales:.2f}</div></div>
            <div class="kpi-card"><div class="kpi-label">ACOS</div><div class="kpi-value">{acos_text}</div></div>
            <div class="kpi-card"><div class="kpi-label">转化率</div><div class="kpi-value">{cvr_text}</div></div>
            <div class="kpi-card"><div class="kpi-label">托管状态</div><div class="kpi-value">{ai_text}</div></div>
            <div class="kpi-card"><div class="kpi-label">数据更新</div><div class="kpi-value">{latest_report}</div></div>
        </div>
        <div>
            <span class="status-chip {health_class}">投放健康: {health_label}</span>
            <span class="status-chip chip-neutral">同步状态: {sync_status}</span>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_workflow_strip():
    st.markdown(
        """
        <div class="workflow-grid">
            <div class="flow-step">
                <div class="flow-no">STEP 1</div>
                <div class="flow-title">先同步数据</div>
                <div class="flow-tip">左侧点击“立即同步最近 7 天”，确保今天数据完整。</div>
            </div>
            <div class="flow-step">
                <div class="flow-no">STEP 2</div>
                <div class="flow-title">看经营总览</div>
                <div class="flow-tip">优先看 ACOS、花费、转化率，再决定是否放量。</div>
            </div>
            <div class="flow-step">
                <div class="flow-no">STEP 3</div>
                <div class="flow-title">调自动驾驶</div>
                <div class="flow-tip">在自动驾驶页改规则并保存，按模拟→实盘推进。</div>
            </div>
            <div class="flow-step">
                <div class="flow-no">STEP 4</div>
                <div class="flow-title">复盘动作日志</div>
                <div class="flow-tip">查看今日动作是否符合预期，再继续迭代参数。</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_ai_log_center(target_name):
    st.markdown("#### 托管规则")
    st.markdown(
        "- 仅对白名单活动执行自动优化\n"
        "- 活动预算锁定：自动驾驶不会修改你后台设置的预算\n"
        "- 竞价按 ACOS、止损、流量信号动态调整\n"
        "- 表现不佳时自动暂停放量；表现达标才放量\n"
        "- 启用持续学习后，系统会按近 7 天表现小步调参"
    )
    last_run = get_system_value(AUTO_AI_LAST_RUN_KEY)
    if last_run:
        st.caption(f"最近自动驾驶运行: {last_run}")

    today_str = datetime.now().strftime("%Y-%m-%d")
    conn = get_db_connection()
    try:
        logs_df = pd.read_sql_query(
            "SELECT * FROM automation_logs WHERE timestamp LIKE ? ORDER BY timestamp DESC",
            conn,
            params=(f"{today_str}%",),
        )
        campaign_df = pd.read_sql_query("SELECT campaign_id, campaign_name FROM campaign_settings", conn)
    except Exception:
        logs_df = pd.DataFrame()
        campaign_df = pd.DataFrame()
    finally:
        conn.close()

    if logs_df.empty:
        st.info("今日暂无托管动作记录。")
        return

    id_to_name = {}
    if not campaign_df.empty:
        campaign_df["campaign_id"] = campaign_df["campaign_id"].fillna("").astype(str)
        campaign_df["campaign_name"] = campaign_df["campaign_name"].fillna("").astype(str)
        id_to_name = dict(zip(campaign_df["campaign_id"], campaign_df["campaign_name"]))

    logs_df = logs_df.copy()

    def _display_object(raw_value):
        raw = str(raw_value or "")
        if raw.startswith("活动:"):
            cid = raw.split(":", 1)[-1]
            cname = id_to_name.get(cid)
            return f"{cname} ({cid})" if cname else raw
        return raw

    logs_df["对象"] = logs_df["campaign_name"].apply(_display_object)
    status_series = logs_df["status"].fillna("").astype(str)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("今日动作数", len(logs_df))
    c2.metric("已执行", int((status_series == "已执行").sum()))
    c3.metric("失败/部分失败", int((status_series.isin(["失败", "部分失败"])).sum()))
    c4.metric("模拟/暂停/锁定", int((status_series.isin(["模拟", "暂停", "锁定"])).sum()))

    filter_col1, filter_col2, filter_col3 = st.columns([1.4, 1, 1.6])
    with filter_col1:
        default_kw = target_name if target_name else ""
        keyword = st.text_input("对象关键字", value=default_kw, key="log_filter_keyword")
    with filter_col2:
        action_keyword = st.text_input("动作关键字", value="", key="log_filter_action")
    with filter_col3:
        status_options = ["已执行", "部分失败", "失败", "模拟", "暂停", "锁定"]
        selected_status = st.multiselect("状态筛选", status_options, default=status_options)

    filtered = logs_df.copy()
    if keyword:
        filtered = filtered[filtered["对象"].astype(str).str.contains(keyword, na=False)]
    if action_keyword:
        filtered = filtered[filtered["action_type"].astype(str).str.contains(action_keyword, na=False)]
    if selected_status:
        filtered = filtered[filtered["status"].astype(str).isin(selected_status)]

    if filtered.empty:
        st.info("筛选后无匹配记录。")
        return

    display_df = filtered.rename(
        columns={
            "timestamp": "时间",
            "action_type": "动作",
            "old_value": "原值",
            "new_value": "新值",
            "reason": "原因",
            "status": "状态",
        }
    )
    st.dataframe(
        display_df[["时间", "对象", "动作", "原值", "新值", "原因", "状态"]],
        use_container_width=True,
        hide_index=True,
    )


def _resolve_date_range(today):
    quick_map = {"最近7天": 7, "最近14天": 14, "最近30天": 30}
    c1, c2 = st.columns([1.2, 3.2])
    with c1:
        quick_choice = st.selectbox("分析周期", ["最近7天", "最近14天", "最近30天", "自定义"], index=0)
    with c2:
        if quick_choice == "自定义":
            dr = st.date_input("自定义日期范围", value=(today - timedelta(days=7), today))
            if isinstance(dr, tuple) and len(dr) == 2:
                s_d, e_d = dr
            else:
                s_d = e_d = today
        else:
            days = quick_map.get(quick_choice, 7)
            s_d = today - timedelta(days=days)
            e_d = today
            st.caption(f"当前使用 {quick_choice}：{s_d} 至 {e_d}")
    if isinstance(s_d, datetime):
        s_d = s_d.date()
    if isinstance(e_d, datetime):
        e_d = e_d.date()
    return s_d, e_d


# --- App bootstrap ---
_inject_global_styles()
init_db()
start_auto_sync()
ui_ctx = _render_sidebar_panel()

today = get_real_today()
st.markdown(
    """
    <div class="hero-wrap">
        <h1 class="hero-title">亚马逊广告全托管中心</h1>
        <div class="hero-sub">围绕“同步 → 诊断 → 自动驾驶 → 复盘”设计，降低操作负担，突出关键决策信息。</div>
    </div>
    """,
    unsafe_allow_html=True,
)

s_d, e_d = _resolve_date_range(today)
df = get_dashboard_data(s_d.strftime("%Y-%m-%d"), e_d.strftime("%Y-%m-%d"))
trend = get_trend_data(s_d.strftime("%Y-%m-%d"), e_d.strftime("%Y-%m-%d"))

asin_df = None
product_ads_df = None
if ui_ctx["show_advanced"]:
    asin_df = get_asin_dashboard_data(s_d.strftime("%Y-%m-%d"), e_d.strftime("%Y-%m-%d"))
    product_ads_df = get_product_ads_data()

_render_overview_cards(
    df,
    ui_ctx["latest_report"],
    ui_ctx["sync_status"],
    ui_ctx["ai_enabled"],
    ui_ctx["ai_live"],
)
_render_workflow_strip()

if df.empty and (asin_df is None or asin_df.empty) and (product_ads_df is None or product_ads_df.empty):
    st.warning("当前日期范围暂无数据。建议先同步后再查看，或切换到更早的日期区间。")
else:
    base_tabs = ["📈 经营总览", "🤖 自动驾驶", "🧰 操作中心", "🧾 托管日志"]
    adv_tabs = ["🧩 ASIN 维度", "🧾 商品广告", "🚫 否词管理"] if ui_ctx["show_advanced"] else []
    tabs = st.tabs(base_tabs + adv_tabs)

    with tabs[0]:
        render_dashboard_tab(df, trend)

    with tabs[1]:
        render_autopilot_tab(ui_ctx["deepseek_key"])

    with tabs[2]:
        render_manage_tab(df)

    with tabs[3]:
        _render_ai_log_center(ui_ctx["target_name"])

    if ui_ctx["show_advanced"]:
        with tabs[4]:
            render_asin_tab(asin_df)

        with tabs[5]:
            render_product_ads_tab(product_ads_df)

        with tabs[6]:
            render_negative_keywords_tab()
