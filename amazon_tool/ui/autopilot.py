from datetime import datetime

import pandas as pd
import streamlit as st

from ..amazon_api import get_amazon_session_and_headers, list_sp_campaigns, update_sp_campaign_states
from ..automation import run_optimization_logic
from ..config import (
    AUTO_AI_BASELINE_MIN_KEY,
    AUTO_AI_ENABLED_KEY,
    AUTO_AI_HARVEST_MIN_ORDERS_KEY,
    AUTO_AI_LAST_RUN_KEY,
    AUTO_AI_LEARNING_ENABLED_KEY,
    AUTO_AI_LEARNING_RATE_KEY,
    AUTO_AI_LIVE_KEY,
    AUTO_AI_MAX_BID_KEY,
    AUTO_AI_MAX_UP_PCT_KEY,
    AUTO_AI_MEMORY_FLOOR_RATIO_KEY,
    AUTO_AI_MIN_BID_CLOSE_KEY,
    AUTO_AI_MIN_BID_COMP_KEY,
    AUTO_AI_MIN_BID_KEY,
    AUTO_AI_MIN_BID_LOOSE_KEY,
    AUTO_AI_MIN_BID_SUB_KEY,
    AUTO_AI_STOP_LOSS_KEY,
    AUTO_AI_TARGET_ACOS_KEY,
    AUTO_NEGATIVE_ACOS_MULT_KEY,
    AUTO_NEGATIVE_CLICKS_KEY,
    AUTO_NEGATIVE_DAYS_KEY,
    AUTO_NEGATIVE_ENABLED_KEY,
    AUTO_NEGATIVE_LEVEL_KEY,
    AUTO_NEGATIVE_MATCH_KEY,
    AUTO_NEGATIVE_SPEND_KEY,
    get_auto_ai_campaign_whitelist,
)
from ..db import get_db_connection, get_system_value, set_system_value


def _get_float_setting(key, default):
    val = get_system_value(key)
    if val is None:
        return default
    try:
        return float(val)
    except Exception:
        return default


def _get_int_setting(key, default):
    val = get_system_value(key)
    if val is None:
        return default
    try:
        return int(float(val))
    except Exception:
        return default


def _get_bool_setting(key, default):
    val = get_system_value(key)
    if val is None:
        return default
    try:
        return str(val).strip() in ["1", "true", "True", "yes", "YES", "on", "ON"]
    except Exception:
        return default


def _chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _one_click_disable_ai_ads():
    set_system_value(AUTO_AI_ENABLED_KEY, "0")
    set_system_value(AUTO_AI_LIVE_KEY, "0")

    session, headers = get_amazon_session_and_headers()
    if not session:
        return False, "已关闭AI开关，但未检测到Amazon API配置，未执行活动暂停"

    whitelist = [w.strip() for w in get_auto_ai_campaign_whitelist() if str(w).strip()]
    if not whitelist:
        return True, "已关闭AI开关；白名单为空，无需暂停活动"
    whitelist_set = {str(w).strip() for w in whitelist}

    campaigns = list_sp_campaigns(session, headers, include_extended=True)
    if not campaigns:
        return True, "已关闭AI开关；未拉到活动列表，未执行暂停"

    to_pause = []
    for c in campaigns:
        campaign_id = str(c.get("campaignId", c.get("campaign_id")) or "").strip()
        if not campaign_id:
            continue
        campaign_name = str(c.get("name", c.get("campaignName", "")) or "").strip()
        if campaign_id not in whitelist_set and campaign_name not in whitelist_set:
            continue
        state = str(c.get("state", "") or "").upper()
        if state in ["PAUSED", "ARCHIVED"]:
            continue
        to_pause.append({"campaignId": campaign_id, "state": "PAUSED"})

    if not to_pause:
        return True, "已关闭AI开关；白名单活动已是暂停状态"

    failed = 0
    for batch in _chunked(to_pause, 50):
        ok, _ = update_sp_campaign_states(session, headers, batch)
        if not ok:
            failed += len(batch)

    paused = len(to_pause) - failed
    if failed > 0:
        return False, f"已关闭AI开关；活动暂停部分失败（成功{paused}，失败{failed}）"
    return True, f"已关闭AI开关，并暂停白名单活动 {paused} 个"


def render_autopilot_tab(deepseek_key):
    c_set, c_log = st.columns([1, 2])

    with c_set:
        st.markdown("#### 极简自动驾驶")

        default_target = _get_float_setting(AUTO_AI_TARGET_ACOS_KEY, 25.0)
        default_max_bid = _get_float_setting(AUTO_AI_MAX_BID_KEY, 0.8)
        default_harvest_min_orders = _get_int_setting(AUTO_AI_HARVEST_MIN_ORDERS_KEY, 1)
        default_stop_loss = _get_float_setting(AUTO_AI_STOP_LOSS_KEY, 15.0)

        target_acos = st.slider("目标 ACOS (%)", 10, 60, int(min(max(default_target, 10), 60)))
        max_bid = st.number_input("最高出价 ($)", 0.02, 10.0, float(min(max(default_max_bid, 0.02), 10.0)))
        harvest_min_orders = st.slider("收词最低订单数", 1, 5, int(min(max(default_harvest_min_orders, 1), 5)))

        mode = st.radio("运行模式", ["🧪 模拟", "🔥 实弹"], horizontal=True)
        st.caption("固定规则：仅白名单活动；预算按22小时节奏小步控速（快烧微降、慢烧微提）；只收关键词到精准投放(EXACT)。")

        st.divider()
        auto_enabled = st.checkbox("自动驾驶开启", value=_get_bool_setting(AUTO_AI_ENABLED_KEY, False))
        auto_live = st.checkbox("自动驾驶实弹", value=_get_bool_setting(AUTO_AI_LIVE_KEY, False))
        last_run = get_system_value(AUTO_AI_LAST_RUN_KEY)
        if last_run:
            st.caption(f"最近运行: {last_run}")
        if st.button("一键关闭AI广告", use_container_width=True):
            ok, msg = _one_click_disable_ai_ads()
            if ok:
                st.success(msg)
            else:
                st.warning(msg)

        if st.button("⚡ 运行 AI 引擎", type="primary", use_container_width=True):
            with st.status("AI 正在执行极简策略...", expanded=True) as s:
                logs = run_optimization_logic(
                    target_acos,
                    max_bid,
                    float(default_stop_loss),
                    mode.startswith("🔥"),
                    deepseek_key,
                    auto_negative_config={"enabled": False},
                )
                s.update(label=f"✅ 完成：{len(logs)} 条动作", state="complete")

        if st.button("💾 保存设置", use_container_width=True):
            min_bid = max(0.02, min(max_bid, _get_float_setting(AUTO_AI_MIN_BID_KEY, 0.2)))
            min_bid_close = max(
                min_bid,
                min(max_bid, _get_float_setting(AUTO_AI_MIN_BID_CLOSE_KEY, max(min_bid, 0.3))),
            )
            min_bid_loose = max(
                min_bid,
                min(max_bid, _get_float_setting(AUTO_AI_MIN_BID_LOOSE_KEY, max(min_bid, 0.25))),
            )
            min_bid_sub = max(
                min_bid,
                min(max_bid, _get_float_setting(AUTO_AI_MIN_BID_SUB_KEY, max(min_bid, 0.2))),
            )
            min_bid_comp = max(
                min_bid,
                min(max_bid, _get_float_setting(AUTO_AI_MIN_BID_COMP_KEY, max(min_bid, 0.15))),
            )
            baseline_min_bid = max(
                min_bid,
                min(max_bid, _get_float_setting(AUTO_AI_BASELINE_MIN_KEY, max(min_bid, 0.5))),
            )
            set_system_value(AUTO_AI_ENABLED_KEY, "1" if auto_enabled else "0")
            set_system_value(AUTO_AI_LIVE_KEY, "1" if auto_live else "0")
            set_system_value(AUTO_AI_TARGET_ACOS_KEY, str(target_acos))
            set_system_value(AUTO_AI_MAX_BID_KEY, str(max_bid))
            set_system_value(AUTO_AI_HARVEST_MIN_ORDERS_KEY, str(harvest_min_orders))
            set_system_value(AUTO_AI_STOP_LOSS_KEY, str(default_stop_loss))

            # 固定高级参数，避免策略复杂化
            set_system_value(AUTO_AI_MIN_BID_KEY, str(min_bid))
            set_system_value(AUTO_AI_MIN_BID_CLOSE_KEY, str(min_bid_close))
            set_system_value(AUTO_AI_MIN_BID_LOOSE_KEY, str(min_bid_loose))
            set_system_value(AUTO_AI_MIN_BID_SUB_KEY, str(min_bid_sub))
            set_system_value(AUTO_AI_MIN_BID_COMP_KEY, str(min_bid_comp))
            set_system_value(AUTO_AI_MAX_UP_PCT_KEY, "100")
            set_system_value(AUTO_AI_BASELINE_MIN_KEY, str(baseline_min_bid))
            set_system_value(AUTO_AI_MEMORY_FLOOR_RATIO_KEY, "90")
            set_system_value(AUTO_AI_LEARNING_ENABLED_KEY, "0")
            set_system_value(AUTO_AI_LEARNING_RATE_KEY, "1.0")

            # 极简模式关闭自动否词
            set_system_value(AUTO_NEGATIVE_ENABLED_KEY, "0")
            set_system_value(AUTO_NEGATIVE_LEVEL_KEY, "adgroup")
            set_system_value(AUTO_NEGATIVE_MATCH_KEY, "NEGATIVE_EXACT")
            set_system_value(AUTO_NEGATIVE_DAYS_KEY, "7")
            set_system_value(AUTO_NEGATIVE_SPEND_KEY, "3.0")
            set_system_value(AUTO_NEGATIVE_CLICKS_KEY, "8")
            set_system_value(AUTO_NEGATIVE_ACOS_MULT_KEY, "1.5")

            st.success("极简设置已保存")

    with c_log:
        st.markdown("#### 自动驾驶日志")
        conn = get_db_connection()
        try:
            logs_df = pd.read_sql("SELECT * FROM automation_logs ORDER BY timestamp DESC LIMIT 120", conn)
        except Exception:
            logs_df = pd.DataFrame()
        finally:
            conn.close()

        if logs_df.empty:
            st.info("暂无日志")
        else:
            view_df = logs_df.rename(
                columns={
                    "timestamp": "时间",
                    "campaign_name": "活动",
                    "action_type": "动作",
                    "old_value": "原值",
                    "new_value": "新值",
                    "reason": "原因",
                    "status": "状态",
                }
            )
            st.dataframe(view_df, use_container_width=True, hide_index=True)
