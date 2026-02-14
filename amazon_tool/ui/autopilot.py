import pandas as pd
from datetime import datetime
import streamlit as st

from ..automation import run_optimization_logic
from ..config import (
    AUTO_AI_ENABLED_KEY,
    AUTO_AI_LEARNING_ENABLED_KEY,
    AUTO_AI_LEARNING_NOTE_KEY,
    AUTO_AI_LEARNING_RATE_KEY,
    AUTO_AI_LAST_RUN_KEY,
    AUTO_AI_LIVE_KEY,
    AUTO_AI_MAX_BID_KEY,
    AUTO_AI_MAX_UP_PCT_KEY,
    AUTO_AI_BASELINE_MIN_KEY,
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
    AUTO_NEGATIVE_LAST_RUN_KEY,
    AUTO_NEGATIVE_LEVEL_KEY,
    AUTO_NEGATIVE_MATCH_KEY,
    AUTO_NEGATIVE_SPEND_KEY,
)
from ..db import (
    get_auto_negative_keywords,
    get_db_connection,
    get_negative_product_targets,
    get_system_value,
    set_system_value,
)


def _get_float_setting(key, default):
    val = get_system_value(key)
    if val is None:
        return default
    try:
        return float(val)
    except Exception:
        return default




def _load_campaign_map():
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT campaign_id, campaign_name FROM campaign_settings", conn)
    except Exception:
        return {}
    finally:
        conn.close()
    if df.empty:
        return {}
    df["campaign_id"] = df["campaign_id"].fillna("").astype(str)
    df["campaign_name"] = df["campaign_name"].fillna("")
    return dict(zip(df["campaign_id"], df["campaign_name"]))


def _load_adgroup_map():
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT ad_group_id, ad_group_name FROM ad_group_settings", conn)
    except Exception:
        return {}
    finally:
        conn.close()
    if df.empty:
        return {}
    df["ad_group_id"] = df["ad_group_id"].fillna("").astype(str)
    df["ad_group_name"] = df["ad_group_name"].fillna("")
    return dict(zip(df["ad_group_id"], df["ad_group_name"]))


def _get_bool_setting(key, default):
    val = get_system_value(key)
    if val is None:
        return default
    try:
        return str(val).strip() in ["1", "true", "True", "yes", "YES", "on", "ON"]
    except Exception:
        return default


def render_autopilot_tab(deepseek_key):
    c_set, c_act = st.columns([1, 2])
    with c_set:
        st.markdown("#### ⚙️ 规则设定")
        default_target = _get_float_setting(AUTO_AI_TARGET_ACOS_KEY, 25.0)
        default_max_bid = _get_float_setting(AUTO_AI_MAX_BID_KEY, 2.5)
        default_stop_loss = _get_float_setting(AUTO_AI_STOP_LOSS_KEY, 15.0)

        target_default = int(min(max(default_target, 10), 60))
        max_bid_default = float(min(max(default_max_bid, 0.02), 10.0))
        stop_loss_default = float(min(max(default_stop_loss, 1.0), 100.0))

        target_acos = st.slider("目标 ACOS (%)", 10, 60, target_default)
        max_bid = st.number_input("最高出价 ($)", 0.02, 10.0, max_bid_default)
        stop_loss = st.number_input("止损阈值 ($/7天)", 1.0, 100.0, stop_loss_default)
        default_min_bid = _get_float_setting(AUTO_AI_MIN_BID_KEY, 0.2)
        default_min_close = _get_float_setting(AUTO_AI_MIN_BID_CLOSE_KEY, max(default_min_bid, 0.3))
        default_min_loose = _get_float_setting(AUTO_AI_MIN_BID_LOOSE_KEY, max(default_min_bid, 0.25))
        default_min_sub = _get_float_setting(AUTO_AI_MIN_BID_SUB_KEY, max(default_min_bid, 0.2))
        default_min_comp = _get_float_setting(AUTO_AI_MIN_BID_COMP_KEY, max(default_min_bid, 0.15))
        default_up_pct = _get_float_setting(AUTO_AI_MAX_UP_PCT_KEY, 100.0)
        default_baseline_min = _get_float_setting(AUTO_AI_BASELINE_MIN_KEY, 0.5)
        default_memory_ratio = _get_float_setting(AUTO_AI_MEMORY_FLOOR_RATIO_KEY, 0.9)
        if default_memory_ratio <= 1:
            default_memory_ratio = default_memory_ratio * 100
        if default_up_pct <= 3:
            default_up_pct = default_up_pct * 100
        st.markdown("##### 出价下限 / 爬坡")
        min_bid = st.number_input("通用最低出价 ($)", 0.02, 10.0, float(min(default_min_bid, 10.0)))
        c1, c2 = st.columns(2)
        with c1:
            min_close = st.number_input("自动-紧密最低出价 ($)", 0.02, 10.0, float(min(default_min_close, 10.0)))
            min_sub = st.number_input("自动-替代最低出价 ($)", 0.02, 10.0, float(min(default_min_sub, 10.0)))
        with c2:
            min_loose = st.number_input("自动-宽泛最低出价 ($)", 0.02, 10.0, float(min(default_min_loose, 10.0)))
            min_comp = st.number_input("自动-互补最低出价 ($)", 0.02, 10.0, float(min(default_min_comp, 10.0)))
        max_up_pct = st.slider("每次上调上限 (%)", 10, 300, int(min(default_up_pct, 300)))
        st.markdown("##### 记忆价保护")
        baseline_min_bid = st.number_input("记忆价最低保护 ($)", 0.02, 10.0, float(min(default_baseline_min, 10.0)))
        memory_ratio = st.slider("记忆价保护比例 (%)", 50, 100, int(min(default_memory_ratio, 100)))
        st.caption("用于分步抬价，避免一次性拉升。")
        mode = st.radio("模式", ["🧪 模拟", "🔥 实弹"])
        st.caption("实弹将调整 SP 关键词/投放/广告位倍率；不修改活动预算。")

        # 自动否词参数默认值
        neg_enabled = _get_bool_setting(AUTO_NEGATIVE_ENABLED_KEY, False)
        neg_level = get_system_value(AUTO_NEGATIVE_LEVEL_KEY) or "adgroup"
        neg_match = get_system_value(AUTO_NEGATIVE_MATCH_KEY) or "NEGATIVE_EXACT"
        neg_spend = _get_float_setting(AUTO_NEGATIVE_SPEND_KEY, 3.0)
        neg_clicks = _get_float_setting(AUTO_NEGATIVE_CLICKS_KEY, 8.0)
        neg_acos_mult = _get_float_setting(AUTO_NEGATIVE_ACOS_MULT_KEY, 1.5)
        neg_days = _get_float_setting(AUTO_NEGATIVE_DAYS_KEY, 7.0)

        st.divider()
        st.markdown("#### 🤖 自动驾驶")
        auto_enabled = _get_bool_setting(AUTO_AI_ENABLED_KEY, False)
        auto_live = _get_bool_setting(AUTO_AI_LIVE_KEY, False)
        learn_enabled = _get_bool_setting(AUTO_AI_LEARNING_ENABLED_KEY, True)
        learn_rate = _get_float_setting(AUTO_AI_LEARNING_RATE_KEY, 1.0)
        learn_rate = max(0.5, min(learn_rate, 2.0))
        auto_enabled = st.checkbox("自动驾驶开启", value=auto_enabled)
        auto_live = st.checkbox("自动驾驶实弹", value=auto_live)
        learn_enabled = st.checkbox("持续学习开启", value=learn_enabled)
        learn_rate = st.slider("学习强度 (%)", 50, 200, int(learn_rate * 100))
        st.caption("自动驾驶需要配合计划任务运行（脚本: scripts/install_autopilot_task.ps1）。")
        last_run = get_system_value(AUTO_AI_LAST_RUN_KEY)
        if last_run:
            st.caption(f"自动驾驶最近运行: {last_run}")
        learn_note = get_system_value(AUTO_AI_LEARNING_NOTE_KEY)
        if learn_note:
            st.caption(f"持续学习最近动作: {learn_note}")

        st.markdown("#### 🚫 自动否词")
        neg_enabled = st.checkbox("自动否词开启", value=neg_enabled)
        neg_level = st.selectbox(
            "否词层级",
            ["广告组否词", "活动级否词"],
            index=0 if str(neg_level).lower().startswith("ad") else 1,
        )
        neg_match = st.selectbox(
            "匹配方式",
            ["否定精准", "否定词组"],
            index=0 if str(neg_match).upper() == "NEGATIVE_EXACT" else 1,
        )
        neg_days = st.slider("统计天数", 3, 30, int(neg_days))
        neg_spend = st.number_input("触发花费阈值 ($)", 0.0, 100.0, float(neg_spend))
        neg_clicks = st.number_input("触发点击阈值", 0.0, 200.0, float(neg_clicks))
        neg_acos_mult = st.number_input("ACOS 放大倍数", 1.0, 5.0, float(neg_acos_mult))
        neg_last = get_system_value(AUTO_NEGATIVE_LAST_RUN_KEY)
        if neg_last:
            st.caption(f"自动否词最近运行: {neg_last}")

        auto_neg_config = {
            "enabled": neg_enabled,
            "level": "campaign" if neg_level == "活动级否词" else "adgroup",
            "match": "NEGATIVE_EXACT" if neg_match == "否定精准" else "NEGATIVE_PHRASE",
            "spend": neg_spend,
            "clicks": neg_clicks,
            "acos_mult": neg_acos_mult,
            "days": neg_days,
        }

        if st.button("⚡ 运行 AI 引擎", type="primary", use_container_width=True):
            with st.status("AI 正在工作...", expanded=True) as s:
                logs = run_optimization_logic(
                    target_acos,
                    max_bid,
                    stop_loss,
                    mode.startswith("🔥"),
                    deepseek_key,
                    auto_negative_config=auto_neg_config,
                )
                s.update(label=f"✅ 完成！生成 {len(logs)} 条指令", state="complete")
        if st.button("💾 保存自动驾驶设置", use_container_width=True):
            set_system_value(AUTO_AI_ENABLED_KEY, "1" if auto_enabled else "0")
            set_system_value(AUTO_AI_LIVE_KEY, "1" if auto_live else "0")
            set_system_value(AUTO_AI_LEARNING_ENABLED_KEY, "1" if learn_enabled else "0")
            set_system_value(AUTO_AI_LEARNING_RATE_KEY, str(learn_rate / 100.0))
            set_system_value(AUTO_AI_TARGET_ACOS_KEY, str(target_acos))
            set_system_value(AUTO_AI_MAX_BID_KEY, str(max_bid))
            set_system_value(AUTO_AI_MIN_BID_KEY, str(min_bid))
            set_system_value(AUTO_AI_MIN_BID_CLOSE_KEY, str(min_close))
            set_system_value(AUTO_AI_MIN_BID_LOOSE_KEY, str(min_loose))
            set_system_value(AUTO_AI_MIN_BID_SUB_KEY, str(min_sub))
            set_system_value(AUTO_AI_MIN_BID_COMP_KEY, str(min_comp))
            set_system_value(AUTO_AI_MAX_UP_PCT_KEY, str(max_up_pct))
            set_system_value(AUTO_AI_BASELINE_MIN_KEY, str(baseline_min_bid))
            set_system_value(AUTO_AI_MEMORY_FLOOR_RATIO_KEY, str(memory_ratio))
            set_system_value(AUTO_AI_STOP_LOSS_KEY, str(stop_loss))
            set_system_value(AUTO_NEGATIVE_ENABLED_KEY, "1" if neg_enabled else "0")
            set_system_value(
                AUTO_NEGATIVE_LEVEL_KEY, "campaign" if neg_level == "活动级否词" else "adgroup"
            )
            set_system_value(
                AUTO_NEGATIVE_MATCH_KEY,
                "NEGATIVE_EXACT" if neg_match == "否定精准" else "NEGATIVE_PHRASE",
            )
            set_system_value(AUTO_NEGATIVE_DAYS_KEY, str(neg_days))
            set_system_value(AUTO_NEGATIVE_SPEND_KEY, str(neg_spend))
            set_system_value(AUTO_NEGATIVE_CLICKS_KEY, str(neg_clicks))
            set_system_value(AUTO_NEGATIVE_ACOS_MULT_KEY, str(neg_acos_mult))
            st.success("自动驾驶设置已保存")

    with c_act:
        st.markdown("#### 今日否词 / 否 ASIN")
        only_ai = st.checkbox("只看 AI", value=False, key="today_only_ai")
        today_str = datetime.now().strftime("%Y-%m-%d")

        campaign_map = _load_campaign_map()
        adgroup_map = _load_adgroup_map()

        neg_df = get_auto_negative_keywords()
        if not neg_df.empty:
            neg_df["created_at"] = neg_df.get("created_at", "").fillna("").astype(str)
            neg_df["last_updated"] = neg_df.get("last_updated", "").fillna("").astype(str)
            if only_ai:
                neg_df = neg_df[neg_df["source"].astype(str).str.upper() == "AI"]
            mask = neg_df["created_at"].str.startswith(today_str) | neg_df["last_updated"].str.startswith(today_str)
            neg_today = neg_df[mask].copy()
        else:
            neg_today = pd.DataFrame()

        if neg_today.empty:
            st.info("今日暂无否词")
        else:
            neg_today["活动名称"] = neg_today["campaign_id"].map(campaign_map).fillna("")
            neg_today["广告组名称"] = neg_today["ad_group_id"].map(adgroup_map).fillna("")
            neg_view = neg_today.rename(
                columns={
                    "keyword_text": "否词",
                    "match_type": "匹配",
                    "level": "层级",
                    "source": "来源",
                    "status": "状态",
                    "last_updated": "更新时间",
                }
            )
            st.dataframe(
                neg_view[
                    [
                        "活动名称",
                        "广告组名称",
                        "否词",
                        "匹配",
                        "层级",
                        "来源",
                        "状态",
                        "更新时间",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )

        prod_df = get_negative_product_targets()
        if not prod_df.empty:
            prod_df["created_at"] = prod_df.get("created_at", "").fillna("").astype(str)
            prod_df["last_updated"] = prod_df.get("last_updated", "").fillna("").astype(str)
            if only_ai:
                prod_df = prod_df[prod_df["source"].astype(str).str.upper() == "AI"]
            mask = prod_df["created_at"].str.startswith(today_str) | prod_df["last_updated"].str.startswith(today_str)
            prod_today = prod_df[mask].copy()
        else:
            prod_today = pd.DataFrame()

        if prod_today.empty:
            st.info("今日暂无否 ASIN")
        else:
            prod_today["活动名称"] = prod_today["campaign_id"].map(campaign_map).fillna("")
            prod_today["广告组名称"] = prod_today["ad_group_id"].map(adgroup_map).fillna("")
            prod_view = prod_today.rename(
                columns={
                    "asin": "ASIN",
                    "level": "层级",
                    "source": "来源",
                    "status": "状态",
                    "last_updated": "更新时间",
                }
            )
            st.dataframe(
                prod_view[
                    [
                        "活动名称",
                        "广告组名称",
                        "ASIN",
                        "层级",
                        "来源",
                        "状态",
                        "更新时间",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )

        st.caption("如需修改/撤回请到高级功能 → 否词管理")

        st.divider()
        st.markdown("#### 操作日志")
        conn = get_db_connection()
        try:
            logs_df = pd.read_sql("SELECT * FROM automation_logs ORDER BY timestamp DESC LIMIT 100", conn)
            if not logs_df.empty:
                display_df = logs_df.rename(
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
                st.dataframe(display_df, use_container_width=True, hide_index=True)
            else:
                st.info("暂无操作记录")
        except Exception:
            pass
        conn.close()

