import time
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from ..db import db_write_lock, get_db_connection


def _load_recent_perf(days=7):
    end_date = date.today().strftime("%Y-%m-%d")
    start_date = (date.today() - timedelta(days=days)).strftime("%Y-%m-%d")
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT campaign_id, COALESCE(ad_type, 'SP') AS ad_type,
                   SUM(cost) AS cost, SUM(sales) AS sales,
                   SUM(clicks) AS clicks, SUM(impressions) AS impressions, SUM(orders) AS orders
            FROM campaign_reports
            WHERE date >= ? AND date <= ?
            GROUP BY campaign_id, ad_type
            """,
            conn,
            params=(start_date, end_date),
        )
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()
    return df


def _compact_group_labels(labels, limit=3):
    items = [str(x).strip() for x in labels if str(x).strip()]
    if not items:
        return ""
    seen = set()
    uniq = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        uniq.append(item)
    if len(uniq) <= limit:
        return " / ".join(uniq)
    return " / ".join(uniq[:limit]) + f" ...(+{len(uniq) - limit})"


def render_manage_tab(perf_df=None):
    st.subheader("操作中心")
    st.caption("快速总览 + 异常提示 + 主推活动")

    conn = get_db_connection()
    try:
        settings_df = pd.read_sql_query("SELECT * FROM campaign_settings", conn)
        ad_groups_df = pd.read_sql_query(
            "SELECT ad_group_id, ad_group_name, campaign_id FROM ad_group_settings",
            conn,
        )
    except Exception:
        settings_df = pd.DataFrame()
        ad_groups_df = pd.DataFrame()
    finally:
        conn.close()

    if settings_df.empty:
        st.info("暂无广告活动配置，请先同步广告配置。")
        return

    # 兼容旧表结构或缺字段
    required_defaults = {
        "campaign_id": "",
        "campaign_name": "",
        "ad_type": "SP",
        "current_status": "",
        "current_budget": 0,
        "is_star": 0,
    }
    for col, default in required_defaults.items():
        if col not in settings_df.columns:
            settings_df[col] = default

    if perf_df is None or perf_df.empty:
        perf_df = _load_recent_perf(7)

    if not perf_df.empty:
        merged = pd.merge(settings_df, perf_df, on=["campaign_id", "ad_type"], how="left")
    else:
        merged = settings_df.copy()
        merged["cost"] = 0
        merged["sales"] = 0
        merged["clicks"] = 0
        merged["impressions"] = 0
        merged["orders"] = 0

    if not ad_groups_df.empty:
        ad_groups_df["campaign_id"] = ad_groups_df["campaign_id"].fillna("").astype(str)
        ad_groups_df["ad_group_id"] = ad_groups_df["ad_group_id"].fillna("").astype(str)
        ad_groups_df["ad_group_name"] = ad_groups_df["ad_group_name"].fillna("").astype(str)
        ad_groups_df["ad_group_label"] = ad_groups_df.apply(
            lambda r: f"{r['ad_group_name']}({r['ad_group_id']})" if r["ad_group_name"] else r["ad_group_id"],
            axis=1,
        )
        ad_group_summary = (
            ad_groups_df.groupby("campaign_id", as_index=False)
            .agg(
                ad_group_count=("ad_group_id", "nunique"),
                ad_group_list=("ad_group_label", _compact_group_labels),
            )
            .reset_index(drop=True)
        )
        merged = pd.merge(merged, ad_group_summary, on="campaign_id", how="left")
    else:
        merged["ad_group_count"] = 0
        merged["ad_group_list"] = ""

    required_cols = {
        "campaign_id": "",
        "campaign_name": "",
        "ad_type": "SP",
        "current_status": "",
        "current_budget": 0,
        "is_star": 0,
        "cost": 0,
        "sales": 0,
        "clicks": 0,
        "impressions": 0,
        "orders": 0,
        "ad_group_count": 0,
        "ad_group_list": "",
    }
    for col, default in required_cols.items():
        if col not in merged.columns:
            merged[col] = default

    for col in ["cost", "sales", "clicks", "impressions", "orders", "current_budget"]:
        merged[col] = merged[col].fillna(0)

    merged["acos"] = merged.apply(lambda x: x["cost"] / x["sales"] if x["sales"] > 0 else 0, axis=1)
    merged["is_star"] = merged["is_star"].fillna(0).astype(bool)
    merged["current_status"] = merged["current_status"].fillna("")
    merged["ad_group_count"] = merged["ad_group_count"].fillna(0).astype(int)
    merged["ad_group_list"] = merged["ad_group_list"].fillna("")

    # 概览
    total_campaigns = len(merged)
    active_count = merged[merged["current_status"].str.upper().isin(["ENABLED", "ENABLED_WITH_PENDING_CHANGES"])].shape[0]
    spend_total = float(merged["cost"].sum())
    high_acos_count = merged[(merged["sales"] > 0) & (merged["acos"] >= 0.3)].shape[0]
    no_sales_count = merged[(merged["sales"] <= 0) & (merged["cost"] >= 5)].shape[0]

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("活动总数", total_campaigns)
    k2.metric("启用活动", active_count)
    k3.metric("近7日花费", f"${spend_total:.2f}")
    k4.metric("异常数量", high_acos_count + no_sales_count)

    st.divider()
    st.markdown("#### 异常提示 · 高 ACOS / 无转化")
    anomalies = merged[
        ((merged["sales"] <= 0) & (merged["cost"] >= 5)) | ((merged["sales"] > 0) & (merged["acos"] >= 0.3))
    ].copy()
    anomalies = anomalies.sort_values("cost", ascending=False).head(20)
    if anomalies.empty:
        st.info("暂无异常")
    else:
        display_anom = anomalies.rename(
            columns={
                "campaign_name": "活动名称",
                "campaign_id": "活动ID",
                "ad_type": "类型",
                "current_status": "状态",
                "ad_group_count": "广告组数",
                "ad_group_list": "广告组",
                "cost": "花费",
                "sales": "销售额",
                "acos": "ACOS",
                "clicks": "点击",
                "orders": "订单",
            }
        )
        st.dataframe(
            display_anom[
                ["活动名称", "活动ID", "类型", "状态", "广告组数", "广告组", "花费", "销售额", "ACOS", "点击", "订单"]
            ],
            use_container_width=True,
            hide_index=True,
        )

    st.divider()
    st.markdown("#### 主推活动")
    st.caption("勾选主推活动，用于自动驾驶优先优化。")
    edited = st.data_editor(
        merged[
            [
                "is_star",
                "campaign_name",
                "ad_type",
                "current_status",
                "current_budget",
                "ad_group_count",
                "ad_group_list",
                "campaign_id",
            ]
        ].rename(
            columns={
                "is_star": "主推",
                "campaign_name": "活动名称",
                "ad_type": "类型",
                "current_status": "状态",
                "current_budget": "预算",
                "ad_group_count": "广告组数",
                "ad_group_list": "广告组",
                "campaign_id": "活动ID",
            }
        ),
        column_config={
            "主推": st.column_config.CheckboxColumn("主推", width="small"),
            "类型": st.column_config.TextColumn("类型", disabled=True, width="small"),
            "活动名称": st.column_config.TextColumn("活动名称", disabled=True),
            "状态": st.column_config.TextColumn("状态", disabled=True),
            "预算": st.column_config.NumberColumn("预算", disabled=True),
            "广告组数": st.column_config.NumberColumn("广告组数", disabled=True, width="small"),
            "广告组": st.column_config.TextColumn("广告组", disabled=True),
            "活动ID": st.column_config.TextColumn("活动ID", disabled=True),
        },
        use_container_width=True,
        height=420,
        hide_index=True,
    )

    if st.button("保存主推设置", type="primary"):
        with db_write_lock():
            conn = get_db_connection()
            for _, r in edited.iterrows():
                val = 1 if r["主推"] else 0
                conn.execute(
                    "UPDATE campaign_settings SET is_star=? WHERE campaign_id=? AND ad_type=?",
                    (val, r["活动ID"], r["类型"]),
                )
            conn.commit()
            conn.close()
        st.success("主推设置已保存")
        time.sleep(1)
        st.rerun()
