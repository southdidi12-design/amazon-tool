import streamlit as st


def render_dashboard_tab(df, trend):
    if df.empty:
        st.info("暂无 Campaign 维度数据")
        return
    c_trend, c_bar = st.columns([2, 1])
    with c_trend:
        st.subheader("📈 业绩走势")
        if not trend.empty:
            display_trend = trend.rename(columns={"cost": "花费", "sales": "销售额"})
            st.line_chart(display_trend, color=["#FF4B4B", "#0068C9"], height=300)
    with c_bar:
        st.subheader("💸 花费 Top 5")
        top_spend = df.sort_values("cost", ascending=False).head(5)
        st.bar_chart(top_spend.set_index("campaign_name")["cost"], color="#FF4B4B", height=300)

    st.subheader("📋 详细数据表")
    display_df = df.rename(
        columns={
            "campaign_id": "活动ID",
            "campaign_name": "活动名称",
            "ad_type": "类型",
            "budget_type": "预算类型",
            "current_budget": "当前预算",
            "current_status": "状态",
            "is_star": "主推",
            "cost": "花费",
            "sales": "销售额",
            "clicks": "点击",
            "impressions": "曝光",
            "orders": "订单",
            "cpc": "CPC",
            "acos": "ACOS",
            "cr": "转化率",
        }
    )
    st.dataframe(display_df, use_container_width=True)
