import streamlit as st


def render_product_ads_tab(product_ads_df):
    st.subheader("商品广告")
    if product_ads_df.empty:
        st.info("暂无商品广告数据")
        return
    display_df = product_ads_df.rename(
        columns={
            "ad_id": "广告ID",
            "asin": "ASIN",
            "sku": "SKU",
            "state": "状态",
            "serving_status": "投放状态",
            "campaign_name": "广告活动",
            "campaign_id": "活动ID",
            "ad_group_id": "广告组ID",
            "creation_date": "创建时间",
            "last_update_date": "更新时间",
            "last_synced": "同步时间",
        }
    )
    states = sorted([s for s in display_df["状态"].unique() if s])
    sel_states = st.multiselect("状态筛选", states, default=states)
    if sel_states:
        display_df = display_df[display_df["状态"].isin(sel_states)]
    st.dataframe(display_df, use_container_width=True, hide_index=True)
