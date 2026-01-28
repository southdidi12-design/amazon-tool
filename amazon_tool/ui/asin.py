import pandas as pd
import streamlit as st

from ..db import (
    get_budget_group_items,
    get_budget_groups,
    get_product_settings,
    save_budget_group,
    save_budget_group_items,
    save_product_settings,
)


def render_asin_tab(asin_df):
    st.subheader("ASIN 维度")

    settings_df = get_product_settings()
    if settings_df.empty:
        settings_df = pd.DataFrame(
            columns=[
                "asin",
                "sku",
                "daily_budget",
                "target_acos",
                "budget_flex",
                "is_star",
                "ai_enabled",
                "last_updated",
            ]
        )

    asin_options = []
    if not asin_df.empty:
        asin_options.extend([a for a in asin_df["asin"].dropna().astype(str).tolist() if a.strip()])
    if not settings_df.empty:
        asin_options.extend([a for a in settings_df["asin"].dropna().astype(str).tolist() if a.strip()])
    asin_options = sorted(set(asin_options))

    sku_map = {}
    if not asin_df.empty:
        for asin, group in asin_df.groupby("asin"):
            if pd.isna(asin) or not str(asin).strip():
                continue
            sku_list = [s for s in group["sku"].dropna().astype(str).tolist() if s.strip()]
            if sku_list:
                sku_map[str(asin).strip()] = sorted(set(sku_list))

    data_df = asin_df.copy()
    if not data_df.empty:
        data_df["asin"] = data_df["asin"].fillna("")
        data_df["sku"] = data_df["sku"].fillna("")
    else:
        data_df = pd.DataFrame(columns=["asin", "sku", "cost", "sales", "acos", "clicks", "impressions", "orders"])

    st.markdown("#### 🧮 预算池分配")
    st.caption("设置总日预算后按权重分配到 SKU，保存后自动写入每个 SKU 的日预算。")

    groups_df = get_budget_groups()
    group_names = []
    if not groups_df.empty:
        group_names = sorted([g for g in groups_df["group_name"].dropna().astype(str).tolist() if g.strip()])

    with st.expander("预算池设置", expanded=False):
        g1, g2 = st.columns([2, 1])
        with g1:
            if group_names:
                selected_group = st.selectbox("预算池", ["新建预算池"] + group_names)
                if selected_group == "新建预算池":
                    group_name = st.text_input("预算池名称", value="")
                else:
                    group_name = selected_group
            else:
                group_name = st.text_input("预算池名称", value="")

        with g2:
            default_total = 0.0
            if group_name and not groups_df.empty:
                match = groups_df[groups_df["group_name"].astype(str) == group_name]
                if not match.empty:
                    default_total = float(match.iloc[0].get("total_budget", 0) or 0)
            total_budget = st.number_input("总日预算", min_value=0.0, value=default_total)

        items_df = get_budget_group_items(group_name) if group_name else pd.DataFrame()
        base_items = pd.DataFrame(columns=["asin", "sku"])
        if not data_df.empty:
            base_items = data_df[["asin", "sku"]].copy()
        if not settings_df.empty:
            base_items = pd.concat([base_items, settings_df[["asin", "sku"]]], ignore_index=True)
        if not base_items.empty:
            base_items["asin"] = base_items["asin"].fillna("").astype(str).str.strip()
            base_items["sku"] = base_items["sku"].fillna("").astype(str).str.strip()
            base_items = base_items[base_items["asin"] != ""]
            base_items = base_items.drop_duplicates()

        if items_df.empty:
            items_df = base_items.copy()
            if not items_df.empty:
                items_df["weight"] = 1.0
            else:
                items_df = pd.DataFrame(columns=["asin", "sku", "weight"])
        else:
            items_df["asin"] = items_df["asin"].fillna("").astype(str).str.strip()
            items_df["sku"] = items_df["sku"].fillna("").astype(str).str.strip()
            if "weight" not in items_df.columns:
                items_df["weight"] = 0.0
            items_df["weight"] = pd.to_numeric(items_df["weight"], errors="coerce").fillna(0.0)
            if not base_items.empty:
                merged_items = base_items.merge(items_df[["asin", "sku"]], on=["asin", "sku"], how="left", indicator=True)
                missing = merged_items[merged_items["_merge"] == "left_only"][["asin", "sku"]]
                if not missing.empty:
                    missing = missing.copy()
                    missing["weight"] = 0.0
                    items_df = pd.concat([items_df, missing], ignore_index=True)

        edited_alloc = st.data_editor(
            items_df[["asin", "sku", "weight"]],
            num_rows="dynamic",
            column_config={
                "asin": st.column_config.TextColumn("ASIN"),
                "sku": st.column_config.TextColumn("SKU"),
                "weight": st.column_config.NumberColumn("权重", min_value=0.0),
            },
            use_container_width=True,
            hide_index=True,
        )

        total_weight = 0.0
        if not edited_alloc.empty and "weight" in edited_alloc.columns:
            total_weight = float(pd.to_numeric(edited_alloc["weight"], errors="coerce").fillna(0.0).sum())
        st.caption(f"当前权重合计: {total_weight:.2f}")

        if st.button("💾 保存预算池并分配日预算"):
            if not group_name:
                st.warning("请填写预算池名称")
            elif total_budget <= 0:
                st.warning("请设置总日预算")
            elif edited_alloc.empty:
                st.warning("请添加要分配的 SKU")
            elif total_weight <= 0:
                st.warning("权重合计需要大于 0")
            else:
                alloc = edited_alloc.copy()
                alloc["asin"] = alloc["asin"].fillna("").astype(str).str.strip()
                alloc["sku"] = alloc["sku"].fillna("").astype(str).str.strip()
                alloc["weight"] = pd.to_numeric(alloc["weight"], errors="coerce").fillna(0.0)
                alloc = alloc[alloc["asin"] != ""]
                if alloc.empty:
                    st.warning("请至少保留一个 ASIN/SKU")
                else:
                    alloc["daily_budget"] = (alloc["weight"] / alloc["weight"].sum()) * total_budget
                    base_settings = settings_df.copy()
                    base_settings["asin"] = base_settings["asin"].fillna("").astype(str)
                    base_settings["sku"] = base_settings["sku"].fillna("").astype(str)
                    save_df = pd.merge(
                        alloc,
                        base_settings[["asin", "sku", "target_acos", "budget_flex", "is_star", "ai_enabled"]],
                        on=["asin", "sku"],
                        how="left",
                    )
                    save_df["target_acos"] = save_df["target_acos"].fillna(0.0)
                    save_df["budget_flex"] = save_df["budget_flex"].fillna(0.0)
                    save_df["is_star"] = save_df["is_star"].fillna(False)
                    save_df["ai_enabled"] = save_df["ai_enabled"].fillna(True)
                    save_product_settings(
                        save_df[
                            ["asin", "sku", "daily_budget", "target_acos", "budget_flex", "is_star", "ai_enabled"]
                        ]
                    )
                    save_budget_group(group_name, total_budget)
                    save_budget_group_items(group_name, alloc[["asin", "sku", "weight"]])
                    st.success("预算池已保存，并更新 SKU 日预算")
                    st.rerun()

    st.markdown("#### 🧩 ASIN 设置")
    st.caption("先选 ASIN/SKU，再填写日预算与目标 ACOS。AI 会按此规则调整 SP 广告组默认出价。")
    with st.expander("添加/更新 ASIN 设置", expanded=True):
        c1, c2 = st.columns([2, 2])
        with c1:
            if asin_options:
                selected_asin = st.selectbox("ASIN", asin_options)
                manual_asin = st.text_input("或手动输入 ASIN", value="")
                asin_value = manual_asin.strip() if manual_asin.strip() else selected_asin
            else:
                asin_value = st.text_input("ASIN", value="").strip()

            sku_candidates = sku_map.get(asin_value, [])
            if sku_candidates:
                selected_sku = st.selectbox("SKU", [""] + sku_candidates)
                manual_sku = st.text_input("或手动输入 SKU", value="")
                sku_value = manual_sku.strip() if manual_sku.strip() else selected_sku
            else:
                sku_value = st.text_input("SKU", value="").strip()

        with c2:
            defaults = settings_df[
                (settings_df["asin"].astype(str) == asin_value)
                & (settings_df["sku"].astype(str) == sku_value)
            ]
            if not defaults.empty:
                defaults_row = defaults.iloc[0]
                default_budget = float(defaults_row.get("daily_budget", 0) or 0)
                default_acos = float(defaults_row.get("target_acos", 0) or 0)
                default_flex = float(defaults_row.get("budget_flex", 0) or 0)
                default_star = bool(defaults_row.get("is_star", False))
                default_ai = bool(defaults_row.get("ai_enabled", True))
            else:
                default_budget = 0.0
                default_acos = 0.0
                default_flex = 0.0
                default_star = False
                default_ai = True

            daily_budget = st.number_input("日预算", min_value=0.0, value=default_budget)
            target_acos = st.number_input("目标 ACOS (%)", min_value=0.0, value=default_acos)
            budget_flex = st.number_input("超额比例 (%)", min_value=0.0, max_value=100.0, value=default_flex)
            is_star = st.checkbox("主推", value=default_star)
            ai_enabled = st.checkbox("AI 启用", value=default_ai)

        if st.button("✅ 保存此 ASIN 设置", type="primary"):
            if not asin_value:
                st.warning("请先填写 ASIN")
            else:
                save_df = pd.DataFrame(
                    [
                        {
                            "asin": asin_value,
                            "sku": sku_value,
                            "daily_budget": daily_budget,
                            "target_acos": target_acos,
                            "budget_flex": budget_flex,
                            "is_star": is_star,
                            "ai_enabled": ai_enabled,
                        }
                    ]
                )
                save_product_settings(save_df)
                st.success("ASIN 设置已保存")

    merged = pd.merge(data_df, settings_df, on=["asin", "sku"], how="outer")
    merged["daily_budget"] = merged["daily_budget"].fillna(0.0)
    merged["target_acos"] = merged["target_acos"].fillna(0.0)
    merged["budget_flex"] = merged["budget_flex"].fillna(0.0)
    merged["is_star"] = merged["is_star"].fillna(0).astype(bool)
    merged["ai_enabled"] = merged["ai_enabled"].fillna(1).astype(bool)
    for col in ["cost", "sales", "acos", "clicks", "impressions", "orders"]:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0)

    st.markdown("#### 📋 已配置列表")
    filter_asins = st.multiselect("仅显示这些 ASIN", asin_options, default=asin_options[:10] if asin_options else [])
    display_settings = merged.copy()
    if filter_asins:
        display_settings = display_settings[display_settings["asin"].isin(filter_asins)]
    edited = st.data_editor(
        display_settings[
            [
                "ai_enabled",
                "is_star",
                "daily_budget",
                "target_acos",
                "budget_flex",
                "asin",
                "sku",
                "cost",
                "sales",
                "acos",
                "clicks",
                "impressions",
                "orders",
            ]
        ],
        num_rows="dynamic",
        column_config={
            "ai_enabled": st.column_config.CheckboxColumn("AI启用", width="small"),
            "is_star": st.column_config.CheckboxColumn("主推", width="small"),
            "daily_budget": st.column_config.NumberColumn("日预算", min_value=0.0),
            "target_acos": st.column_config.NumberColumn("目标ACOS(%)", min_value=0.0),
            "budget_flex": st.column_config.NumberColumn("超额(%)", min_value=0.0, max_value=100.0),
            "asin": st.column_config.TextColumn("ASIN"),
            "sku": st.column_config.TextColumn("SKU"),
            "cost": st.column_config.NumberColumn("花费", disabled=True),
            "sales": st.column_config.NumberColumn("销售额", disabled=True),
            "acos": st.column_config.NumberColumn("ACOS", disabled=True),
            "clicks": st.column_config.NumberColumn("点击", disabled=True),
            "impressions": st.column_config.NumberColumn("曝光", disabled=True),
            "orders": st.column_config.NumberColumn("订单", disabled=True),
        },
        use_container_width=True,
        hide_index=True,
    )
    if st.button("💾 保存已配置列表", type="secondary"):
        save_df = edited[["asin", "sku", "daily_budget", "target_acos", "budget_flex", "is_star", "ai_enabled"]].copy()
        save_product_settings(save_df)
        st.success("ASIN 设置已保存")

    if asin_df.empty:
        st.info("暂无 ASIN 维度数据")
        return
    asin_sorted = asin_df.sort_values("cost", ascending=False)
    c_asin_bar, c_asin_table = st.columns([1, 2])
    with c_asin_bar:
        st.subheader("花费 Top 10")
        top_spend_asin = (
            asin_sorted.groupby("asin", as_index=False)["cost"]
            .sum()
            .sort_values("cost", ascending=False)
            .head(10)
        )
        st.bar_chart(top_spend_asin.set_index("asin")["cost"], color="#FF4B4B", height=300)
    with c_asin_table:
        st.subheader("ASIN 详情")
        display_asin = asin_sorted.rename(
            columns={
                "asin": "ASIN",
                "sku": "SKU",
                "cost": "花费",
                "sales": "销售额",
                "acos": "ACOS",
                "clicks": "点击",
                "impressions": "曝光",
                "orders": "订单",
                "cpc": "CPC",
                "cr": "转化率",
            }
        )
        st.dataframe(display_asin, use_container_width=True)
