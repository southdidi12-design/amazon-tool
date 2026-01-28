import pandas as pd
import streamlit as st

from ..amazon_api import (
    create_sp_negative_keywords,
    create_sp_negative_targets,
    archive_sp_negative_targets,
    delete_sp_negative_keywords,
    get_amazon_session_and_headers,
    list_sp_campaign_negative_keywords,
    list_sp_negative_keywords,
    list_sp_negative_targets,
)
from ..db import (
    get_auto_negative_keywords,
    get_db_connection,
    get_negative_product_targets,
    save_auto_negative_keywords,
    save_negative_product_targets,
    update_auto_negative_status,
    update_negative_product_status,
)


def _normalize_match_type(match_type):
    if not match_type:
        return ""
    mt = str(match_type).strip()
    lower = mt.lower()
    if lower in ["negativeexact", "negative_exact", "negative exact"]:
        return "NEGATIVE_EXACT"
    if lower in ["negativephrase", "negative_phrase", "negative phrase"]:
        return "NEGATIVE_PHRASE"
    if lower == "exact":
        return "NEGATIVE_EXACT"
    if lower == "phrase":
        return "NEGATIVE_PHRASE"
    if mt in ["NEGATIVE_EXACT", "NEGATIVE_PHRASE"]:
        return mt
    if mt in ["negativeExact", "negativePhrase"]:
        return "NEGATIVE_EXACT" if mt == "negativeExact" else "NEGATIVE_PHRASE"
    return mt


def _load_campaigns():
    conn = get_db_connection()
    try:
        df = pd.read_sql(
            "SELECT campaign_id, campaign_name, ad_type FROM campaign_settings WHERE ad_type='SP'",
            conn,
        )
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()
    df["campaign_id"] = df["campaign_id"].fillna("").astype(str)
    df["campaign_name"] = df["campaign_name"].fillna("")
    return df


def _load_adgroups(campaign_id):
    if not campaign_id:
        return pd.DataFrame()
    conn = get_db_connection()
    try:
        df = pd.read_sql(
            "SELECT ad_group_id, ad_group_name FROM ad_group_settings WHERE campaign_id=?",
            conn,
            params=(campaign_id,),
        )
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()
    df["ad_group_id"] = df["ad_group_id"].fillna("").astype(str)
    df["ad_group_name"] = df["ad_group_name"].fillna("")
    return df


def render_negative_keywords_tab():
    st.subheader("否词管理 (SP)")

    session, headers = get_amazon_session_and_headers()
    if not session:
        st.warning("未检测到 Amazon API 配置，无法同步/提交否词。")
        return

    campaigns_df = _load_campaigns()
    if campaigns_df.empty:
        st.info("暂无 SP 活动，请先同步广告设置。")
        return

    level = st.radio("否词层级", ["活动级否词", "广告组否词"], horizontal=True)
    campaign_options = [
        f"{row['campaign_name']} ({row['campaign_id']})" for _, row in campaigns_df.iterrows()
    ]
    selected_campaign = st.selectbox("选择活动", campaign_options)
    campaign_id = ""
    if selected_campaign:
        campaign_id = selected_campaign.split("(")[-1].rstrip(")")

    ad_group_id = ""
    if level == "广告组否词":
        adgroups_df = _load_adgroups(campaign_id)
        if adgroups_df.empty:
            st.warning("该活动暂无广告组，请先同步广告组。")
            return
        adgroup_options = [
            f"{row['ad_group_name']} ({row['ad_group_id']})" for _, row in adgroups_df.iterrows()
        ]
        selected_adgroup = st.selectbox("选择广告组", adgroup_options)
        if selected_adgroup:
            ad_group_id = selected_adgroup.split("(")[-1].rstrip(")")

    match_type = st.selectbox("匹配方式", ["否定词组", "否定精准"])
    match_value = "NEGATIVE_PHRASE" if match_type == "否定词组" else "NEGATIVE_EXACT"
    keyword_text = st.text_area("否词列表（每行一个）", height=160, placeholder="例如：\n免费\n二手\n配件")

    if st.button("🚫 添加否词", type="primary"):
        entries = [line.strip() for line in keyword_text.splitlines() if line.strip()]
        if not campaign_id:
            st.warning("请先选择活动")
        elif level == "广告组否词" and not ad_group_id:
            st.warning("请先选择广告组")
        elif not entries:
            st.warning("请填写至少一个否词")
        else:
            payloads = []
            records = []
            for word in entries:
                item = {
                    "campaignId": str(campaign_id),
                    "keywordText": word,
                    "matchType": match_value,
                    "state": "ENABLED",
                }
                if level == "广告组否词":
                    item["adGroupId"] = str(ad_group_id)
                payloads.append(item)
                records.append(
                    {
                        "campaign_id": str(campaign_id),
                        "ad_group_id": str(ad_group_id) if level == "广告组否词" else "",
                        "keyword_text": word,
                        "match_type": match_value,
                        "level": "adgroup" if level == "广告组否词" else "campaign",
                        "source": "manual",
                        "status": "pending",
                    }
                )
            ok, resp = create_sp_negative_keywords(
                session, headers, payloads, campaign_level=(level == "活动级否词")
            )
            if ok:
                for r in records:
                    r["status"] = "created"
                save_auto_negative_keywords(records)
                st.success("否词已提交")
            else:
                for r in records:
                    r["status"] = "failed"
                save_auto_negative_keywords(records)
                st.error(f"提交失败：{resp}")

    st.divider()
    st.markdown("#### AI 否词记录")
    ai_df = get_auto_negative_keywords(source="AI")
    if ai_df.empty:
        st.info("暂无 AI 否词记录")
    else:
        ai_view = ai_df.copy()
        ai_view["campaign_id"] = ai_view["campaign_id"].fillna("").astype(str)
        ai_view["ad_group_id"] = ai_view["ad_group_id"].fillna("").astype(str)
        if campaign_id:
            ai_view = ai_view[ai_view["campaign_id"] == str(campaign_id)]
        if level == "广告组否词" and ad_group_id:
            ai_view = ai_view[ai_view["ad_group_id"] == str(ad_group_id)]

        if not ai_view.empty:
            ai_view = ai_view.merge(
                campaigns_df[["campaign_id", "campaign_name"]],
                on="campaign_id",
                how="left",
            )
            if level == "广告组否词":
                adgroups_df = _load_adgroups(campaign_id)
                ai_view = ai_view.merge(
                    adgroups_df[["ad_group_id", "ad_group_name"]],
                    on="ad_group_id",
                    how="left",
                )
            ai_view["level"] = ai_view["level"].apply(lambda x: "活动级" if str(x) == "campaign" else "广告组级")
            ai_display = ai_view.rename(
                columns={
                    "campaign_name": "活动名称",
                    "campaign_id": "活动ID",
                    "ad_group_name": "广告组名称",
                    "ad_group_id": "广告组ID",
                    "keyword_text": "否词",
                    "match_type": "匹配方式",
                    "level": "层级",
                    "status": "状态",
                    "created_at": "创建时间",
                    "last_updated": "最近更新",
                }
            )
            st.dataframe(
                ai_display[
                    [
                        c
                        for c in [
                            "活动名称",
                            "活动ID",
                            "广告组名称",
                            "广告组ID",
                            "否词",
                            "匹配方式",
                            "层级",
                            "状态",
                            "创建时间",
                            "最近更新",
                        ]
                        if c in ai_display.columns
                    ]
                ],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("当前筛选条件下暂无 AI 否词记录")

    st.divider()
    st.markdown("#### 已有否词")
    if st.button("🔄 刷新列表"):
        st.rerun()

    if level == "活动级否词":
        items = list_sp_campaign_negative_keywords(session, headers)
    else:
        items = list_sp_negative_keywords(session, headers)

    if not items:
        st.info("暂无否词数据")
        return

    data = pd.DataFrame(items)
    data["campaignId"] = data.get("campaignId", "")
    data["adGroupId"] = data.get("adGroupId", "")
    data["keywordText"] = data.get("keywordText", "")
    data["matchType"] = data.get("matchType", "")
    data["state"] = data.get("state", "")
    data["campaignId"] = data["campaignId"].fillna("").astype(str)
    data["adGroupId"] = data["adGroupId"].fillna("").astype(str)
    data["keywordText"] = data["keywordText"].fillna("").astype(str)
    data["matchType"] = data["matchType"].fillna("").astype(str)
    if "keywordId" not in data.columns:
        data["keywordId"] = ""
    if "campaignNegativeKeywordId" not in data.columns:
        data["campaignNegativeKeywordId"] = ""

    # 关联活动/广告组名称
    data = data.merge(
        campaigns_df[["campaign_id", "campaign_name"]],
        left_on="campaignId",
        right_on="campaign_id",
        how="left",
    )
    if level == "广告组否词":
        adgroups_df = _load_adgroups(campaign_id)
        data = data.merge(
            adgroups_df[["ad_group_id", "ad_group_name"]],
            left_on="adGroupId",
            right_on="ad_group_id",
            how="left",
        )

    # 标记 AI 否词
    ai_df = get_auto_negative_keywords(source="AI")
    ai_map = {}
    if not ai_df.empty:
        for _, r in ai_df.iterrows():
            key = (
                str(r.get("campaign_id", "") or ""),
                str(r.get("ad_group_id", "") or ""),
                str(r.get("keyword_text", "") or "").strip().lower(),
                _normalize_match_type(r.get("match_type")),
                str(r.get("level", "") or ""),
            )
            ai_map[key] = {
                "created_at": r.get("created_at", ""),
                "status": r.get("status", ""),
            }

    level_key = "campaign" if level == "活动级否词" else "adgroup"
    sources = []
    ai_created = []
    ai_status = []
    for _, row in data.iterrows():
        key = (
            str(row.get("campaignId", "") or ""),
            str(row.get("adGroupId", "") or ""),
            str(row.get("keywordText", "") or "").strip().lower(),
            _normalize_match_type(row.get("matchType")),
            level_key,
        )
        info = ai_map.get(key)
        if info:
            sources.append("AI")
            ai_created.append(info.get("created_at", ""))
            ai_status.append(info.get("status", ""))
        else:
            sources.append("手动/未知")
            ai_created.append("")
            ai_status.append("")
    data["ai_source"] = sources
    data["ai_created_at"] = ai_created
    data["ai_status"] = ai_status

    only_ai = st.checkbox("只看 AI 否词", value=False)
    if only_ai:
        data = data[data["ai_source"] == "AI"]

    show_cols = ["campaign_name", "campaignId", "keywordText", "matchType", "state", "ai_source", "ai_status", "ai_created_at"]
    if level == "广告组否词":
        show_cols.insert(1, "ad_group_name")
        show_cols.insert(2, "adGroupId")

    display = data.copy()
    display = display.rename(
        columns={
            "campaign_name": "活动名称",
            "campaignId": "活动ID",
            "ad_group_name": "广告组名称",
            "adGroupId": "广告组ID",
            "keywordText": "否词",
            "matchType": "匹配方式",
            "state": "状态",
            "ai_source": "来源",
            "ai_status": "AI状态",
            "ai_created_at": "AI时间",
        }
    )

    display = display.reset_index(drop=True)
    display["选择"] = False
    display["__row_id"] = display.index
    col_order = ["选择"] + [c for c in display.columns if c not in ["选择", "__row_id"]]
    edited = st.data_editor(
        display[["__row_id"] + col_order],
        use_container_width=True,
        hide_index=True,
        column_config={
            "选择": st.column_config.CheckboxColumn("选择", width="small"),
            "__row_id": None,
        },
        disabled=["__row_id"],
    )

    if st.button("🗑 删除所选否词"):
        selected = edited[edited["选择"]]
        if selected.empty:
            st.warning("请先选择要删除的否词")
        else:
            ids = []
            status_rows = []
            for _, row in selected.iterrows():
                row_id = row.get("__row_id")
                if row_id is None:
                    continue
                try:
                    row_id = int(row_id)
                except Exception:
                    continue
                if row_id < 0 or row_id >= len(data):
                    continue
                m = data.iloc[row_id]
                keyword_id = m.get("keywordId") or m.get("campaignNegativeKeywordId")
                if keyword_id:
                    ids.append(keyword_id)
                source = "AI" if str(m.get("ai_source", "")) == "AI" else "manual"
                status_rows.append(
                    {
                        "campaign_id": str(m.get("campaignId", "") or ""),
                        "ad_group_id": str(m.get("adGroupId", "") or ""),
                        "keyword_text": str(m.get("keywordText", "") or ""),
                        "match_type": _normalize_match_type(m.get("matchType")),
                        "level": "campaign" if level == "活动级否词" else "adgroup",
                        "source": source,
                    }
                )
            ok, resp = delete_sp_negative_keywords(
                session, headers, ids, campaign_level=(level == "活动级否词")
            )
            if ok:
                if status_rows:
                    update_auto_negative_status(status_rows, "deleted")
                st.success("删除请求已提交")
                st.rerun()
            else:
                st.error(f"删除失败：{resp}")

    st.markdown("#### 修改选中否词")
    st.caption("可批量修改：新否词为空则保持原词，匹配方式可统一调整。")
    new_text = st.text_input("新否词（留空不改）", value="")
    new_match_choice = st.selectbox("新匹配方式", ["保持不变", "否定词组", "否定精准"])
    if st.button("✏️ 更新选中否词"):
        selected = edited[edited["选择"]]
        if selected.empty:
            st.warning("请先选择要修改的否词")
        elif not new_text.strip() and new_match_choice == "保持不变":
            st.warning("没有修改内容，如需删除请用上面的删除按钮")
        else:
            ids = []
            rows = []
            status_rows = []
            for _, row in selected.iterrows():
                row_id = row.get("__row_id")
                if row_id is None:
                    continue
                try:
                    row_id = int(row_id)
                except Exception:
                    continue
                if row_id < 0 or row_id >= len(data):
                    continue
                m = data.iloc[row_id]
                rows.append(m)
                keyword_id = m.get("keywordId") or m.get("campaignNegativeKeywordId")
                if keyword_id:
                    ids.append(keyword_id)
                source = "AI" if str(m.get("ai_source", "")) == "AI" else "manual"
                status_rows.append(
                    {
                        "campaign_id": str(m.get("campaignId", "") or ""),
                        "ad_group_id": str(m.get("adGroupId", "") or ""),
                        "keyword_text": str(m.get("keywordText", "") or ""),
                        "match_type": _normalize_match_type(m.get("matchType")),
                        "level": "campaign" if level == "活动级否词" else "adgroup",
                        "source": source,
                    }
                )
            if not ids:
                st.warning("所选否词缺少ID，无法修改")
            else:
                ok, resp = delete_sp_negative_keywords(
                    session, headers, ids, campaign_level=(level == "活动级否词")
                )
                if not ok:
                    st.error(f"删除失败：{resp}")
                else:
                    if status_rows:
                        update_auto_negative_status(status_rows, "edited")
                    payloads = []
                    for m in rows:
                        word = new_text.strip() or str(m.get("keywordText", "") or "")
                        if not word:
                            continue
                        if new_match_choice == "保持不变":
                            match_val = _normalize_match_type(m.get("matchType")) or "NEGATIVE_EXACT"
                        else:
                            match_val = "NEGATIVE_PHRASE" if new_match_choice == "否定词组" else "NEGATIVE_EXACT"
                        item = {
                            "campaignId": str(m.get("campaignId", "") or ""),
                            "keywordText": word,
                            "matchType": match_val,
                            "state": "ENABLED",
                        }
                        if level == "广告组否词":
                            item["adGroupId"] = str(m.get("adGroupId", "") or "")
                        payloads.append(item)
                    if payloads:
                        ok2, resp2 = create_sp_negative_keywords(
                            session, headers, payloads, campaign_level=(level == "活动级否词")
                        )
                        if ok2:
                            st.success("修改请求已提交")
                            st.rerun()
                        else:
                            st.error(f"修改失败：{resp2}")
                    else:
                        st.success("修改已完成（无新增项）")
                        st.rerun()

    st.divider()
    st.subheader("否掉商品 / ASIN (SP)")
    st.caption("用于屏蔽不想投放的商品页面，减少无效消耗。")

    prod_level = st.radio(
        "商品否投层级",
        ["活动级否投", "广告组否投"],
        horizontal=True,
        key="neg_prod_level",
    )
    prod_campaign_options = [
        f"{row['campaign_name']} ({row['campaign_id']})" for _, row in campaigns_df.iterrows()
    ]
    prod_selected_campaign = st.selectbox("选择活动（商品否投）", prod_campaign_options, key="neg_prod_campaign")
    prod_campaign_id = ""
    if prod_selected_campaign:
        prod_campaign_id = prod_selected_campaign.split("(")[-1].rstrip(")")

    prod_ad_group_id = ""
    if prod_level == "广告组否投":
        prod_adgroups_df = _load_adgroups(prod_campaign_id)
        if prod_adgroups_df.empty:
            st.warning("该活动暂无广告组，请先同步广告组。")
        else:
            prod_adgroup_options = [
                f"{row['ad_group_name']} ({row['ad_group_id']})" for _, row in prod_adgroups_df.iterrows()
            ]
            prod_selected_adgroup = st.selectbox("选择广告组（商品否投）", prod_adgroup_options, key="neg_prod_adgroup")
            if prod_selected_adgroup:
                prod_ad_group_id = prod_selected_adgroup.split("(")[-1].rstrip(")")

    prod_asins = st.text_area(
        "要否投的 ASIN（每行一个）",
        height=120,
        placeholder="例如：\nB0XXXXXXX1\nB0XXXXXXX2",
        key="neg_prod_asins",
    )
    if st.button("🚫 添加否投商品", key="neg_prod_add", type="primary"):
        entries = [line.strip() for line in prod_asins.splitlines() if line.strip()]
        if not prod_campaign_id:
            st.warning("请先选择活动")
        elif prod_level == "广告组否投" and not prod_ad_group_id:
            st.warning("请先选择广告组")
        elif not entries:
            st.warning("请至少填写一个 ASIN")
        else:
            payloads = []
            records = []
            for asin in entries:
                item = {
                    "campaignId": str(prod_campaign_id),
                    "state": "ENABLED",
                    "expressionType": "MANUAL",
                    "expression": [{"type": "ASIN_SAME_AS", "value": asin}],
                }
                if prod_level == "广告组否投":
                    item["adGroupId"] = str(prod_ad_group_id)
                payloads.append(item)
                records.append(
                    {
                        "campaign_id": str(prod_campaign_id),
                        "ad_group_id": str(prod_ad_group_id) if prod_level == "广告组否投" else "",
                        "asin": asin,
                        "expression_type": "MANUAL",
                        "level": "adgroup" if prod_level == "广告组否投" else "campaign",
                        "source": "manual",
                        "status": "pending",
                    }
                )
            ok, resp = create_sp_negative_targets(session, headers, payloads)
            if ok:
                for r in records:
                    r["status"] = "created"
                save_negative_product_targets(records)
                st.success("商品否投已提交")
            else:
                for r in records:
                    r["status"] = "failed"
                save_negative_product_targets(records)
                st.error(f"提交失败：{resp}")

    st.markdown("#### 商品否投记录")
    prod_records = get_negative_product_targets()
    if prod_records.empty:
        st.info("暂无商品否投记录")
    else:
        prod_view = prod_records.copy()
        if prod_campaign_id:
            prod_view = prod_view[prod_view["campaign_id"] == str(prod_campaign_id)]
        if prod_level == "广告组否投" and prod_ad_group_id:
            prod_view = prod_view[prod_view["ad_group_id"] == str(prod_ad_group_id)]
        prod_view = prod_view.merge(
            campaigns_df[["campaign_id", "campaign_name"]],
            on="campaign_id",
            how="left",
        )
        if prod_level == "广告组否投":
            prod_adgroups_df = _load_adgroups(prod_campaign_id)
            prod_view = prod_view.merge(
                prod_adgroups_df[["ad_group_id", "ad_group_name"]],
                on="ad_group_id",
                how="left",
            )
        prod_view["level"] = prod_view["level"].apply(lambda x: "活动级" if str(x) == "campaign" else "广告组级")
        prod_display = prod_view.rename(
            columns={
                "campaign_name": "活动名称",
                "campaign_id": "活动ID",
                "ad_group_name": "广告组名称",
                "ad_group_id": "广告组ID",
                "asin": "ASIN",
                "expression_type": "表达式",
                "level": "层级",
                "source": "来源",
                "status": "状态",
                "created_at": "创建时间",
                "last_updated": "最近更新",
            }
        )
        st.dataframe(
            prod_display[
                [
                    c
                    for c in [
                        "活动名称",
                        "活动ID",
                        "广告组名称",
                        "广告组ID",
                        "ASIN",
                        "表达式",
                        "层级",
                        "来源",
                        "状态",
                        "创建时间",
                        "最近更新",
                    ]
                    if c in prod_display.columns
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("#### 已有商品否投（Amazon）")
    prod_items = list_sp_negative_targets(
        session,
        headers,
        campaign_id=prod_campaign_id or None,
        ad_group_id=prod_ad_group_id if prod_level == "广告组否投" else None,
    )
    if not prod_items:
        st.info("Amazon 暂无商品否投数据")
    else:
        prod_rows = []
        for item in prod_items:
            expr = item.get("expression") or []
            asin_val = ""
            if isinstance(expr, list) and expr:
                val = expr[0].get("value") if isinstance(expr[0], dict) else ""
                asin_val = val or ""
            prod_rows.append(
                {
                    "targetId": item.get("targetId") or item.get("target_id") or "",
                    "campaignId": item.get("campaignId") or "",
                    "adGroupId": item.get("adGroupId") or "",
                    "state": item.get("state") or "",
                    "expressionType": item.get("expressionType") or "",
                    "asin": asin_val,
                }
            )
        prod_api_df = pd.DataFrame(prod_rows)
        prod_api_df["campaignId"] = prod_api_df["campaignId"].fillna("").astype(str)
        prod_api_df["adGroupId"] = prod_api_df["adGroupId"].fillna("").astype(str)
        prod_api_df["asin"] = prod_api_df["asin"].fillna("").astype(str)
        prod_api_df = prod_api_df.merge(
            campaigns_df[["campaign_id", "campaign_name"]],
            left_on="campaignId",
            right_on="campaign_id",
            how="left",
        )
        if prod_level == "广告组否投":
            prod_adgroups_df = _load_adgroups(prod_campaign_id)
            prod_api_df = prod_api_df.merge(
                prod_adgroups_df[["ad_group_id", "ad_group_name"]],
                left_on="adGroupId",
                right_on="ad_group_id",
                how="left",
            )
        prod_api_display = prod_api_df.rename(
            columns={
                "campaign_name": "活动名称",
                "campaignId": "活动ID",
                "ad_group_name": "广告组名称",
                "adGroupId": "广告组ID",
                "asin": "ASIN",
                "expressionType": "表达式",
                "state": "状态",
                "targetId": "TargetID",
            }
        )
        prod_api_display = prod_api_display.reset_index(drop=True)
        prod_api_display["选择"] = False
        prod_api_display["__row_id"] = prod_api_display.index
        prod_col_order = ["选择"] + [c for c in prod_api_display.columns if c not in ["选择", "__row_id"]]
        prod_edited = st.data_editor(
            prod_api_display[["__row_id"] + prod_col_order],
            use_container_width=True,
            hide_index=True,
            column_config={
                "选择": st.column_config.CheckboxColumn("选择", width="small"),
                "__row_id": None,
            },
            disabled=["__row_id"],
        )
        if st.button("🗑 删除所选商品否投", key="neg_prod_delete"):
            selected = prod_edited[prod_edited["选择"]]
            if selected.empty:
                st.warning("请先选择要删除的商品否投")
            else:
                ids = []
                recs = []
                for _, row in selected.iterrows():
                    row_id = row.get("__row_id")
                    if row_id is None:
                        continue
                    try:
                        row_id = int(row_id)
                    except Exception:
                        continue
                    if row_id < 0 or row_id >= len(prod_api_df):
                        continue
                    m = prod_api_df.iloc[row_id]
                    tid = m.get("targetId")
                    if tid:
                        ids.append(tid)
                    recs.append(
                        {
                            "campaign_id": str(m.get("campaignId", "") or ""),
                            "ad_group_id": str(m.get("adGroupId", "") or ""),
                            "asin": str(m.get("asin", "") or ""),
                            "expression_type": str(m.get("expressionType", "") or ""),
                            "level": "adgroup" if prod_level == "广告组否投" else "campaign",
                            "source": "manual",
                        }
                    )
                ok, resp = archive_sp_negative_targets(session, headers, ids)
                if ok:
                    update_negative_product_status(recs, "deleted")
                    st.success("删除请求已提交")
                    st.rerun()
                else:
                    st.error(f"删除失败：{resp}")
