import json
import re
import time
from datetime import date, datetime, timedelta

import pandas as pd

from .amazon_api import (
    create_sp_negative_keywords,
    get_amazon_session_and_headers,
    get_row_value,
    list_sp_campaign_negative_keywords,
    list_sp_campaigns,
    list_sp_keywords,
    list_sp_negative_keywords,
    list_sp_targets,
    update_sp_campaign_bidding,
    update_sp_keyword_bids,
    update_sp_target_bids,
)
from .config import (
    AUTO_NEGATIVE_ACOS_MULT_KEY,
    AUTO_NEGATIVE_CLICKS_KEY,
    AUTO_NEGATIVE_DAYS_KEY,
    AUTO_NEGATIVE_ENABLED_KEY,
    AUTO_NEGATIVE_LAST_RUN_KEY,
    AUTO_NEGATIVE_LEVEL_KEY,
    AUTO_NEGATIVE_MATCH_KEY,
    AUTO_NEGATIVE_PROTECT_KEY,
    AUTO_NEGATIVE_PROTECT_MODE_KEY,
    AUTO_NEGATIVE_SPEND_KEY,
    MIN_BID,
    REPORT_POLL_MAX,
    REPORT_POLL_SLEEP_SECONDS,
    get_real_today,
)
from .db import (
    db_write_lock,
    get_db_connection,
    get_system_value,
    save_auto_negative_keywords,
    set_system_value,
    update_auto_negative_status,
)
from .sync import sync_asin_report, sync_product_ads, sync_sp_adgroups

# --- 2. DeepSeek AI ---
try:
    import openai

    HAS_OPENAI = True
except Exception:
    HAS_OPENAI = False


def deepseek_audit(api_key, row, proposed_value, rule_reason, is_star, value_label):
    if not HAS_OPENAI or not api_key:
        return "未审核", proposed_value
    try:
        client = openai.OpenAI(api_key=api_key, base_url="https://api.deepseek.com")
        role = "【⭐主推款】(抢排名策略)" if is_star else "【普通款】(保利润策略)"
        prompt = f"""
        我是亚马逊运营。
        对象：{row['campaign_name']} ({role})
        数据：花费${row['cost']}, ACOS {row['acos']*100:.1f}%。
        算法建议：{value_label}调整为 ${proposed_value} (理由: {rule_reason})
        请审核该建议是否合理？
        返回 JSON: {{"comment": "理由", "final_value": 数字}}
        """
        response = client.chat.completions.create(
            model="deepseek-chat", messages=[{"role": "user", "content": prompt}], temperature=0.1
        )
        res = json.loads(re.search(r"\{.*\}", response.choices[0].message.content, re.DOTALL).group())
        final_value = res.get("final_value", res.get("final_bid", proposed_value))
        comment = res.get("comment", "")
        return f"AI: {comment}".strip(), float(final_value)
    except Exception:
        return "AI报错", proposed_value


def _chunked(items, size=100):
    for i in range(0, len(items), size):
        yield items[i : i + size]


INVALID_COLUMNS_ERROR = "columns includes invalid values"
AUTO_NEGATIVE_PENDING_KEY = "auto_negative_pending_report"
SEARCH_COST_KEYS = ["cost", "spend"]
SEARCH_SALES_KEYS = [
    "sales7d",
    "sales14d",
    "sales1d",
    "sales",
    "attributedsales14d",
    "attributedsales7d",
    "attributedsales1d",
]
SEARCH_ORDER_KEYS = [
    "purchases7d",
    "purchases14d",
    "purchases1d",
    "orders",
    "attributedconversions14d",
    "attributedconversions7d",
    "attributedconversions1d",
]


def _parse_allowed_columns(text):
    patterns = [
        r"Allowed values:\s*\((.*?)\)",
        r"Allowed values:\s*\[(.*?)\]",
    ]
    for pat in patterns:
        match = re.search(pat, text, re.DOTALL)
        if match:
            raw = match.group(1)
            return [c.strip() for c in raw.split(",") if c.strip()]
    match = re.search(r"Allowed values:\s*([^\r\n]+)", text)
    if match:
        raw = match.group(1)
        raw = raw.replace("(", "").replace(")", "").replace("[", "").replace("]", "")
        raw = raw.split(".")[0]
        return [c.strip() for c in raw.split(",") if c.strip()]
    return []


def _resolve_search_columns(allowed):
    columns = []
    for req in ["searchTerm", "campaignId", "adGroupId"]:
        if req in allowed:
            columns.append(req)
    for optional in ["keywordId", "keywordText", "matchType", "clicks", "impressions"]:
        if optional in allowed and optional not in columns:
            columns.append(optional)
    for key in SEARCH_COST_KEYS:
        if key in allowed and key not in columns:
            columns.append(key)
            break
    sales_keys = []
    for key in SEARCH_SALES_KEYS:
        if key in allowed and key not in columns:
            columns.append(key)
            sales_keys.append(key)
            break
    orders_keys = []
    for key in SEARCH_ORDER_KEYS:
        if key in allowed and key not in columns:
            columns.append(key)
            orders_keys.append(key)
            break
    return columns, sales_keys, orders_keys


def _fetch_search_term_report(session, headers, start_date, end_date):
    columns = [
        "searchTerm",
        "campaignId",
        "adGroupId",
        "keywordId",
        "keywordText",
        "matchType",
        "cost",
        "clicks",
        "impressions",
        "sales7d",
        "purchases7d",
    ]
    sales_keys = ["sales7d", "sales14d", "sales1d", "sales"]
    orders_keys = ["purchases7d", "purchases14d", "purchases1d", "orders"]

    def _request(cols, s_keys, o_keys):
        req = session.post(
            "https://advertising-api.amazon.com/reporting/reports",
            headers=headers,
            json={
                "startDate": start_date,
                "endDate": end_date,
                "configuration": {
                    "adProduct": "SPONSORED_PRODUCTS",
                    "groupBy": ["searchTerm"],
                    "columns": cols,
                    "reportTypeId": "spSearchTerm",
                    "timeUnit": "DAILY",
                    "format": "GZIP_JSON",
                },
            },
        )
        if req.status_code in [200, 201, 202]:
            rid = req.json().get("reportId")
        elif "duplicate" in req.text.lower():
            rid = req.json().get("detail", "").split(":")[-1].strip()
        elif req.status_code == 400 and INVALID_COLUMNS_ERROR in req.text:
            allowed = _parse_allowed_columns(req.text)
            if allowed:
                new_cols, new_sales, new_orders = _resolve_search_columns(allowed)
                if new_cols:
                    return _request(new_cols, new_sales, new_orders)
            return None, s_keys, o_keys, f"search term invalid columns"
        else:
            return None, s_keys, o_keys, f"search term create failed {req.status_code}: {req.text[:200]}"
        return rid, s_keys, o_keys, None

    return _request(columns, sales_keys, orders_keys)


def _check_report_once(session, headers, report_id):
    try:
        chk = session.get(f"https://advertising-api.amazon.com/reporting/reports/{report_id}", headers=headers)
        if chk.status_code != 200:
            return None, f"status {chk.status_code}: {chk.text[:200]}"
        payload = chk.json()
        status = payload.get("status")
        if status == "COMPLETED":
            url = payload.get("url")
            if url:
                data = pd.read_json(url, compression="gzip")
                return data, None
            return None, "completed without url"
        if status in ["FAILED", "CANCELLED"]:
            return None, f"status {status}"
        return None, "pending"
    except Exception as exc:
        return None, f"exception: {exc}"


def _wait_for_report(session, headers, report_id, max_polls=10):
    for _ in range(max_polls):
        time.sleep(REPORT_POLL_SLEEP_SECONDS)
        data, err = _check_report_once(session, headers, report_id)
        if err == "pending":
            continue
        return data, err
    return None, "pending"


def _load_pending_report():
    raw = get_system_value(AUTO_NEGATIVE_PENDING_KEY)
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _save_pending_report(report_id, start_date, end_date, sales_keys, orders_keys):
    payload = {
        "report_id": report_id,
        "start": start_date,
        "end": end_date,
        "sales_keys": sales_keys,
        "orders_keys": orders_keys,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    set_system_value(AUTO_NEGATIVE_PENDING_KEY, json.dumps(payload))


def _clear_pending_report():
    set_system_value(AUTO_NEGATIVE_PENDING_KEY, "")


def _get_auto_negative_config(cfg):
    defaults = {
        "enabled": False,
        "level": "adgroup",
        "match": "NEGATIVE_EXACT",
        "spend": 3.0,
        "clicks": 8,
        "acos_mult": 1.5,
        "days": 7,
        "protect_terms": [],
        "protect_mode": "contains",
    }
    if cfg:
        defaults.update(cfg)
    return defaults


def _normalize_negative_match(match_type):
    if not match_type:
        return "NEGATIVE_EXACT"
    mt = str(match_type).strip().upper()
    if mt in ["NEGATIVE_EXACT", "NEGATIVE_PHRASE"]:
        return mt
    if mt in ["EXACT", "PHRASE"]:
        return f"NEGATIVE_{mt}"
    if mt in ["NEGATIVEEXACT"]:
        return "NEGATIVE_EXACT"
    if mt in ["NEGATIVEPHRASE"]:
        return "NEGATIVE_PHRASE"
    return mt


def _parse_protect_terms(raw):
    if not raw:
        return []
    if isinstance(raw, list):
        terms = raw
    else:
        text = str(raw)
        parts = []
        for chunk in text.replace(",", "\n").replace(";", "\n").splitlines():
            chunk = chunk.strip()
            if chunk:
                parts.append(chunk)
        terms = parts
    return [t.lower() for t in terms if str(t).strip()]


def _is_protected_term(term, protect_terms, mode):
    if not protect_terms:
        return False
    t = str(term or "").strip().lower()
    if not t:
        return False
    if str(mode).lower() == "exact":
        return t in protect_terms
    return any(p in t for p in protect_terms)

def run_optimization_logic(
    base_target_acos, base_max_bid, base_stop_loss, is_live_mode, deepseek_key, auto_negative_config=None
):
    conn = get_db_connection()
    logs = []
    session, headers = get_amazon_session_and_headers()
    live_note = ""
    if not session:
        if is_live_mode:
            is_live_mode = False
            live_note = " | 无API配置，已转模拟"
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        reason = "无API配置，无法拉取关键词/投放/广告位"
        with db_write_lock():
            conn.execute(
                "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                (ts, "系统", "SP 自动驾驶", 0, 0, reason, "失败"),
            )
            conn.commit()
        conn.close()
        return logs
    try:
        sync_sp_adgroups(session, headers)
        sync_product_ads(session, headers)
    except Exception:
        pass
    today = get_real_today()
    start = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")

    # 先确保 ASIN 报表有最近 7 天数据
    try:
        row = conn.execute("SELECT MAX(date) FROM asin_reports").fetchone()
    except Exception:
        row = None
    latest_asin_date = None
    if row and row[0]:
        try:
            latest_asin_date = date.fromisoformat(row[0])
        except Exception:
            latest_asin_date = None
    target_end = today - timedelta(days=1)
    missing_days = 0
    if latest_asin_date is None:
        missing_days = 7
    else:
        missing_days = (target_end - latest_asin_date).days
        if missing_days < 0:
            missing_days = 0
        missing_days = min(missing_days, 7)

    if missing_days > 0:
        errors = []
        for i in range(1, missing_days + 1):
            d_str = (today - timedelta(days=i)).strftime("%Y-%m-%d")
            errors.extend(sync_asin_report(session, headers, d_str))
        if errors:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            reason = errors[0]
            with db_write_lock():
                conn.execute(
                    "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                    (ts, "系统", "ASIN 报表同步", 0, 0, reason, "失败"),
                )
                conn.commit()

    # === ASIN 级别：预算 + 目标 ACOS 调整 SP 关键词/投放/广告位 ===
    try:
        settings_df = pd.read_sql(
            "SELECT asin, sku, daily_budget, target_acos, budget_flex, is_star, ai_enabled FROM product_settings",
            conn,
        )
    except Exception:
        settings_df = pd.DataFrame()

    if not settings_df.empty:
        settings_df["asin"] = settings_df["asin"].fillna("").astype(str)
        settings_df["sku"] = settings_df["sku"].fillna("").astype(str)
        settings_df["daily_budget"] = settings_df["daily_budget"].fillna(0.0)
        settings_df["target_acos"] = settings_df["target_acos"].fillna(0.0)
        settings_df["budget_flex"] = settings_df["budget_flex"].fillna(0.0)
        settings_df["is_star"] = settings_df["is_star"].fillna(0).astype(bool)
        settings_df["ai_enabled"] = settings_df["ai_enabled"].fillna(1).astype(bool)

        asin_perf = pd.read_sql(
            f"""
            SELECT asin, sku, SUM(cost) as cost, SUM(sales) as sales
            FROM asin_reports
            WHERE date >= '{start}' AND date <= '{end}'
            GROUP BY asin, sku
            """,
            conn,
        )
        if not asin_perf.empty:
            asin_perf["asin"] = asin_perf["asin"].fillna("").astype(str)
            asin_perf["sku"] = asin_perf["sku"].fillna("").astype(str)
        else:
            asin_perf = pd.DataFrame(columns=["asin", "sku", "cost", "sales"])

        asin_data = pd.merge(settings_df, asin_perf, on=["asin", "sku"], how="left")
        asin_data["cost"] = asin_data["cost"].fillna(0.0)
        asin_data["sales"] = asin_data["sales"].fillna(0.0)
        asin_data["acos"] = asin_data.apply(
            lambda x: x["cost"] / x["sales"] if x["sales"] > 0 else 0, axis=1
        )

        days_window = 7
        asin_rules = {}
        asin_rules_by_asin = {}
        for _, row in asin_data.iterrows():
            if not row["ai_enabled"]:
                continue
            asin = str(row["asin"]).strip()
            if not asin:
                continue
            sku = str(row["sku"]).strip()
            daily_budget = float(row["daily_budget"] or 0)
            target_acos = float(row["target_acos"] or 0)
            budget_flex = float(row.get("budget_flex", 0) or 0)
            is_star = bool(row["is_star"])
            cost = float(row["cost"] or 0)
            sales = float(row["sales"] or 0)
            acos = float(row["acos"] or 0)
            if budget_flex < 0:
                budget_flex = 0.0
            if budget_flex > 100:
                budget_flex = 100.0

            if target_acos <= 0:
                target_acos = base_target_acos
            target_acos = target_acos * (1.5 if is_star else 1.0)
            stop_loss = base_stop_loss * (2.0 if is_star else 1.0)
            tag = "⭐" if is_star else "🥔"

            action = "保持"
            reasons = []
            acos_factor = 1.0

            if cost > stop_loss and sales == 0:
                action = "🛑 止损"
                reasons.append(f"{tag} 7天耗${cost:.0f}无单")
            elif sales > 0 and acos > (target_acos / 100):
                action = "📉 降价"
                acos_factor = max((target_acos / 100) / acos, 0.8)
                reasons.append(f"{tag} ACOS偏高")
            elif sales > 0 and acos < (target_acos / 100 * (1.0 if is_star else 0.8)):
                action = "🚀 拓量"
                acos_factor = 1.1
                reasons.append(f"{tag} 表现优异")

            budget_factor = 1.0
            if daily_budget > 0:
                avg_spend = cost / days_window
                budget_limit = daily_budget * (1.0 + budget_flex / 100.0)
                if avg_spend > budget_limit:
                    budget_factor = max(budget_limit / avg_spend, 0.5)
                    if budget_flex > 0:
                        reasons.append(f"超预算 日均${avg_spend:.2f}/{daily_budget:.2f} (+{budget_flex:.0f}%)")
                    else:
                        reasons.append(f"超预算 日均${avg_spend:.2f}/{daily_budget:.2f}")
                    if action == "保持":
                        action = "📉 降价"

            if action == "保持" and budget_factor == 1.0 and acos_factor == 1.0:
                continue

            if action == "🚀 拓量" and budget_factor < 1.0:
                action = "📉 降价"

            final_factor = min(acos_factor, budget_factor)
            reason = " | ".join(reasons) if reasons else "-"
            rule = {
                "asin": asin,
                "sku": sku,
                "action": action,
                "reason": reason,
                "factor": final_factor,
                "tag": tag,
                "target_acos": target_acos,
            }
            asin_rules[(asin, sku)] = rule
            if not sku:
                asin_rules_by_asin[asin] = rule

        ads = pd.read_sql("SELECT ad_group_id, asin, sku, state FROM product_ads", conn)
        ad_groups = pd.read_sql("SELECT ad_group_id, campaign_id, state FROM ad_group_settings", conn)

        enabled_states = ["ENABLED", "ENABLED_WITH_PENDING_CHANGES"]
        ad_group_campaign = {}
        enabled_ad_groups = None
        if not ad_groups.empty:
            ad_groups["ad_group_id"] = ad_groups["ad_group_id"].fillna("").astype(str)
            ad_groups["campaign_id"] = ad_groups["campaign_id"].fillna("").astype(str)
            groups_state = ad_groups["state"].fillna("").astype(str).str.upper()
            ad_groups = ad_groups[(groups_state == "") | (groups_state.isin(enabled_states))]
            ad_group_campaign = dict(zip(ad_groups["ad_group_id"], ad_groups["campaign_id"]))
            enabled_ad_groups = set(ad_groups["ad_group_id"].tolist())

        if not ads.empty:
            ads["asin"] = ads["asin"].fillna("").astype(str).str.strip()
            ads["sku"] = ads["sku"].fillna("").astype(str).str.strip()
            ads_state = ads["state"].fillna("").astype(str).str.upper()
            ads = ads[(ads_state == "") | (ads_state.isin(enabled_states))]

        ad_group_map = {}
        for ad_group_id, group in ads.groupby("ad_group_id"):
            if not ad_group_id:
                continue
            asins = sorted({a for a in group["asin"].tolist() if a})
            if len(asins) != 1:
                continue
            asin = asins[0]
            skus = sorted({s for s in group["sku"].tolist() if s})
            rule = None
            if len(skus) == 1:
                rule = asin_rules.get((asin, skus[0])) or asin_rules_by_asin.get(asin)
            else:
                rule = asin_rules_by_asin.get(asin)
            if not rule:
                continue
            ad_group_map[str(ad_group_id)] = rule

        ad_group_campaign = dict(zip(ad_groups["ad_group_id"], ad_groups["campaign_id"]))
        keywords = list_sp_keywords(session, headers)
        targets = list_sp_targets(session, headers)
        campaigns = list_sp_campaigns(session, headers, include_extended=True)

        keyword_updates = []
        target_updates = []
        rule_stats = {}

        for rule in ad_group_map.values():
            key = (rule["asin"], rule["sku"])
            rule_stats[key] = {"keywords": 0, "targets": 0, "placements": 0, "reason": rule["reason"]}

        for kw in keywords:
            keyword_id = kw.get("keywordId") or kw.get("keyword_id")
            if not keyword_id:
                continue
            ad_group_id = str(kw.get("adGroupId") or kw.get("ad_group_id") or "")
            rule = ad_group_map.get(ad_group_id)
            if not rule:
                continue
            state_flag = str(kw.get("state", "")).upper()
            if state_flag and state_flag not in enabled_states:
                continue
            old_bid = float(kw.get("bid", 0) or 0)
            if old_bid <= 0:
                continue
            if rule["action"] == "🛑 止损":
                new_bid = MIN_BID
            else:
                new_bid = max(MIN_BID, min(old_bid * rule["factor"], base_max_bid))
            if abs(new_bid - old_bid) < 0.01:
                continue
            keyword_updates.append({"keywordId": str(keyword_id), "bid": round(new_bid, 2)})
            rule_stats[(rule["asin"], rule["sku"])]["keywords"] += 1

        for tg in targets:
            id_key = "targetId"
            target_id = tg.get("targetId") or tg.get("target_id")
            if not target_id:
                id_key = "targetingClauseId"
                target_id = tg.get("targetingClauseId") or tg.get("targeting_clause_id")
            if not target_id:
                continue
            ad_group_id = str(tg.get("adGroupId") or tg.get("ad_group_id") or "")
            rule = ad_group_map.get(ad_group_id)
            if not rule:
                continue
            state_flag = str(tg.get("state", "")).upper()
            if state_flag and state_flag not in enabled_states:
                continue
            old_bid = float(tg.get("bid", 0) or 0)
            if old_bid <= 0:
                continue
            if rule["action"] == "🛑 止损":
                new_bid = MIN_BID
            else:
                new_bid = max(MIN_BID, min(old_bid * rule["factor"], base_max_bid))
            if abs(new_bid - old_bid) < 0.01:
                continue
            target_updates.append({id_key: str(target_id), "bid": round(new_bid, 2)})
            rule_stats[(rule["asin"], rule["sku"])]["targets"] += 1

        placement_updates = []
        placement_predicates = ["placementTop", "placementProductPage", "placementRestOfSearch"]
        defaults_up = {"placementTop": 20, "placementProductPage": 10, "placementRestOfSearch": 5}
        campaign_bids = {}
        for c in campaigns:
            campaign_id = c.get("campaignId", c.get("campaign_id"))
            if not campaign_id:
                continue
            bidding = c.get("bidding") or {}
            adjustments = bidding.get("adjustments") or []
            current = {}
            for adj in adjustments:
                predicate = adj.get("predicate")
                pct = adj.get("percentage")
                if predicate:
                    current[predicate] = pct if pct is not None else 0
            campaign_bids[str(campaign_id)] = current

        campaign_rule_keys = {}
        for ad_group_id, rule in ad_group_map.items():
            campaign_id = ad_group_campaign.get(ad_group_id, "")
            if not campaign_id:
                continue
            key = (rule["asin"], rule["sku"])
            campaign_rule_keys.setdefault(campaign_id, set()).add(key)

        for campaign_id, keys in campaign_rule_keys.items():
            if len(keys) != 1:
                continue
            rule_key = next(iter(keys))
            rule = asin_rules.get(rule_key)
            if not rule:
                continue
            if rule["action"] == "保持" and rule["factor"] == 1.0:
                continue
            current = campaign_bids.get(str(campaign_id), {})
            adjustments = []
            for predicate in placement_predicates:
                current_pct = int(current.get(predicate, 0) or 0)
                if rule["action"] == "🛑 止损":
                    new_pct = 0
                else:
                    if current_pct > 0:
                        new_pct = int(round(current_pct * rule["factor"]))
                    else:
                        new_pct = defaults_up[predicate] if rule["factor"] > 1.0 else 0
                new_pct = max(0, min(new_pct, 900))
                if current_pct == new_pct:
                    continue
                adjustments.append({"predicate": predicate, "percentage": new_pct})
            if not adjustments:
                continue
            placement_updates.append({"campaignId": str(campaign_id), "bidding": {"adjustments": adjustments}})
            rule_stats[rule_key]["placements"] += 1

        keyword_ok = True
        target_ok = True
        placement_ok = True
        if is_live_mode:
            for batch in _chunked(keyword_updates, 100):
                ok, _ = update_sp_keyword_bids(session, headers, batch)
                if not ok:
                    keyword_ok = False
            for batch in _chunked(target_updates, 100):
                ok, _ = update_sp_target_bids(session, headers, batch)
                if not ok:
                    target_ok = False
            for update in placement_updates:
                ok, _ = update_sp_campaign_bidding(session, headers, [update])
                if not ok and len(update["bidding"]["adjustments"]) > 1:
                    filtered = [
                        adj for adj in update["bidding"]["adjustments"] if adj["predicate"] != "placementRestOfSearch"
                    ]
                    if filtered:
                        ok, _ = update_sp_campaign_bidding(
                            session, headers, [{"campaignId": update["campaignId"], "bidding": {"adjustments": filtered}}]
                        )
                if not ok:
                    placement_ok = False

        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with db_write_lock():
            for rule_key, stats in rule_stats.items():
                total_ops = stats["keywords"] + stats["targets"] + stats["placements"]
                if total_ops <= 0:
                    continue
                status = "模拟"
                if is_live_mode:
                    if keyword_ok and target_ok and placement_ok:
                        status = "已执行"
                    elif keyword_ok or target_ok or placement_ok:
                        status = "部分失败"
                    else:
                        status = "失败"
                asin, sku = rule_key
                reason = stats["reason"]
                if live_note:
                    reason = f"{reason}{live_note}"
                reason = f"{reason} | 关键词{stats['keywords']} 投放{stats['targets']} 广告位{stats['placements']}"
                action_label = "SP 竞价调整"
                logs.append(
                    {
                        "时间": ts,
                        "广告": f"ASIN:{asin}",
                        "类型": "ASIN",
                        "动作": action_label,
                        "原价": 0,
                        "新价": 0,
                        "理由": reason,
                    }
                )
                conn.execute(
                    "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                    (ts, f"ASIN:{asin}", action_label, 0, 0, reason, status),
                )
            conn.commit()

        # === 自动否词：基于搜索词表现 ===
        auto_neg_cfg = _get_auto_negative_config(auto_negative_config)
        if auto_neg_cfg["enabled"]:
            level = auto_neg_cfg.get("level", "adgroup")
            match_type = _normalize_negative_match(auto_neg_cfg.get("match", "NEGATIVE_EXACT"))
            spend_threshold = float(auto_neg_cfg.get("spend", 3.0) or 0)
            clicks_threshold = int(auto_neg_cfg.get("clicks", 8) or 0)
            acos_mult = float(auto_neg_cfg.get("acos_mult", 1.5) or 1.0)
            days = int(auto_neg_cfg.get("days", 7) or 7)
            days = max(1, min(days, 30))
            protect_terms = auto_neg_cfg.get("protect_terms")
            if not protect_terms:
                protect_terms = get_system_value(AUTO_NEGATIVE_PROTECT_KEY)
            protect_mode = auto_neg_cfg.get("protect_mode") or get_system_value(
                AUTO_NEGATIVE_PROTECT_MODE_KEY
            ) or "contains"
            protect_terms = _parse_protect_terms(protect_terms)
            if str(protect_mode).lower() == "exact":
                protect_terms = set(protect_terms)

            end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
            start_date = (today - timedelta(days=days)).strftime("%Y-%m-%d")

            data = None
            sales_keys = []
            orders_keys = []
            pending = _load_pending_report()

            if (
                pending
                and pending.get("report_id")
                and pending.get("start") == start_date
                and pending.get("end") == end_date
            ):
                sales_keys = pending.get("sales_keys") or []
                orders_keys = pending.get("orders_keys") or []
                data, err = _check_report_once(session, headers, pending.get("report_id"))
                if err == "pending":
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    reason = f"否词报告生成中: {pending.get('report_id')}"
                    logs.append(
                        {
                            "时间": ts,
                            "广告": "系统",
                            "类型": "否词",
                            "动作": "SP 自动否词",
                            "原价": 0,
                            "新价": 0,
                            "理由": reason,
                        }
                    )
                    with db_write_lock():
                        conn.execute(
                            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                            (ts, "系统", "SP 自动否词", 0, 0, reason, "模拟"),
                        )
                        conn.commit()
                    data = None
                elif err:
                    _clear_pending_report()
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    reason = f"否词报告失败: {err}"
                    logs.append(
                        {
                            "时间": ts,
                            "广告": "系统",
                            "类型": "否词",
                            "动作": "SP 自动否词",
                            "原价": 0,
                            "新价": 0,
                            "理由": reason,
                        }
                    )
                    with db_write_lock():
                        conn.execute(
                            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                            (ts, "系统", "SP 自动否词", 0, 0, reason, "失败"),
                        )
                        conn.commit()
                    data = None
                else:
                    _clear_pending_report()
            else:
                if pending:
                    _clear_pending_report()
                report_id, sales_keys, orders_keys, err = _fetch_search_term_report(
                    session, headers, start_date, end_date
                )
                if err:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    reason = f"否词报告失败: {err}"
                    logs.append(
                        {
                            "时间": ts,
                            "广告": "系统",
                            "类型": "否词",
                            "动作": "SP 自动否词",
                            "原价": 0,
                            "新价": 0,
                            "理由": reason,
                        }
                    )
                    with db_write_lock():
                        conn.execute(
                            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                            (ts, "系统", "SP 自动否词", 0, 0, reason, "失败"),
                        )
                        conn.commit()
                    data = None
                else:
                    data, err = _wait_for_report(session, headers, report_id, max_polls=10)
                    if err == "pending":
                        _save_pending_report(report_id, start_date, end_date, sales_keys, orders_keys)
                        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        reason = f"否词报告生成中: {report_id}"
                        logs.append(
                            {
                                "时间": ts,
                                "广告": "系统",
                                "类型": "否词",
                                "动作": "SP 自动否词",
                                "原价": 0,
                                "新价": 0,
                                "理由": reason,
                            }
                        )
                        with db_write_lock():
                            conn.execute(
                                "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                                (ts, "系统", "SP 自动否词", 0, 0, reason, "模拟"),
                            )
                            conn.commit()
                        data = None
                    elif err:
                        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        reason = f"否词报告失败: {err}"
                        logs.append(
                            {
                                "时间": ts,
                                "广告": "系统",
                                "类型": "否词",
                                "动作": "SP 自动否词",
                                "原价": 0,
                                "新价": 0,
                                "理由": reason,
                            }
                        )
                        with db_write_lock():
                            conn.execute(
                                "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                                (ts, "系统", "SP 自动否词", 0, 0, reason, "失败"),
                            )
                            conn.commit()
                        data = None
                    else:
                        _clear_pending_report()

            if data is None or data.empty:
                if data is not None and data.empty:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    reason = "否词报告为空"
                    logs.append(
                        {
                            "时间": ts,
                            "广告": "系统",
                            "类型": "否词",
                            "动作": "SP 自动否词",
                            "原价": 0,
                            "新价": 0,
                            "理由": reason,
                        }
                    )
                    with db_write_lock():
                        conn.execute(
                            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                            (ts, "系统", "SP 自动否词", 0, 0, reason, "模拟" if not is_live_mode else "失败"),
                        )
                        conn.commit()
            else:
                data.columns = [c.lower() for c in data.columns]
                sales_candidates = [k.lower() for k in (sales_keys or [])] + SEARCH_SALES_KEYS
                order_candidates = [k.lower() for k in (orders_keys or [])] + SEARCH_ORDER_KEYS

                records = []
                for _, row in data.iterrows():
                    search_term = row.get("searchterm") or row.get("search_term")
                    if not search_term:
                        continue
                    campaign_id = row.get("campaignid") or row.get("campaign_id")
                    ad_group_id = row.get("adgroupid") or row.get("ad_group_id")
                    if not ad_group_id:
                        continue
                    cost = get_row_value(row, SEARCH_COST_KEYS, 0)
                    sales = get_row_value(row, sales_candidates, 0)
                    orders = get_row_value(row, order_candidates, 0)
                    clicks = get_row_value(row, ["clicks"], 0)
                    impressions = get_row_value(row, ["impressions"], 0)
                    records.append(
                        {
                            "campaign_id": str(campaign_id or ""),
                            "ad_group_id": str(ad_group_id),
                            "search_term": str(search_term).strip(),
                            "cost": float(cost or 0),
                            "sales": float(sales or 0),
                            "orders": float(orders or 0),
                            "clicks": float(clicks or 0),
                            "impressions": float(impressions or 0),
                        }
                    )

                if records:
                    df_terms = pd.DataFrame(records)
                    agg = (
                        df_terms.groupby(["campaign_id", "ad_group_id", "search_term"], as_index=False)[
                            ["cost", "sales", "orders", "clicks", "impressions"]
                        ]
                        .sum()
                        .reset_index(drop=True)
                    )
                else:
                    agg = pd.DataFrame()

                if agg.empty:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    reason = "否词报告无有效数据"
                    logs.append(
                        {
                            "时间": ts,
                            "广告": "系统",
                            "类型": "否词",
                            "动作": "SP 自动否词",
                            "原价": 0,
                            "新价": 0,
                            "理由": reason,
                        }
                    )
                    with db_write_lock():
                        conn.execute(
                            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                            (ts, "系统", "SP 自动否词", 0, 0, reason, "模拟" if not is_live_mode else "失败"),
                        )
                        conn.commit()
                else:
                    # 已存在否词集合
                    existing = set()
                    if level == "campaign":
                        items = list_sp_campaign_negative_keywords(session, headers)
                        for item in items:
                            key = (
                                str(item.get("campaignId") or ""),
                                "",
                                str(item.get("keywordText") or "").strip().lower(),
                                _normalize_negative_match(item.get("matchType")),
                            )
                            existing.add(key)
                    else:
                        items = list_sp_negative_keywords(session, headers)
                        for item in items:
                            key = (
                                str(item.get("campaignId") or ""),
                                str(item.get("adGroupId") or ""),
                                str(item.get("keywordText") or "").strip().lower(),
                                _normalize_negative_match(item.get("matchType")),
                            )
                            existing.add(key)

                    payloads = []
                    ai_records = []
                    for _, r in agg.iterrows():
                        ad_group_id = r.get("ad_group_id")
                        if ad_group_id is None or (isinstance(ad_group_id, float) and pd.isna(ad_group_id)):
                            ad_group_id = ""
                        ad_group_id = str(ad_group_id or "").strip()
                        if not ad_group_id:
                            continue
                        if enabled_ad_groups is not None and ad_group_id not in enabled_ad_groups:
                            continue
                        rule = ad_group_map.get(ad_group_id)
                        if not rule:
                            rule = {"target_acos": base_target_acos}
                        term = str(r["search_term"]).strip()
                        if not term:
                            continue
                        if _is_protected_term(term, protect_terms, protect_mode):
                            continue
                        cost = float(r["cost"] or 0)
                        sales = float(r["sales"] or 0)
                        clicks = float(r["clicks"] or 0)
                        orders = float(r.get("orders") or 0)
                        if cost < spend_threshold or clicks < clicks_threshold:
                            continue
                        target_acos = float(rule.get("target_acos", base_target_acos) or base_target_acos)
                        term_acos = cost / sales if sales > 0 else None
                        action = None
                        reason_detail = ""
                        if sales <= 0:
                            action = "无转化高花费"
                            reason_detail = f"{action}: 花费${cost:.2f} 点击{int(clicks)}"
                        elif term_acos is not None and term_acos > (target_acos / 100.0 * acos_mult):
                            action = "ACOS过高"
                            reason_detail = (
                                f"{action}: {term_acos*100:.1f}% > 目标{target_acos:.1f}%×{acos_mult}"
                            )
                        if not action:
                            continue
                        campaign_id = r.get("campaign_id")
                        if campaign_id is None or (isinstance(campaign_id, float) and pd.isna(campaign_id)):
                            campaign_id = ""
                        campaign_id = str(campaign_id or "")
                        if not campaign_id:
                            campaign_id = str(ad_group_campaign.get(ad_group_id, "") or "")
                        if not campaign_id:
                            continue
                        key = (
                            campaign_id,
                            "" if level == "campaign" else ad_group_id,
                            term.lower(),
                            match_type,
                        )
                        if key in existing:
                            continue
                        item = {
                            "campaignId": campaign_id,
                            "keywordText": term,
                            "matchType": match_type,
                            "state": "ENABLED",
                        }
                        if level == "adgroup":
                            item["adGroupId"] = ad_group_id
                        payloads.append(item)
                        existing.add(key)
                        ai_records.append(
                            {
                                "campaign_id": str(campaign_id or ""),
                                "ad_group_id": "" if level == "campaign" else str(ad_group_id or ""),
                                "keyword_text": str(term),
                                "match_type": str(match_type),
                                "level": level,
                                "source": "AI",
                                "status": "pending" if is_live_mode else "dry_run",
                                "reason": reason_detail or action,
                                "cost": cost,
                                "sales": sales,
                                "orders": orders,
                                "clicks": clicks,
                            }
                        )
                    if ai_records:
                        save_auto_negative_keywords(ai_records)

                    created = 0
                    status = "模拟"
                    if payloads and is_live_mode:
                        ok_all = True
                        for batch in _chunked(payloads, 100):
                            ok, _ = create_sp_negative_keywords(
                                session, headers, batch, campaign_level=(level == "campaign")
                            )
                            if not ok:
                                ok_all = False
                        if ok_all:
                            status = "已执行"
                            update_auto_negative_status(ai_records, "created")
                        else:
                            status = "部分失败"
                            update_auto_negative_status(ai_records, "partial")
                    elif payloads:
                        status = "模拟"
                        update_auto_negative_status(ai_records, "dry_run")

                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    reason = f"自动否词 {len(payloads)} 条 | 阈值 花费≥{spend_threshold} 点击≥{clicks_threshold} ACOS×{acos_mult}"
                    logs.append(
                        {
                            "时间": ts,
                            "广告": "系统",
                            "类型": "否词",
                            "动作": "SP 自动否词",
                            "原价": 0,
                            "新价": 0,
                            "理由": reason,
                        }
                    )
                    with db_write_lock():
                        conn.execute(
                            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                            (ts, "系统", "SP 自动否词", 0, 0, reason, status),
                        )
                        conn.commit()
                    set_system_value(AUTO_NEGATIVE_LAST_RUN_KEY, ts)

    conn.commit()
    conn.close()
    return logs
