import json
import os
import re
import time
from datetime import date, datetime, timedelta

import pandas as pd

from .amazon_api import (
    create_sp_keywords,
    create_sp_negative_keywords,
    create_sp_negative_targets,
    get_amazon_session_and_headers,
    get_row_value,
    list_sp_campaign_negative_keywords,
    list_sp_campaigns,
    list_sp_keywords,
    list_sp_negative_targets,
    list_sp_negative_keywords,
    list_sp_targets,
    update_campaign_budget,
    update_sp_adgroup_bids,
    update_sp_campaign_bidding,
    update_sp_keyword_bids,
    update_sp_target_bids,
)
from .config import (
    AUTO_KEYWORD_POOL_DAILY_MAX_KEY,
    AUTO_KEYWORD_POOL_ENABLED_KEY,
    AUTO_KEYWORD_POOL_MIN_FLOW_KEY,
    AUTO_KEYWORD_POOL_NEG_CLICKS_KEY,
    AUTO_KEYWORD_POOL_NEG_ORDERS_KEY,
    AUTO_KEYWORD_POOL_PATH_KEY,
    AUTO_AI_LEARNING_ENABLED_KEY,
    AUTO_AI_LEARNING_LAST_DATE_KEY,
    AUTO_AI_LEARNING_NOTE_KEY,
    AUTO_AI_LEARNING_RATE_KEY,
    AUTO_AI_HARVEST_MIN_ORDERS_KEY,
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
    AUTO_NEGATIVE_PROTECT_KEY,
    AUTO_NEGATIVE_PROTECT_MODE_KEY,
    AUTO_NEGATIVE_SPEND_KEY,
    BASE_DIR,
    MIN_BID,
    REPORT_POLL_MAX,
    REPORT_POLL_SLEEP_SECONDS,
    get_auto_ai_campaign_exclusions,
    get_auto_ai_campaign_whitelist,
    get_real_today,
)
from .db import (
    db_write_lock,
    get_db_connection,
    get_bid_baselines,
    get_system_value,
    save_auto_negative_keywords,
    save_bid_baselines,
    save_negative_product_targets,
    set_system_value,
    update_auto_negative_status,
    update_negative_product_status,
)
from .sync import sync_asin_report, sync_campaign_report, sync_product_ads, sync_sp_adgroups

# --- 2. DeepSeek AI ---
from .ai import HAS_OPENAI, deepseek_audit, deepseek_relevance


def _quick_relevant(term, positive_terms):
    t = str(term or "").lower()
    if not t:
        return False
    for p in positive_terms:
        p = str(p or "").lower().strip()
        if p and p in t:
            return True
    return False


def _chunked(items, size=100):
    for i in range(0, len(items), size):
        yield items[i : i + size]


INVALID_COLUMNS_ERROR = "columns includes invalid values"
AUTO_NEGATIVE_PENDING_KEY = "auto_negative_pending_report"
AUTO_KEYWORD_PENDING_KEY = "auto_keyword_pending_report"
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


def _load_pending_report(key=None):
    if not key:
        key = AUTO_NEGATIVE_PENDING_KEY
    raw = get_system_value(key)
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _save_pending_report(report_id, start_date, end_date, sales_keys, orders_keys, key=None):
    if not key:
        key = AUTO_NEGATIVE_PENDING_KEY
    payload = {
        "report_id": report_id,
        "start": start_date,
        "end": end_date,
        "sales_keys": sales_keys,
        "orders_keys": orders_keys,
        "ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    set_system_value(key, json.dumps(payload))


def _clear_pending_report(key=None):
    if not key:
        key = AUTO_NEGATIVE_PENDING_KEY
    set_system_value(key, "")


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


def _normalize_positive_match(match_type):
    if not match_type:
        return ""
    mt = str(match_type).strip().upper()
    if mt in ["EXACT", "PHRASE", "BROAD"]:
        return mt
    if mt in ["EXACT_MATCH"]:
        return "EXACT"
    if mt in ["PHRASE_MATCH"]:
        return "PHRASE"
    if mt in ["BROAD_MATCH"]:
        return "BROAD"
    return mt


def _is_asin_term(term):
    t = str(term or "").strip().upper()
    if len(t) != 10:
        return False
    if not re.match(r"^[A-Z0-9]{10}$", t):
        return False
    return t.startswith("B")


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
        return str(val).strip().lower() in ["1", "true", "yes", "on"]
    except Exception:
        return default


def _safe_float(value, default=0.0):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    try:
        text = str(value).replace(",", "").replace("%", "").strip()
        if text == "":
            return default
        return float(text)
    except Exception:
        return default


def _pick_column(columns, candidates):
    col_map = {str(c).strip().lower(): c for c in columns}
    for cand in candidates:
        key = str(cand).strip().lower()
        if key in col_map:
            return col_map[key]
    return None


def _load_keyword_pool(path, min_flow=0.0):
    if not path:
        return [], set(), "missing path"
    if not os.path.exists(path):
        return [], set(), f"missing file: {path}"
    try:
        if str(path).lower().endswith((".xlsx", ".xls")):
            df = pd.read_excel(path)
        else:
            df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception as exc:
        return [], set(), f"read failed: {exc}"
    if df is None or df.empty:
        return [], set(), "empty pool"
    col_term = _pick_column(df.columns, ["关键词", "keyword", "search term", "search_term", "term"])
    if not col_term:
        return [], set(), "missing keyword column"
    col_flow = _pick_column(df.columns, ["流量占比", "流量占比%", "traffic share", "traffic_share"])
    col_ppc = _pick_column(df.columns, ["ppc竞价", "ppc", "cpc", "ppc_bid", "PPC竞价"])
    col_label = _pick_column(df.columns, ["关键词分类", "分类", "label", "cluster"])

    records = []
    term_set = set()
    for _, row in df.iterrows():
        term = str(row.get(col_term) or "").strip()
        if not term:
            continue
        term_lower = term.lower()
        if term_lower in term_set:
            continue
        flow = _safe_float(row.get(col_flow)) if col_flow else 0.0
        if min_flow and flow < min_flow:
            continue
        ppc = _safe_float(row.get(col_ppc)) if col_ppc else 0.0
        label = str(row.get(col_label) or "").strip() if col_label else ""
        records.append(
            {
                "term": term,
                "term_lower": term_lower,
                "flow": flow,
                "ppc": ppc,
                "label": label,
            }
        )
        term_set.add(term_lower)
    if col_flow:
        records.sort(key=lambda r: r.get("flow", 0), reverse=True)
    return records, term_set, None


def _save_setting_direct(conn, key, value):
    conn.execute("INSERT OR REPLACE INTO system_logs (key, value) VALUES (?, ?)", (key, str(value)))


def _calc_campaign_window_metrics(conn, start_date, end_date, allowed_campaign_ids=None):
    try:
        df = pd.read_sql(
            f"""
            SELECT campaign_id, SUM(cost) as cost, SUM(sales) as sales, SUM(clicks) as clicks, SUM(orders) as orders
            FROM campaign_reports
            WHERE date >= '{start_date}' AND date <= '{end_date}'
            GROUP BY campaign_id
            """,
            conn,
        )
    except Exception:
        return {"cost": 0.0, "sales": 0.0, "clicks": 0.0, "orders": 0.0, "acos_pct": None}

    if df.empty:
        return {"cost": 0.0, "sales": 0.0, "clicks": 0.0, "orders": 0.0, "acos_pct": None}

    df["campaign_id"] = df["campaign_id"].fillna("").astype(str)
    for col in ["cost", "sales", "clicks", "orders"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    if allowed_campaign_ids is not None:
        allowed_set = {str(c).strip() for c in allowed_campaign_ids if str(c).strip()}
        df = df[df["campaign_id"].isin(allowed_set)]
        if df.empty:
            return {"cost": 0.0, "sales": 0.0, "clicks": 0.0, "orders": 0.0, "acos_pct": None}

    cost = float(df["cost"].sum())
    sales = float(df["sales"].sum())
    clicks = float(df["clicks"].sum())
    orders = float(df["orders"].sum())
    acos_pct = (cost / sales * 100.0) if sales > 0 else None
    return {"cost": cost, "sales": sales, "clicks": clicks, "orders": orders, "acos_pct": acos_pct}


def _evaluate_scale_up_gate(conn, today, allowed_campaign_ids, target_acos_pct, stop_loss):
    start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    perf = _calc_campaign_window_metrics(conn, start_date, end_date, allowed_campaign_ids)
    cost = float(perf.get("cost") or 0.0)
    sales = float(perf.get("sales") or 0.0)
    clicks = float(perf.get("clicks") or 0.0)
    orders = float(perf.get("orders") or 0.0)
    acos_pct = perf.get("acos_pct")
    acos_text = f"{acos_pct:.1f}%" if acos_pct is not None else "无销售"

    summary = f"近7天 花费${cost:.2f} 销售${sales:.2f} ACOS {acos_text} 点击{int(clicks)} 订单{int(orders)}"

    # 数据不足：先不放量，避免噪声误触发
    if clicks < 12 and cost < max(8.0, stop_loss * 0.6):
        return False, f"放量暂停（数据不足）| {summary}"

    # 无销售：统一不放量（高花费/高点击标注为无转化）
    if sales <= 0:
        if cost >= stop_loss or clicks >= 15:
            return False, f"放量暂停（无转化）| {summary}"
        return False, f"放量暂停（尚无销售）| {summary}"

    # 有销售但 ACOS 高于目标：禁止放量
    if acos_pct is not None and acos_pct > target_acos_pct * 1.05:
        return False, f"放量暂停（ACOS偏高）| {summary}"

    # 其余情况认为可放量
    return True, f"放量开启（表现达标）| {summary}"


def _extract_bucket_key(*texts):
    for text in texts:
        raw = str(text or "").strip()
        if not raw:
            continue
        m = re.search(r"([0-9]{3,6}[A-Za-z]?)", raw)
        if m:
            return m.group(1).upper()
    return ""


def _detect_keyword_group_match_type(name):
    n = str(name or "").strip().lower()
    if not n:
        return ""
    if ("精准" in n) or ("exact" in n):
        return "EXACT"
    if ("词组" in n) or ("phrase" in n):
        return "PHRASE"
    if ("广泛" in n) or ("宽泛" in n) or ("broad" in n):
        return "BROAD"
    return ""


def _build_keyword_harvest_routes(ad_groups, campaign_name_map):
    routes = {}
    if ad_groups is None or ad_groups.empty:
        return routes

    seen = set()
    for _, row in ad_groups.iterrows():
        ad_group_id = str(row.get("ad_group_id") or "").strip()
        campaign_id = str(row.get("campaign_id") or "").strip()
        ad_group_name = str(row.get("ad_group_name") or "").strip()
        if not ad_group_id or not campaign_id:
            continue
        match_type = _detect_keyword_group_match_type(ad_group_name)
        if not match_type:
            continue
        bucket = _extract_bucket_key(ad_group_name, campaign_name_map.get(campaign_id, ""))
        if not bucket:
            continue
        key = (bucket, match_type, campaign_id, ad_group_id)
        if key in seen:
            continue
        seen.add(key)
        routes.setdefault(bucket, {}).setdefault(match_type, []).append(
            {
                "campaign_id": campaign_id,
                "ad_group_id": ad_group_id,
                "ad_group_name": ad_group_name,
            }
        )
    return routes


def _run_continuous_learning(conn, logs, today, allowed_campaign_ids, base_target_acos, base_max_bid, base_stop_loss):
    if not _get_bool_setting(AUTO_AI_LEARNING_ENABLED_KEY, True):
        return

    today_key = today.strftime("%Y-%m-%d")
    learned_date = str(get_system_value(AUTO_AI_LEARNING_LAST_DATE_KEY) or "").strip()
    if learned_date == today_key:
        return

    learning_rate = _get_float_setting(AUTO_AI_LEARNING_RATE_KEY, 1.0)
    learning_rate = max(0.5, min(learning_rate, 2.0))

    recent_start = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    recent_end = (today - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_start = (today - timedelta(days=14)).strftime("%Y-%m-%d")
    prev_end = (today - timedelta(days=8)).strftime("%Y-%m-%d")
    recent = _calc_campaign_window_metrics(conn, recent_start, recent_end, allowed_campaign_ids)
    prev = _calc_campaign_window_metrics(conn, prev_start, prev_end, allowed_campaign_ids)

    target_acos = _get_float_setting(AUTO_AI_TARGET_ACOS_KEY, float(base_target_acos))
    max_bid = _get_float_setting(AUTO_AI_MAX_BID_KEY, float(base_max_bid))
    max_up_pct_raw = _get_float_setting(AUTO_AI_MAX_UP_PCT_KEY, 100.0)
    if max_up_pct_raw <= 3:
        max_up_pct = max_up_pct_raw * 100.0
    else:
        max_up_pct = max_up_pct_raw
    stop_loss = _get_float_setting(AUTO_AI_STOP_LOSS_KEY, float(base_stop_loss))
    target_acos = max(10.0, min(60.0, target_acos))
    max_bid = max(MIN_BID, min(10.0, max_bid))
    max_up_pct = max(10.0, min(300.0, max_up_pct))
    stop_loss = max(1.0, min(100.0, stop_loss))

    tighten = 0
    loosen = 0
    reasons = []
    recent_acos = recent.get("acos_pct")
    prev_acos = prev.get("acos_pct")

    if recent["sales"] > 0 and recent_acos is not None:
        if recent_acos > target_acos * 1.2:
            tighten += 2
            reasons.append(f"近7天ACOS {recent_acos:.1f}% 高于目标")
        elif recent_acos > target_acos * 1.05:
            tighten += 1
            reasons.append(f"近7天ACOS {recent_acos:.1f}% 略高于目标")
        elif recent_acos < target_acos * 0.75 and recent["orders"] >= 3:
            loosen += 2
            reasons.append(f"近7天ACOS {recent_acos:.1f}% 明显优于目标")
        elif recent_acos < target_acos * 0.9 and recent["orders"] >= 2:
            loosen += 1
            reasons.append(f"近7天ACOS {recent_acos:.1f}% 低于目标")
    elif recent["cost"] >= stop_loss and recent["sales"] <= 0:
        tighten += 2
        reasons.append(f"近7天花费 ${recent['cost']:.2f} 无销售")
    elif recent["cost"] >= stop_loss * 0.6 and recent["clicks"] >= 10 and recent["sales"] <= 0:
        tighten += 1
        reasons.append("近7天点击较高但无销售")

    if prev_acos is not None and recent_acos is not None:
        if recent_acos > prev_acos * 1.15:
            tighten += 1
            reasons.append(f"ACOS较前7天上升 {prev_acos:.1f}%-> {recent_acos:.1f}%")
        elif recent_acos < prev_acos * 0.85 and recent["orders"] >= prev["orders"]:
            loosen += 1
            reasons.append(f"ACOS较前7天下降 {prev_acos:.1f}%-> {recent_acos:.1f}%")

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    recent_acos_text = f"{recent_acos:.1f}%" if recent_acos is not None else "无销售"
    prev_acos_text = f"{prev_acos:.1f}%" if prev_acos is not None else "-"

    if recent["clicks"] < 20 and recent["cost"] < 10:
        note = (
            f"{today_key} | 学习跳过: 数据不足 "
            f"(近7天点击{int(recent['clicks'])}, 花费${recent['cost']:.2f}, ACOS {recent_acos_text})"
        )
        _save_setting_direct(conn, AUTO_AI_LEARNING_LAST_DATE_KEY, today_key)
        _save_setting_direct(conn, AUTO_AI_LEARNING_NOTE_KEY, note)
        logs.append(
            {
                "时间": ts,
                "广告": "系统",
                "类型": "学习",
                "动作": "AI 持续学习",
                "原值": max_bid,
                "新值": max_bid,
                "原因": note,
            }
        )
        conn.execute(
            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
            (ts, "系统", "AI 持续学习", max_bid, max_bid, note, "跳过"),
        )
        return

    if tighten == loosen:
        reason = " | ".join(reasons[:3]) if reasons else "信号平衡"
        note = (
            f"{today_key} | 学习保持: 参数不变 "
            f"(近7天ACOS {recent_acos_text}, 前7天ACOS {prev_acos_text}) | {reason}"
        )
        _save_setting_direct(conn, AUTO_AI_LEARNING_LAST_DATE_KEY, today_key)
        _save_setting_direct(conn, AUTO_AI_LEARNING_NOTE_KEY, note)
        logs.append(
            {
                "时间": ts,
                "广告": "系统",
                "类型": "学习",
                "动作": "AI 持续学习",
                "原值": max_bid,
                "新值": max_bid,
                "原因": note,
            }
        )
        conn.execute(
            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
            (ts, "系统", "AI 持续学习", max_bid, max_bid, note, "保持"),
        )
        return

    bias = abs(tighten - loosen)
    direction = -1 if tighten > loosen else 1
    target_delta = 1.0 * learning_rate * bias * direction
    bid_multiplier = 1.0 + 0.04 * learning_rate * bias * direction
    up_multiplier = 1.0 + 0.08 * learning_rate * bias * direction
    stop_multiplier = 1.0 + 0.05 * learning_rate * bias * direction

    new_target_acos = max(10.0, min(60.0, round(target_acos + target_delta, 1)))
    new_max_bid = max(MIN_BID, min(10.0, round(max_bid * bid_multiplier, 2)))
    new_max_up_pct = max(10.0, min(300.0, round(max_up_pct * up_multiplier, 1)))
    new_stop_loss = max(1.0, min(100.0, round(stop_loss * stop_multiplier, 1)))

    changed = (
        abs(new_target_acos - target_acos) >= 0.1
        or abs(new_max_bid - max_bid) >= 0.01
        or abs(new_max_up_pct - max_up_pct) >= 0.1
        or abs(new_stop_loss - stop_loss) >= 0.1
    )
    direction_label = "收紧" if direction < 0 else "放宽"
    reason = " | ".join(reasons[:3]) if reasons else "综合表现调整"
    if changed:
        _save_setting_direct(conn, AUTO_AI_TARGET_ACOS_KEY, new_target_acos)
        _save_setting_direct(conn, AUTO_AI_MAX_BID_KEY, new_max_bid)
        _save_setting_direct(conn, AUTO_AI_MAX_UP_PCT_KEY, new_max_up_pct)
        _save_setting_direct(conn, AUTO_AI_STOP_LOSS_KEY, new_stop_loss)
        note = (
            f"{today_key} | 学习{direction_label}: "
            f"ACOS {target_acos:.1f}->{new_target_acos:.1f}, "
            f"MaxBid {max_bid:.2f}->{new_max_bid:.2f}, "
            f"Up% {max_up_pct:.1f}->{new_max_up_pct:.1f}, "
            f"StopLoss {stop_loss:.1f}->{new_stop_loss:.1f} | "
            f"近7天ACOS {recent_acos_text}, 前7天ACOS {prev_acos_text} | {reason}"
        )
        status = "已学习"
    else:
        note = (
            f"{today_key} | 学习保持: 调整幅度过小 "
            f"(近7天ACOS {recent_acos_text}, 前7天ACOS {prev_acos_text}) | {reason}"
        )
        status = "保持"
        new_max_bid = max_bid

    _save_setting_direct(conn, AUTO_AI_LEARNING_LAST_DATE_KEY, today_key)
    _save_setting_direct(conn, AUTO_AI_LEARNING_NOTE_KEY, note)
    logs.append(
        {
            "时间": ts,
            "广告": "系统",
            "类型": "学习",
            "动作": "AI 持续学习",
            "原值": max_bid,
            "新值": new_max_bid,
            "原因": note,
        }
    )
    conn.execute(
        "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
        (ts, "系统", "AI 持续学习", max_bid, new_max_bid, note, status),
    )

def _extract_campaign_budget_fields(campaign):
    budget = campaign.get("budget")
    budget_type = "DAILY"
    if isinstance(budget, dict):
        budget_type = str(budget.get("budgetType") or budget.get("type") or "DAILY").strip() or "DAILY"
        budget = budget.get("budget")
    elif isinstance(budget, (int, float)):
        budget = float(budget)
    else:
        budget = campaign.get("dailyBudget", campaign.get("daily_budget", 0))
    try:
        budget_val = float(budget or 0)
    except Exception:
        budget_val = 0.0
    return budget_val, budget_type


def _load_today_campaign_spend(conn, day_str):
    try:
        df = pd.read_sql(
            """
            SELECT campaign_id, SUM(cost) AS cost
            FROM campaign_reports
            WHERE date = ? AND COALESCE(ad_type, 'SP') = 'SP'
            GROUP BY campaign_id
            """,
            conn,
            params=(day_str,),
        )
    except Exception:
        return {}
    if df.empty:
        return {}
    df["campaign_id"] = df["campaign_id"].fillna("").astype(str)
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce").fillna(0.0)
    return dict(zip(df["campaign_id"], df["cost"]))


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
    today_str = today.strftime("%Y-%m-%d")
    # Best-effort intraday SP campaign sync so budget pacing can react to today's spend speed.
    try:
        sync_campaign_report(
            session,
            headers,
            "SPONSORED_PRODUCTS",
            "spCampaigns",
            ["campaignId", "campaignName", "cost", "sales1d", "clicks", "impressions", "purchases1d"],
            "SP",
            ["sales7d", "sales14d", "sales1d", "sales"],
            ["purchases7d", "purchases14d", "purchases1d", "orders"],
            today_str,
            allow_retry=False,
        )
    except Exception:
        pass
    max_acos = 30.0
    try:
        max_acos_setting = get_system_value("auto_ai_max_acos")
        if max_acos_setting is not None:
            max_acos = float(max_acos_setting)
    except Exception:
        max_acos = 30.0
    min_bid_global = max(MIN_BID, _get_float_setting(AUTO_AI_MIN_BID_KEY, 0.2))
    min_bid_close = max(
        min_bid_global, _get_float_setting(AUTO_AI_MIN_BID_CLOSE_KEY, max(min_bid_global, 0.3))
    )
    min_bid_loose = max(
        min_bid_global, _get_float_setting(AUTO_AI_MIN_BID_LOOSE_KEY, max(min_bid_global, 0.25))
    )
    min_bid_sub = max(min_bid_global, _get_float_setting(AUTO_AI_MIN_BID_SUB_KEY, max(min_bid_global, 0.2)))
    min_bid_comp = max(min_bid_global, _get_float_setting(AUTO_AI_MIN_BID_COMP_KEY, max(min_bid_global, 0.15)))
    baseline_min_bid = max(min_bid_global, _get_float_setting(AUTO_AI_BASELINE_MIN_KEY, 0.5))
    memory_ratio = _get_float_setting(AUTO_AI_MEMORY_FLOOR_RATIO_KEY, 0.9)
    if memory_ratio > 1:
        memory_ratio = memory_ratio / 100.0
    memory_ratio = max(0.0, min(memory_ratio, 1.0))
    max_up_pct_raw = _get_float_setting(AUTO_AI_MAX_UP_PCT_KEY, 100.0)
    if max_up_pct_raw <= 3:
        max_up_pct = max_up_pct_raw
    else:
        max_up_pct = max_up_pct_raw / 100.0
    max_up_pct = max(0.0, min(max_up_pct, 5.0))
    start = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    end = today.strftime("%Y-%m-%d")
    simple_mode = True
    harvest_min_orders = max(1, _get_int_setting(AUTO_AI_HARVEST_MIN_ORDERS_KEY, 1))
    freeze_bid_updates = False
    freeze_bid_reason = ""

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
            freeze_bid_updates = True
            freeze_bid_reason = "ASIN近7天花费数据不可用，保持原竞价投放"

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
            local_max_acos = max(max_acos, target_acos)
            target_acos = target_acos * (1.5 if is_star else 1.0)
            stop_loss = base_stop_loss * (2.0 if is_star else 1.0)
            tag = "⭐" if is_star else "🥔"

            action = "保持"
            reasons = []
            acos_factor = 1.0

            if cost > stop_loss and sales == 0:
                action = "?? ??"
                reasons.append(f"{tag} 7??${cost:.0f}??")
            elif sales > 0 and acos > (local_max_acos / 100):
                action = "?? ??"
                acos_factor = max((local_max_acos / 100) / acos, 0.6)
                reasons.append(f"{tag} ACOS???")
            elif sales > 0 and acos > (target_acos / 100):
                action = "?? ??"
                acos_factor = max((target_acos / 100) / acos, 0.8)
                reasons.append(f"{tag} ACOS??")
            elif sales > 0 and acos < (target_acos / 100 * (1.0 if is_star else 0.8)):
                action = "?? ??"
                acos_factor = 1.1
                reasons.append(f"{tag} ????")

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
        ad_groups = pd.read_sql(
            "SELECT ad_group_id, campaign_id, ad_group_name, default_bid, state FROM ad_group_settings",
            conn,
        )
        campaigns = list_sp_campaigns(session, headers, include_extended=True)

        whitelist = [w.strip() for w in get_auto_ai_campaign_whitelist() if str(w).strip()]
        exclusions = [w.strip() for w in get_auto_ai_campaign_exclusions() if str(w).strip()]
        allowed_campaign_ids = None
        campaign_name_map = {}
        if whitelist:
            whitelist_set = {str(w).strip() for w in whitelist if str(w).strip()}
            exclusion_set = {str(w).strip() for w in exclusions if str(w).strip()}
            for c in campaigns:
                campaign_id = c.get("campaignId", c.get("campaign_id"))
                if campaign_id is None:
                    continue
                campaign_name_map[str(campaign_id)] = str(c.get("name", "")).strip()
            allowed_campaign_ids = {
                cid
                for cid, name in campaign_name_map.items()
                if (cid in whitelist_set or name in whitelist_set)
                and cid not in exclusion_set
                and name not in exclusion_set
            }
            if not allowed_campaign_ids:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                reason = f"未匹配到白名单活动: {', '.join(whitelist)}"
                logs.append(
                    {
                        "时间": ts,
                        "广告": "系统",
                        "类型": "自动驾驶",
                        "动作": "白名单过滤",
                        "原价": 0,
                        "新价": 0,
                        "理由": reason,
                    }
                )
                with db_write_lock():
                    conn.execute(
                        "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                        (ts, "系统", "白名单过滤", 0, 0, reason, "失败"),
                    )
                    conn.commit()
                conn.close()
                return logs

        scale_up_allowed = True
        scale_up_prefix = f"极简模式：仅执行 ACOS 调价 + 精准收词（订单≥{harvest_min_orders}）"
        gate_allowed, gate_reason = _evaluate_scale_up_gate(
            conn,
            today,
            allowed_campaign_ids,
            float(base_target_acos),
            float(base_stop_loss),
        )
        if simple_mode:
            scale_up_allowed = gate_allowed
            scale_up_reason = f"{scale_up_prefix} | {gate_reason}"
        else:
            scale_up_allowed = gate_allowed
            scale_up_reason = gate_reason
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logs.append(
            {
                "时间": ts,
                "广告": "系统",
                "类型": "放量门控",
                "动作": "自动放量开关",
                "原值": 0,
                "新值": 1 if scale_up_allowed else 0,
                "原因": scale_up_reason,
            }
        )
        with db_write_lock():
            conn.execute(
                "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                (
                    ts,
                    "系统",
                    "自动放量开关",
                    0,
                    1 if scale_up_allowed else 0,
                    scale_up_reason,
                    "开启" if scale_up_allowed else "暂停",
                ),
            )
            conn.commit()

        pool_enabled_setting = get_system_value(AUTO_KEYWORD_POOL_ENABLED_KEY)
        pool_enabled = False
        if pool_enabled_setting is not None:
            pool_enabled = str(pool_enabled_setting).strip().lower() in ["1", "true", "yes", "on"]
        if simple_mode:
            pool_enabled = False
        pool_path = get_system_value(AUTO_KEYWORD_POOL_PATH_KEY)
        if not pool_path:
            pool_path = os.path.join(str(BASE_DIR), "prd_docs", "amazon-keyword-clean", "output.csv")
        pool_daily_max = max(0, _get_int_setting(AUTO_KEYWORD_POOL_DAILY_MAX_KEY, 20))
        pool_neg_clicks = max(0, _get_int_setting(AUTO_KEYWORD_POOL_NEG_CLICKS_KEY, 8))
        pool_neg_orders = max(0, _get_int_setting(AUTO_KEYWORD_POOL_NEG_ORDERS_KEY, 0))
        pool_min_flow = max(0.0, _get_float_setting(AUTO_KEYWORD_POOL_MIN_FLOW_KEY, 0.0))
        pool_records = []
        pool_terms = set()
        if pool_enabled and allowed_campaign_ids:
            pool_records, pool_terms, pool_err = _load_keyword_pool(pool_path, pool_min_flow)
            if pool_err:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                reason = f"关键词池不可用: {pool_err}"
                logs.append(
                    {
                        "时间": ts,
                        "广告": "系统",
                        "类型": "投词",
                        "动作": "关键词池",
                        "原值": 0,
                        "新值": 0,
                        "原因": reason,
                    }
                )
                with db_write_lock():
                    conn.execute(
                        "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                        (ts, "系统", "关键词池", 0, 0, reason, "失败"),
                    )
                    conn.commit()
                pool_records = []
                pool_terms = set()
        else:
            pool_records = []
            pool_terms = set()

        if whitelist and campaigns:
            spend_map = _load_today_campaign_spend(conn, today_str)
            now_local = datetime.now()
            elapsed_hours = now_local.hour + (now_local.minute / 60.0)
            pace_hours = 22.0
            elapsed_ratio = max(0.03, min(1.0, elapsed_hours / pace_hours))
            if spend_map:
                enabled_campaign_states = {"ENABLED", "ENABLED_WITH_PENDING_CHANGES"}
                for campaign in campaigns:
                    campaign_id = str(campaign.get("campaignId", campaign.get("campaign_id")) or "").strip()
                    if not campaign_id:
                        continue
                    if allowed_campaign_ids is not None and campaign_id not in allowed_campaign_ids:
                        continue
                    state = str(campaign.get("state", "") or "").upper()
                    if state and state not in enabled_campaign_states:
                        continue
                    old_budget, budget_type = _extract_campaign_budget_fields(campaign)
                    if old_budget <= 0:
                        continue
                    spent_today = float(spend_map.get(campaign_id, -1))
                    if spent_today < 0:
                        continue

                    expected_spend = max(0.01, old_budget * elapsed_ratio)
                    fast_threshold = expected_spend * 1.12
                    slow_threshold = expected_spend * 0.85
                    budget_step = 0.0
                    pace_note = ""

                    if spent_today > fast_threshold:
                        over_ratio = min(0.8, (spent_today / expected_spend) - 1.0)
                        budget_step = -min(0.12, max(0.03, over_ratio * 0.25))
                        pace_note = "花费偏快，微降预算"
                    elif spent_today < slow_threshold and elapsed_ratio < 0.98:
                        under_ratio = min(0.8, 1.0 - (spent_today / expected_spend))
                        budget_step = min(0.12, max(0.03, under_ratio * 0.20))
                        pace_note = "花费偏慢，微提预算"
                    elif spent_today >= old_budget * 0.98 and elapsed_ratio < 0.95:
                        budget_step = 0.08
                        pace_note = "预算接近耗尽，提前补量"

                    if abs(budget_step) < 1e-9:
                        continue

                    new_budget = old_budget * (1.0 + budget_step)
                    min_budget = max(1.0, old_budget * 0.75)
                    max_budget = old_budget * 1.25
                    new_budget = round(max(min_budget, min(max_budget, new_budget)), 2)
                    if abs(new_budget - old_budget) < 0.05:
                        continue

                    campaign_name = str(campaign.get("name", campaign.get("campaignName", campaign_id)) or campaign_id)
                    reason = (
                        f"22小时控速: {pace_note}; 今日花费 ${spent_today:.2f}, "
                        f"当前时段应花 ${expected_spend:.2f} (已过 {elapsed_hours:.1f}h/22h)"
                    )
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    logs.append(
                        {
                            "时间": ts,
                            "广告": campaign_name,
                            "类型": "预算",
                            "动作": "SP 活动预算",
                            "原价": old_budget,
                            "新价": new_budget,
                            "理由": reason,
                        }
                    )
                    status = "模拟"
                    if is_live_mode:
                        ok, err = update_campaign_budget(
                            session,
                            headers,
                            "SP",
                            campaign_id,
                            new_budget,
                            budget_type,
                        )
                        status = "成功" if ok else "失败"
                        if not ok and err:
                            reason = f"{reason} | {err}"
                    with db_write_lock():
                        conn.execute(
                            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                            (ts, campaign_name, "SP 活动预算", old_budget, new_budget, reason, status),
                        )
                        conn.commit()
            else:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                reason = "22小时控速：今天还没有可用的 SP 花费数据，暂不调预算"
                logs.append(
                    {
                        "时间": ts,
                        "广告": "系统",
                        "类型": "预算",
                        "动作": "SP 活动预算",
                        "原价": 0,
                        "新价": 0,
                        "理由": reason,
                    }
                )
                with db_write_lock():
                    conn.execute(
                        "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                        (ts, "系统", "SP 活动预算", 0, 0, reason, "跳过"),
                    )
                    conn.commit()

        enabled_states = ["ENABLED", "ENABLED_WITH_PENDING_CHANGES"]
        ad_group_campaign = {}
        enabled_ad_groups = None
        ad_group_default_bid = {}
        ad_group_name_map = {}
        keyword_harvest_routes = {}
        if not ad_groups.empty:
            ad_groups["ad_group_id"] = ad_groups["ad_group_id"].fillna("").astype(str)
            ad_groups["campaign_id"] = ad_groups["campaign_id"].fillna("").astype(str)
            if "ad_group_name" not in ad_groups.columns:
                ad_groups["ad_group_name"] = ""
            ad_groups["ad_group_name"] = ad_groups["ad_group_name"].fillna("").astype(str)
            ad_groups["default_bid"] = ad_groups["default_bid"].fillna(0.0)
            groups_state = ad_groups["state"].fillna("").astype(str).str.upper()
            ad_groups = ad_groups[(groups_state == "") | (groups_state.isin(enabled_states))]
            if allowed_campaign_ids is not None:
                ad_groups = ad_groups[ad_groups["campaign_id"].isin(allowed_campaign_ids)]
            ad_group_campaign = dict(zip(ad_groups["ad_group_id"], ad_groups["campaign_id"]))
            ad_group_default_bid = dict(zip(ad_groups["ad_group_id"], ad_groups["default_bid"]))
            ad_group_name_map = dict(zip(ad_groups["ad_group_id"], ad_groups["ad_group_name"]))
            enabled_ad_groups = set(ad_groups["ad_group_id"].tolist())
            keyword_harvest_routes = _build_keyword_harvest_routes(ad_groups, campaign_name_map)

        campaign_rules = {}
        if allowed_campaign_ids is not None:
            try:
                campaign_perf = pd.read_sql(
                    f"""
                    SELECT campaign_id, SUM(cost) as cost, SUM(sales) as sales
                    FROM campaign_reports
                    WHERE date >= '{start}' AND date <= '{end}'
                    GROUP BY campaign_id
                    """,
                    conn,
                )
            except Exception:
                campaign_perf = pd.DataFrame()
            if not campaign_perf.empty:
                campaign_perf["campaign_id"] = campaign_perf["campaign_id"].fillna("").astype(str)
                campaign_perf["cost"] = campaign_perf["cost"].fillna(0.0)
                campaign_perf["sales"] = campaign_perf["sales"].fillna(0.0)
                for _, row in campaign_perf.iterrows():
                    campaign_id = str(row.get("campaign_id") or "").strip()
                    if not campaign_id or campaign_id not in allowed_campaign_ids:
                        continue
                    cost = float(row.get("cost") or 0)
                    sales = float(row.get("sales") or 0)
                    if cost <= 0 and sales <= 0:
                        continue
                    target_acos = base_target_acos
                    local_max_acos = max(max_acos, target_acos)
                    action = "保持"
                    reasons = []
                    acos_factor = 1.0
                    if cost > base_stop_loss * 0.8 and sales <= 0:
                        action = "🛑 止损"
                        reasons.append(f"活动7天花费${cost:.0f}无单")
                    elif sales > 0:
                        acos = cost / sales if sales else 0
                        if acos > (local_max_acos / 100):
                            action = "?? ??"
                            acos_factor = max((local_max_acos / 100) / acos, 0.6)
                            reasons.append("??ACOS???")
                        elif acos > (target_acos / 100 * 1.1):
                            action = "?? ??"
                            acos_factor = max((target_acos / 100) / acos, 0.75)
                            reasons.append("??ACOS??")
                        elif acos < (target_acos / 100 * 0.9):
                            action = "?? ??"
                            acos_factor = 1.1
                            reasons.append("??????")
                    if action == "保持" and acos_factor == 1.0:
                        continue
                    reason = " | ".join(reasons) if reasons else "活动层级规则"
                    campaign_rules[campaign_id] = {
                        "asin": f"CAMP:{campaign_id}",
                        "sku": "",
                        "action": action,
                        "reason": f"活动层级: {reason}",
                        "factor": acos_factor,
                        "tag": "🎯",
                      "target_acos": target_acos,
                  }
        traffic_stats = {}
        traffic_rules = {}
        if allowed_campaign_ids is not None:
            traffic_end = (today - timedelta(days=1)).strftime("%Y-%m-%d")
            traffic_start = (today - timedelta(days=3)).strftime("%Y-%m-%d")
            try:
                traffic_perf = pd.read_sql(
                    f"""
                    SELECT campaign_id, SUM(clicks) as clicks, SUM(impressions) as impressions,
                           SUM(cost) as cost, SUM(sales) as sales
                    FROM campaign_reports
                    WHERE date >= '{traffic_start}' AND date <= '{traffic_end}'
                    GROUP BY campaign_id
                    """,
                    conn,
                )
            except Exception:
                traffic_perf = pd.DataFrame()
            if not traffic_perf.empty:
                traffic_perf["campaign_id"] = traffic_perf["campaign_id"].fillna("").astype(str)
                traffic_perf["clicks"] = traffic_perf["clicks"].fillna(0.0)
                traffic_perf["impressions"] = traffic_perf["impressions"].fillna(0.0)
                traffic_perf["cost"] = traffic_perf["cost"].fillna(0.0)
                traffic_perf["sales"] = traffic_perf["sales"].fillna(0.0)
                for _, row in traffic_perf.iterrows():
                    cid = str(row.get("campaign_id") or "").strip()
                    if not cid or cid not in allowed_campaign_ids:
                        continue
                    clicks = float(row.get("clicks") or 0)
                    impressions = float(row.get("impressions") or 0)
                    cost = float(row.get("cost") or 0)
                    sales = float(row.get("sales") or 0)
                    traffic_stats[cid] = {
                        "clicks": clicks,
                        "impressions": impressions,
                        "cost": cost,
                        "sales": sales,
                    }
                    acos = cost / sales if sales > 0 else None
                    ctr = clicks / impressions if impressions > 0 else 0.0
                    reasons = []
                    factor = 1.0
                    max_factor = None

                    if impressions <= 0:
                        factor = 2.0
                        reasons.append("近3天0曝光")
                    elif impressions >= 200 and clicks <= 0:
                        factor = 1.5
                        reasons.append(f"近3天曝光{impressions:.0f} 点击0")
                    elif impressions >= 800 and clicks < 3:
                        factor = 1.2
                        reasons.append(f"近3天曝光{impressions:.0f} 点击{clicks:.0f}")

                    if clicks >= 5 and sales > 0 and acos is not None and acos <= (base_target_acos / 100 * 0.9):
                        if factor < 1.15:
                            factor = 1.15
                        reasons.append(f"近3天ACOS {acos*100:.1f}% 低于目标")

                    if sales > 0 and acos is not None:
                        if acos >= (max_acos / 100):
                            factor = min(factor, 0.85)
                            reasons.append(f"近3天ACOS {acos*100:.1f}% ≥ 上限{max_acos:.1f}%")
                        elif acos >= (base_target_acos / 100):
                            factor = min(factor, 0.9)
                            reasons.append(f"近3天ACOS {acos*100:.1f}% ≥ 目标{base_target_acos:.1f}%")
                    elif sales <= 0 and impressions >= 1000 and clicks >= 15:
                        factor = min(factor, 0.75)
                        reasons.append(f"近3天点击{clicks:.0f}无转化")
                    elif sales <= 0 and impressions >= 500 and clicks >= 8:
                        factor = min(factor, 0.85)
                        reasons.append(f"近3天点击{clicks:.0f}无转化")

                    if impressions >= 500 and ctr >= 0.005:
                        max_factor = 1.0
                        reasons.append(f"CTR {ctr*100:.2f}% 充足")
                    if sales > 0 and acos is not None and acos >= (base_target_acos / 100):
                        max_factor = 1.0 if max_factor is None else min(max_factor, 1.0)
                        reasons.append(f"ACOS {acos*100:.1f}% ≥ 目标{base_target_acos:.1f}%")

                    if reasons or max_factor is not None or factor != 1.0:
                        rule = {"factor": factor, "reason": " | ".join(reasons)}
                        rule["force_floor"] = True if rule["factor"] > 1.0 else False
                        rule["max_up_pct"] = min(max_up_pct * 2.0, 5.0) if rule["factor"] > 1.0 else None
                        rule["max_factor"] = max_factor
                        traffic_rules[cid] = rule

        if not ads.empty:
            ads["asin"] = ads["asin"].fillna("").astype(str).str.strip()
            ads["sku"] = ads["sku"].fillna("").astype(str).str.strip()
            ads_state = ads["state"].fillna("").astype(str).str.upper()
            ads = ads[(ads_state == "") | (ads_state.isin(enabled_states))]

        ad_group_map = {}
        for ad_group_id, group in ads.groupby("ad_group_id"):
            if not ad_group_id:
                continue
            if enabled_ad_groups is not None and str(ad_group_id) not in enabled_ad_groups:
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

        if campaign_rules and ad_group_campaign:
            fallback_groups = enabled_ad_groups if enabled_ad_groups is not None else ad_group_campaign.keys()
            for ad_group_id in fallback_groups:
                ad_group_id = str(ad_group_id)
                if ad_group_id in ad_group_map:
                    continue
                campaign_id = str(ad_group_campaign.get(ad_group_id) or "")
                if not campaign_id:
                    continue
                rule = campaign_rules.get(campaign_id)
                if rule:
                    ad_group_map[ad_group_id] = rule

        if allowed_campaign_ids is not None and ad_group_campaign:
            fallback_groups = enabled_ad_groups if enabled_ad_groups is not None else ad_group_campaign.keys()
            for ad_group_id in fallback_groups:
                ad_group_id = str(ad_group_id)
                if ad_group_id in ad_group_map:
                    continue
                campaign_id = str(ad_group_campaign.get(ad_group_id) or "")
                if not campaign_id:
                    continue
                stats = traffic_stats.get(campaign_id, {})
                clicks = float(stats.get("clicks", 0) or 0)
                impressions = float(stats.get("impressions", 0) or 0)
                action = "补足底价"
                factor = 1.0
                reasons = []
                if impressions <= 0:
                    action = "🚀 冷启动"
                    factor = 2.0
                    reasons.append("近3天0曝光")
                elif clicks <= 0:
                    action = "🚀 冷启动"
                    factor = 1.5
                    reasons.append(f"近3天曝光{impressions:.0f} 点击0")
                elif clicks < 3 and impressions < 500:
                    action = "🚀 冷启动"
                    factor = 1.2
                    reasons.append(f"近3天曝光{impressions:.0f} 点击{clicks:.0f}")
                if not reasons:
                    reasons.append("记忆价保护")
                reason = " | ".join(reasons)
                ad_group_map[ad_group_id] = {
                    "asin": f"COLD:{campaign_id}",
                    "sku": "",
                    "action": action,
                    "reason": f"冷启动: {reason}",
                    "factor": factor,
                    "tag": "❄️",
                    "target_acos": base_target_acos,
                    "force_floor": True,
                    "max_up_pct": min(max_up_pct * 2.0, 5.0),
                }

        if traffic_rules and ad_group_campaign:
            for ad_group_id, rule in list(ad_group_map.items()):
                if rule.get("action") == "🛑 止损":
                    continue
                campaign_id = str(ad_group_campaign.get(ad_group_id) or "")
                if not campaign_id:
                    continue
                traffic_rule = traffic_rules.get(campaign_id)
                if not traffic_rule:
                    continue
                factor = float(rule.get("factor", 1.0) or 1.0) * float(traffic_rule.get("factor", 1.0) or 1.0)
                max_factor = traffic_rule.get("max_factor")
                if max_factor is not None and factor > max_factor:
                    factor = max_factor
                rule["factor"] = factor
                reason = rule.get("reason") or ""
                traffic_reason = traffic_rule.get("reason") or ""
                if traffic_reason:
                    rule["reason"] = f"{reason} | 流量: {traffic_reason}" if reason else f"流量: {traffic_reason}"
                if traffic_rule.get("force_floor"):
                    rule["force_floor"] = True
                if traffic_rule.get("max_up_pct"):
                    rule["max_up_pct"] = max(rule.get("max_up_pct", 0) or 0, traffic_rule.get("max_up_pct") or 0)

        if not scale_up_allowed:
            gate_note = "放量暂停：仅允许维持或降价"
            for _, rule in ad_group_map.items():
                rule["freeze_up"] = True
                # Keep floor recovery active so very low bids can recover from penny-bid lock.
                rule["freeze_floor"] = False
                rule["force_floor"] = False
                rule["max_up_pct"] = None
                factor = float(rule.get("factor", 1.0) or 1.0)
                if factor > 1.0:
                    rule["factor"] = 1.0
                reason = str(rule.get("reason") or "").strip()
                if gate_note not in reason:
                    rule["reason"] = f"{reason} | {gate_note}" if reason else gate_note

        ad_group_campaign = dict(zip(ad_groups["ad_group_id"], ad_groups["campaign_id"]))
        keywords = list_sp_keywords(session, headers)
        targets = list_sp_targets(session, headers)
        baseline_cache = get_bid_baselines()
        adgroup_samples = {}
        adgroup_auto_samples = {}
        adgroup_kw_samples = {}
        adgroup_tg_samples = {}
        for kw in keywords:
            ad_group_id = str(kw.get("adGroupId") or kw.get("ad_group_id") or "")
            if not ad_group_id:
                continue
            state_flag = str(kw.get("state", "")).upper()
            if state_flag and state_flag not in enabled_states:
                continue
            bid = float(kw.get("bid", 0) or 0)
            if bid <= 0:
                continue
            adgroup_samples.setdefault(ad_group_id, []).append(bid)
            adgroup_kw_samples.setdefault(ad_group_id, []).append(bid)
        for tg in targets:
            ad_group_id = str(tg.get("adGroupId") or tg.get("ad_group_id") or "")
            if not ad_group_id:
                continue
            state_flag = str(tg.get("state", "")).upper()
            if state_flag and state_flag not in enabled_states:
                continue
            bid = float(tg.get("bid", 0) or 0)
            expr_type = str(tg.get("expressionType") or "").upper()
            expr = (
                tg.get("expression")
                or tg.get("resolvedExpression")
                or tg.get("targetingExpression")
                or tg.get("targetingExpressions")
                or tg.get("targetingExpressionList")
            )
            is_auto = False
            if expr_type == "AUTO":
                is_auto = True
            elif isinstance(expr, list):
                for e in expr:
                    if str(e.get("type") or "").upper() == "AUTO":
                        is_auto = True
                        break
            if bid <= 0 and is_auto:
                bid = float(ad_group_default_bid.get(ad_group_id, 0) or 0)
            if bid <= 0:
                continue
            adgroup_samples.setdefault(ad_group_id, []).append(bid)
            adgroup_tg_samples.setdefault(ad_group_id, []).append(bid)
            if is_auto:
                adgroup_auto_samples.setdefault(ad_group_id, []).append(bid)

        def _median(values):
            if not values:
                return 0.0
            vals = sorted(values)
            mid = len(vals) // 2
            if len(vals) % 2 == 1:
                return float(vals[mid])
            return float((vals[mid - 1] + vals[mid]) / 2.0)

        adgroup_floors = {}
        baseline_records = []
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ad_group_name_map = {}
        try:
            ad_group_name_map = dict(zip(ad_groups["ad_group_id"], ad_groups["ad_group_name"]))
        except Exception:
            ad_group_name_map = {}
        all_groups = set(ad_group_default_bid.keys()) | set(adgroup_samples.keys())
        for ad_group_id in all_groups:
            samples = adgroup_samples.get(ad_group_id, [])
            kw_med = _median(adgroup_kw_samples.get(ad_group_id, []))
            tg_med = _median(adgroup_tg_samples.get(ad_group_id, []))
            auto_med = _median(adgroup_auto_samples.get(ad_group_id, []))
            baseline = _median(samples)
            if baseline <= 0:
                baseline = float(baseline_cache.get(ad_group_id, 0) or 0)
            if baseline <= 0:
                baseline = float(ad_group_default_bid.get(ad_group_id, 0) or 0)
            floor = max(min_bid_global, baseline_min_bid)
            if baseline > 0:
                floor = max(floor, baseline * memory_ratio)
            adgroup_floors[ad_group_id] = floor
            if baseline > 0:
                baseline_records.append(
                    {
                        "ad_group_id": ad_group_id,
                        "ad_group_name": ad_group_name_map.get(ad_group_id, ""),
                        "baseline_bid": baseline,
                        "keyword_median": kw_med,
                        "target_median": tg_med,
                        "auto_median": auto_med,
                        "updated_at": now_ts,
                    }
                )
        save_bid_baselines(baseline_records)
        campaigns_for_placement = campaigns
        if allowed_campaign_ids is not None:
            campaigns_for_placement = [
                c
                for c in campaigns
                if str(c.get("campaignId", c.get("campaign_id")) or "") in allowed_campaign_ids
            ]

        keyword_updates = []
        target_updates = []
        rule_stats = {}

        for rule in ad_group_map.values():
            key = (rule["asin"], rule["sku"])
            rule_stats[key] = {
                "keywords": 0,
                "targets": 0,
                "auto_targets": 0,
                "adgroups": 0,
                "placements": 0,
                "reason": rule["reason"],
            }

        def _apply_bid_limits(base_bid, proposed_bid, min_floor, max_bid, allow_floor=True, up_cap=None):
            bid = min(proposed_bid, max_bid)
            if allow_floor:
                bid = max(bid, min_floor)
            cap_ratio = max_up_pct if up_cap is None else up_cap
            floor_rescue = allow_floor and base_bid > 0 and min_floor > base_bid and bid >= min_floor
            if base_bid > 0 and cap_ratio > 0 and bid > base_bid and not floor_rescue:
                cap = base_bid * (1.0 + cap_ratio)
                # Avoid no-op updates when tiny capped increases round back to the same 2-decimal bid.
                cap = max(cap, base_bid + 0.01)
                if bid > cap:
                    bid = cap
            return max(MIN_BID, min(bid, max_bid))

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
            group_floor = adgroup_floors.get(ad_group_id, min_bid_global)
            if rule["action"] == "🛑 止损":
                new_bid = max(MIN_BID, min(min_bid_global, base_max_bid))
            else:
                proposed = old_bid * rule["factor"]
                if rule.get("freeze_up") and proposed > old_bid:
                    proposed = old_bid
                allow_floor = (
                    (not rule.get("freeze_floor"))
                    and (old_bid < group_floor or rule.get("force_floor") or rule["factor"] >= 1.0)
                )
                new_bid = _apply_bid_limits(
                    old_bid,
                    proposed,
                    group_floor,
                    base_max_bid,
                    allow_floor,
                    rule.get("max_up_pct"),
                )
            if abs(new_bid - old_bid) < 0.01:
                continue
            keyword_updates.append({"keywordId": str(keyword_id), "bid": round(new_bid, 2)})
            rule_stats[(rule["asin"], rule["sku"])]["keywords"] += 1

        auto_weights = {
            "CLOSE_MATCH": 1.05,
            "LOOSE_MATCH": 0.95,
            "SUBSTITUTES": 0.9,
            "COMPLEMENTS": 0.85,
            "QUERY_HIGH_REL_MATCHES": 1.05,
            "QUERY_BROAD_REL_MATCHES": 0.95,
            "ASIN_SUBSTITUTE_RELATED": 0.9,
            "ASIN_ACCESSORY_RELATED": 0.85,
        }
        auto_min_floors = {
            "CLOSE_MATCH": min_bid_close,
            "LOOSE_MATCH": min_bid_loose,
            "SUBSTITUTES": min_bid_sub,
            "COMPLEMENTS": min_bid_comp,
            "QUERY_HIGH_REL_MATCHES": min_bid_close,
            "QUERY_BROAD_REL_MATCHES": min_bid_loose,
            "ASIN_SUBSTITUTE_RELATED": min_bid_sub,
            "ASIN_ACCESSORY_RELATED": min_bid_comp,
        }

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
            expr = (
                tg.get("expression")
                or tg.get("resolvedExpression")
                or tg.get("targetingExpression")
                or tg.get("targetingExpressions")
                or tg.get("targetingExpressionList")
            )
            is_auto = False
            auto_predicate = ""
            expr_type = str(tg.get("expressionType") or "").upper()
            if expr_type == "AUTO" and isinstance(expr, list) and expr:
                is_auto = True
                auto_predicate = str(expr[0].get("type") or expr[0].get("predicate") or "").upper()
            elif isinstance(expr, list):
                for e in expr:
                    if str(e.get("type") or "").upper() == "AUTO":
                        is_auto = True
                        auto_predicate = str(e.get("predicate") or "").upper()
                        break

            base_bid = old_bid if old_bid > 0 else float(ad_group_default_bid.get(ad_group_id, 0) or 0)
            if base_bid <= 0:
                continue

            weight = auto_weights.get(auto_predicate, 1.0) if is_auto else 1.0
            group_floor = adgroup_floors.get(ad_group_id, min_bid_global)
            min_floor = auto_min_floors.get(auto_predicate, min_bid_global) if is_auto else min_bid_global
            min_floor = max(min_floor, group_floor)
            if rule["action"] == "🛑 止损":
                new_bid = max(MIN_BID, min(min_bid_global, base_max_bid))
            else:
                proposed = base_bid * rule["factor"] * weight
                if rule.get("freeze_up") and proposed > base_bid:
                    proposed = base_bid
                allow_floor = (
                    (not rule.get("freeze_floor"))
                    and (base_bid < min_floor or rule.get("force_floor") or rule["factor"] >= 1.0)
                )
                new_bid = _apply_bid_limits(
                    base_bid,
                    proposed,
                    min_floor,
                    base_max_bid,
                    allow_floor,
                    rule.get("max_up_pct"),
                )
            compare_bid = base_bid
            if abs(new_bid - compare_bid) < 0.01:
                continue
            update_key = "targetingClauseId" if is_auto else id_key
            target_updates.append({update_key: str(target_id), "bid": round(new_bid, 2)})
            rule_stats[(rule["asin"], rule["sku"])]["targets"] += 1
            if is_auto:
                rule_stats[(rule["asin"], rule["sku"])]["auto_targets"] += 1

        adgroup_updates = []
        for ad_group_id, rule in ad_group_map.items():
            old_bid = float(ad_group_default_bid.get(str(ad_group_id), 0) or 0)
            if old_bid <= 0:
                continue
            group_floor = adgroup_floors.get(str(ad_group_id), min_bid_global)
            if rule["action"] == "🛑 止损":
                new_bid = max(MIN_BID, min(min_bid_global, base_max_bid))
            else:
                proposed = old_bid * rule["factor"]
                if rule.get("freeze_up") and proposed > old_bid:
                    proposed = old_bid
                allow_floor = (
                    (not rule.get("freeze_floor"))
                    and (old_bid < group_floor or rule.get("force_floor") or rule["factor"] >= 1.0)
                )
                new_bid = _apply_bid_limits(
                    old_bid,
                    proposed,
                    group_floor,
                    base_max_bid,
                    allow_floor,
                    rule.get("max_up_pct"),
                )
            if abs(new_bid - old_bid) < 0.01:
                continue
            adgroup_updates.append({"adGroupId": str(ad_group_id), "defaultBid": round(new_bid, 2)})
            rule_stats[(rule["asin"], rule["sku"])]["adgroups"] += 1

        placement_updates = []
        placement_predicates = ["placementTop", "placementProductPage", "placementRestOfSearch"]
        defaults_up = {"placementTop": 20, "placementProductPage": 10, "placementRestOfSearch": 5}
        campaign_bids = {}
        for c in campaigns_for_placement:
            campaign_id = c.get("campaignId", c.get("campaign_id"))
            if not campaign_id:
                continue
            current = {}
            bidding = c.get("bidding") or {}
            adjustments = bidding.get("adjustments") or []
            for adj in adjustments:
                predicate = adj.get("predicate")
                pct = adj.get("percentage")
                if predicate:
                    current[predicate] = pct if pct is not None else 0
            dynamic_bidding = c.get("dynamicBidding") or {}
            placement_bidding = dynamic_bidding.get("placementBidding") or []
            placement_map = {
                "PLACEMENT_TOP": "placementTop",
                "PLACEMENT_PRODUCT_PAGE": "placementProductPage",
                "PLACEMENT_REST_OF_SEARCH": "placementRestOfSearch",
            }
            for adj in placement_bidding:
                placement = str(adj.get("placement") or "").upper()
                predicate = placement_map.get(placement)
                if not predicate:
                    continue
                pct = adj.get("percentage")
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
        adgroup_ok = True
        placement_ok = True
        if freeze_bid_updates:
            keyword_updates = []
            target_updates = []
            adgroup_updates = []
            placement_updates = []
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            reason = freeze_bid_reason or "竞价数据不可用，保持原竞价投放"
            logs.append(
                {
                    "时间": ts,
                    "广告": "系统",
                    "类型": "竞价",
                    "动作": "SP 出价更新",
                    "原价": 0,
                    "新价": 0,
                    "理由": reason,
                }
            )
            with db_write_lock():
                conn.execute(
                    "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                    (ts, "系统", "SP 出价更新", 0, 0, reason, "跳过"),
                )
                conn.commit()
        if is_live_mode:
            for batch in _chunked(keyword_updates, 100):
                ok, _ = update_sp_keyword_bids(session, headers, batch)
                if not ok:
                    keyword_ok = False
            for batch in _chunked(target_updates, 100):
                ok, _ = update_sp_target_bids(session, headers, batch)
                if not ok:
                    target_ok = False
            for batch in _chunked(adgroup_updates, 100):
                ok, _ = update_sp_adgroup_bids(session, headers, batch)
                if not ok:
                    adgroup_ok = False
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
                total_ops = stats["keywords"] + stats["targets"] + stats["adgroups"] + stats["placements"]
                if total_ops <= 0:
                    continue
                status = "模拟"
                if is_live_mode:
                    if keyword_ok and target_ok and adgroup_ok and placement_ok:
                        status = "已执行"
                    elif keyword_ok or target_ok or adgroup_ok or placement_ok:
                        status = "部分失败"
                    else:
                        status = "失败"
                asin, sku = rule_key
                reason = stats["reason"]
                if live_note:
                    reason = f"{reason}{live_note}"
                reason = (
                    f"{reason} | 关键词{stats['keywords']} 投放{stats['targets']}(自动{stats['auto_targets']}) "
                    f"广告组{stats['adgroups']} 广告位{stats['placements']}"
                )
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
        if simple_mode:
            auto_neg_cfg["enabled"] = False
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
                        term_lower = term.lower()
                        if pool_terms and term_lower in pool_terms:
                            continue
                        cost = float(r["cost"] or 0)
                        sales = float(r["sales"] or 0)
                        clicks = float(r["clicks"] or 0)
                        orders = float(r.get("orders") or 0)
                        if cost < spend_threshold or clicks < clicks_threshold:
                            continue
                        if pool_terms:
                            if clicks < max(clicks_threshold, pool_neg_clicks):
                                continue
                            if orders > pool_neg_orders:
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

                    # === AI 语义否词 + 不相关 ASIN 自动否投 ===
                    semantic_enabled = True
                    semantic_clicks = 0
                    semantic_spend = 0.0
                    semantic_max_terms = None
                    semantic_match_type = "NEGATIVE_PHRASE"
                    asin_clicks = 0

                    has_ai = bool(HAS_OPENAI and deepseek_key)
                    positive_terms = []
                    if not agg.empty:
                        pos_df = agg[agg["orders"] > 0].sort_values(["orders", "sales"], ascending=False)
                        for term in pos_df["search_term"].tolist()[:30]:
                            term = str(term).strip()
                            if term:
                                positive_terms.append(term)

                    allowed_asins = set()
                    if not ads.empty:
                        for _, row in ads.iterrows():
                            ad_group_id = str(row.get("ad_group_id") or "")
                            campaign_id = str(ad_group_campaign.get(ad_group_id) or "")
                            if allowed_campaign_ids is not None and campaign_id not in allowed_campaign_ids:
                                continue
                            asin = str(row.get("asin") or "").strip().upper()
                            if asin:
                                allowed_asins.add(asin)

                    existing_neg_targets = set()
                    if allowed_campaign_ids:
                        for cid in allowed_campaign_ids:
                            items = list_sp_negative_targets(session, headers, campaign_id=cid)
                            for item in items or []:
                                expr = item.get("expression") or []
                                asin_val = ""
                                if isinstance(expr, list):
                                    for e in expr:
                                        if str(e.get("type") or "").upper() == "ASIN_SAME_AS":
                                            asin_val = str(e.get("value") or "").strip().upper()
                                            break
                                if asin_val:
                                    existing_neg_targets.add(
                                        (
                                            str(item.get("campaignId") or ""),
                                            str(item.get("adGroupId") or ""),
                                            asin_val,
                                        )
                                    )

                    semantic_payloads = []
                    semantic_records = []
                    asin_payloads = []
                    asin_records = []
                    semantic_checked = 0

                    for _, r in agg.iterrows():
                        term = str(r.get("search_term") or "").strip()
                        if not term:
                            continue
                        term_lower = term.lower()
                        if pool_terms and term_lower in pool_terms:
                            continue
                        campaign_id = str(r.get("campaign_id") or "")
                        ad_group_id = str(r.get("ad_group_id") or "")
                        if not campaign_id:
                            campaign_id = str(ad_group_campaign.get(ad_group_id, "") or "")
                        if not campaign_id or not ad_group_id:
                            continue
                        if allowed_campaign_ids is not None and campaign_id not in allowed_campaign_ids:
                            continue
                        if enabled_ad_groups is not None and ad_group_id not in enabled_ad_groups:
                            continue
                        clicks = float(r.get("clicks") or 0)
                        orders = float(r.get("orders") or 0)
                        cost = float(r.get("cost") or 0)

                        if _is_asin_term(term):
                            asin_term = str(term).strip().upper()
                            if asin_term in allowed_asins:
                                continue
                            if clicks < asin_clicks:
                                continue
                            key = (campaign_id, ad_group_id, asin_term)
                            if key in existing_neg_targets:
                                continue
                            item = {
                                "campaignId": campaign_id,
                                "adGroupId": ad_group_id,
                                "state": "ENABLED",
                                "expressionType": "MANUAL",
                                "expression": [{"type": "ASIN_SAME_AS", "value": asin_term}],
                            }
                            asin_payloads.append(item)
                            existing_neg_targets.add(key)
                            asin_records.append(
                                {
                                    "campaign_id": campaign_id,
                                    "ad_group_id": ad_group_id,
                                    "asin": asin_term,
                                    "expression_type": "MANUAL",
                                    "level": "adgroup",
                                    "source": "AI",
                                    "status": "pending" if is_live_mode else "dry_run",
                                }
                            )
                            continue

                        if not semantic_enabled:
                            continue
                        if orders > 0 or clicks < semantic_clicks or cost < semantic_spend:
                            continue
                        if _is_protected_term(term, protect_terms, protect_mode):
                            continue
                        if _quick_relevant(term, positive_terms):
                            continue
                        if semantic_max_terms is not None and semantic_checked >= semantic_max_terms:
                            continue
                        if not has_ai or not positive_terms:
                            continue
                        relevant, ai_reason = deepseek_relevance(
                            deepseek_key,
                            campaign_name_map.get(campaign_id, ""),
                            positive_terms,
                            term,
                        )
                        semantic_checked += 1
                        if relevant is None or relevant:
                            continue
                        key = (campaign_id, ad_group_id, term.lower(), semantic_match_type)
                        if key in existing:
                            continue
                        semantic_payloads.append(
                            {
                                "campaignId": campaign_id,
                                "adGroupId": ad_group_id,
                                "keywordText": term,
                                "matchType": semantic_match_type,
                                "state": "ENABLED",
                            }
                        )
                        existing.add(key)
                        semantic_records.append(
                            {
                                "campaign_id": campaign_id,
                                "ad_group_id": ad_group_id,
                                "keyword_text": term,
                                "match_type": semantic_match_type,
                                "level": "adgroup",
                                "source": "AI",
                                "status": "pending" if is_live_mode else "dry_run",
                                "reason": f"语义不相关: {ai_reason}".strip(),
                                "cost": cost,
                                "sales": float(r.get("sales") or 0),
                                "orders": orders,
                                "clicks": clicks,
                            }
                        )

                    if semantic_records:
                        save_auto_negative_keywords(semantic_records)

                    semantic_status = "模拟"
                    if semantic_payloads and is_live_mode:
                        ok_all = True
                        for batch in _chunked(semantic_payloads, 100):
                            ok, _ = create_sp_negative_keywords(session, headers, batch, campaign_level=False)
                            if not ok:
                                ok_all = False
                        semantic_status = "已执行" if ok_all else "部分失败"
                        update_auto_negative_status(
                            semantic_records, "created" if ok_all else "partial"
                        )
                    elif semantic_payloads:
                        update_auto_negative_status(semantic_records, "dry_run")

                    if semantic_payloads:
                        ts2 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        reason2 = f"语义否词 {len(semantic_payloads)} 条 | 点击≥{semantic_clicks} 花费≥{semantic_spend}"
                        logs.append(
                            {
                                "时间": ts2,
                                "广告": "系统",
                                "类型": "否词",
                                "动作": "SP 语义否词",
                                "原价": 0,
                                "新价": 0,
                                "理由": reason2,
                            }
                        )
                        with db_write_lock():
                            conn.execute(
                                "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                                (ts2, "系统", "SP 语义否词", 0, 0, reason2, semantic_status),
                            )
                            for rec in semantic_records:
                                conn.execute(
                                    "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                                    (
                                        ts2,
                                        f"关键词:{rec.get('keyword_text')}",
                                        "SP 语义否词",
                                        0,
                                        0,
                                        rec.get("reason", ""),
                                        semantic_status,
                                    ),
                                )
                            conn.commit()

                    if asin_records:
                        save_negative_product_targets(asin_records)

                    asin_status = "模拟"
                    if asin_payloads and is_live_mode:
                        ok_all = True
                        for batch in _chunked(asin_payloads, 100):
                            ok, _ = create_sp_negative_targets(session, headers, batch)
                            if not ok:
                                ok_all = False
                        asin_status = "已执行" if ok_all else "部分失败"
                        update_negative_product_status(
                            asin_records, "created" if ok_all else "partial"
                        )
                    elif asin_payloads:
                        update_negative_product_status(asin_records, "dry_run")

                    if asin_payloads:
                        ts3 = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        reason3 = f"自动否投ASIN {len(asin_payloads)} 条 | 点击≥{asin_clicks}"
                        logs.append(
                            {
                                "时间": ts3,
                                "广告": "系统",
                                "类型": "否投",
                                "动作": "SP 自动否投",
                                "原价": 0,
                                "新价": 0,
                                "理由": reason3,
                            }
                        )
                        with db_write_lock():
                            conn.execute(
                                "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                                (ts3, "系统", "SP 自动否投", 0, 0, reason3, asin_status),
                            )
                            for rec in asin_records:
                                conn.execute(
                                    "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                                    (
                                        ts3,
                                        f"ASIN:{rec.get('asin')}",
                                        "SP 自动否投",
                                        0,
                                        0,
                                        "ASIN不相关",
                                        asin_status,
                                    ),
                                )
                            conn.commit()
                    set_system_value(AUTO_NEGATIVE_LAST_RUN_KEY, ts)

        # === 自动投词/拓词：基于搜索词表现 ===
        auto_expand_base = bool(allowed_campaign_ids) if whitelist else False
        auto_expand_enabled = auto_expand_base
        harvest_scale_mode = bool(scale_up_allowed)
        if auto_expand_base and not harvest_scale_mode:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            reason = "自动投词切换保守模式：仅收词(EXACT)，暂停放量型拓词"
            logs.append(
                {
                    "时间": ts,
                    "广告": "系统",
                    "类型": "投词",
                    "动作": "SP 自动投词",
                    "原值": 0,
                    "新值": 0,
                    "原因": reason,
                }
            )
            with db_write_lock():
                conn.execute(
                    "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                    (ts, "系统", "SP 自动投词", 0, 0, reason, "保守"),
                )
                conn.commit()
        if auto_expand_enabled:
            expand_days = 7
            expand_min_clicks = 4
            expand_min_orders = harvest_min_orders
            expand_strong_clicks = 9999
            expand_strong_orders = 9999
            expand_max_new = None
            expand_acos_factor = 1.0
            expand_strong_acos_factor = 0.8
            expand_allow_broad = False

            end_date = (today - timedelta(days=1)).strftime("%Y-%m-%d")
            start_date = (today - timedelta(days=expand_days)).strftime("%Y-%m-%d")

            data = None
            sales_keys = []
            orders_keys = []
            pending = _load_pending_report(AUTO_KEYWORD_PENDING_KEY)

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
                    reason = f"投词报告生成中: {pending.get('report_id')}"
                    logs.append(
                        {
                            "时间": ts,
                            "广告": "系统",
                            "类型": "投词",
                            "动作": "SP 自动投词",
                            "原值": 0,
                            "新值": 0,
                            "原因": reason,
                        }
                    )
                    with db_write_lock():
                        conn.execute(
                            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                            (ts, "系统", "SP 自动投词", 0, 0, reason, "模拟"),
                        )
                        conn.commit()
                    data = None
                elif err:
                    _clear_pending_report(AUTO_KEYWORD_PENDING_KEY)
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    reason = f"投词报告失败: {err}"
                    logs.append(
                        {
                            "时间": ts,
                            "广告": "系统",
                            "类型": "投词",
                            "动作": "SP 自动投词",
                            "原值": 0,
                            "新值": 0,
                            "原因": reason,
                        }
                    )
                    with db_write_lock():
                        conn.execute(
                            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                            (ts, "系统", "SP 自动投词", 0, 0, reason, "失败"),
                        )
                        conn.commit()
                    data = None
                else:
                    _clear_pending_report(AUTO_KEYWORD_PENDING_KEY)
            else:
                if pending:
                    _clear_pending_report(AUTO_KEYWORD_PENDING_KEY)
                report_id, sales_keys, orders_keys, err = _fetch_search_term_report(
                    session, headers, start_date, end_date
                )
                if err:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    reason = f"投词报告失败: {err}"
                    logs.append(
                        {
                            "时间": ts,
                            "广告": "系统",
                            "类型": "投词",
                            "动作": "SP 自动投词",
                            "原值": 0,
                            "新值": 0,
                            "原因": reason,
                        }
                    )
                    with db_write_lock():
                        conn.execute(
                            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                            (ts, "系统", "SP 自动投词", 0, 0, reason, "失败"),
                        )
                        conn.commit()
                    data = None
                else:
                    data, err = _wait_for_report(session, headers, report_id, max_polls=10)
                    if err == "pending":
                        _save_pending_report(
                            report_id, start_date, end_date, sales_keys, orders_keys, key=AUTO_KEYWORD_PENDING_KEY
                        )
                        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        reason = f"投词报告生成中: {report_id}"
                        logs.append(
                            {
                                "时间": ts,
                                "广告": "系统",
                                "类型": "投词",
                                "动作": "SP 自动投词",
                                "原值": 0,
                                "新值": 0,
                                "原因": reason,
                            }
                        )
                        with db_write_lock():
                            conn.execute(
                                "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                                (ts, "系统", "SP 自动投词", 0, 0, reason, "模拟"),
                            )
                            conn.commit()
                        data = None
                    elif err:
                        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        reason = f"投词报告失败: {err}"
                        logs.append(
                            {
                                "时间": ts,
                                "广告": "系统",
                                "类型": "投词",
                                "动作": "SP 自动投词",
                                "原值": 0,
                                "新值": 0,
                                "原因": reason,
                            }
                        )
                        with db_write_lock():
                            conn.execute(
                                "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                                (ts, "系统", "SP 自动投词", 0, 0, reason, "失败"),
                            )
                            conn.commit()
                        data = None
                    else:
                        _clear_pending_report(AUTO_KEYWORD_PENDING_KEY)

            if data is None or data.empty:
                if data is not None and data.empty:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    reason = "投词报告为空"
                    logs.append(
                        {
                            "时间": ts,
                            "广告": "系统",
                            "类型": "投词",
                            "动作": "SP 自动投词",
                            "原值": 0,
                            "新值": 0,
                            "原因": reason,
                        }
                    )
                    with db_write_lock():
                        conn.execute(
                            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                            (ts, "系统", "SP 自动投词", 0, 0, reason, "模拟" if not is_live_mode else "失败"),
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
                    ad_group_id = row.get("adgroupid") or row.get("ad_group_id")
                    if not ad_group_id:
                        continue
                    campaign_id = row.get("campaignid") or row.get("campaign_id")
                    if campaign_id is None or (isinstance(campaign_id, float) and pd.isna(campaign_id)):
                        campaign_id = ""
                    campaign_id = str(campaign_id or "")
                    ad_group_id = str(ad_group_id or "")
                    if not campaign_id:
                        campaign_id = str(ad_group_campaign.get(ad_group_id, "") or "")
                    if not campaign_id:
                        continue
                    if allowed_campaign_ids is not None and campaign_id not in allowed_campaign_ids:
                        continue
                    if enabled_ad_groups is not None and ad_group_id not in enabled_ad_groups:
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
                    reason = "投词报告无有效数据"
                    logs.append(
                        {
                            "时间": ts,
                            "广告": "系统",
                            "类型": "投词",
                            "动作": "SP 自动投词",
                            "原值": 0,
                            "新值": 0,
                            "原因": reason,
                        }
                    )
                    with db_write_lock():
                        conn.execute(
                            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                            (ts, "系统", "SP 自动投词", 0, 0, reason, "模拟" if not is_live_mode else "失败"),
                        )
                        conn.commit()
                else:
                    existing_kw = set()
                    existing_term_any = set()
                    for kw in keywords:
                        keyword_text = str(kw.get("keywordText") or "").strip()
                        if not keyword_text:
                            continue
                        campaign_id = str(kw.get("campaignId") or kw.get("campaign_id") or "")
                        ad_group_id = str(kw.get("adGroupId") or kw.get("ad_group_id") or "")
                        if not campaign_id or not ad_group_id:
                            continue
                        match_type = _normalize_positive_match(kw.get("matchType"))
                        if not match_type:
                            continue
                        existing_kw.add((campaign_id, ad_group_id, keyword_text.lower(), match_type))
                        existing_term_any.add((campaign_id, ad_group_id, keyword_text.lower()))

                    neg_terms = set()
                    items = list_sp_campaign_negative_keywords(session, headers)
                    for item in items:
                        term = str(item.get("keywordText") or "").strip().lower()
                        if not term:
                            continue
                        neg_terms.add((str(item.get("campaignId") or ""), "", term))
                    items = list_sp_negative_keywords(session, headers)
                    for item in items:
                        term = str(item.get("keywordText") or "").strip().lower()
                        if not term:
                            continue
                        neg_terms.add(
                            (
                                str(item.get("campaignId") or ""),
                                str(item.get("adGroupId") or ""),
                                term,
                            )
                        )

                    payloads = []
                    action_rows = []
                    agg_sorted = agg.sort_values(["orders", "sales", "clicks"], ascending=False)
                    for _, r in agg_sorted.iterrows():
                        if expand_max_new is not None and len(payloads) >= expand_max_new:
                            break
                        term = str(r.get("search_term") or "").strip()
                        if not term:
                            continue
                        if _is_asin_term(term):
                            continue
                        term_lower = term.lower()
                        campaign_id = str(r.get("campaign_id") or "")
                        ad_group_id = str(r.get("ad_group_id") or "")
                        if not campaign_id:
                            campaign_id = str(ad_group_campaign.get(ad_group_id, "") or "")
                        if not campaign_id or not ad_group_id:
                            continue
                        source_campaign_id = campaign_id
                        source_ad_group_id = ad_group_id
                        source_ad_group_name = str(ad_group_name_map.get(source_ad_group_id, "") or "")
                        source_bucket = _extract_bucket_key(
                            source_ad_group_name, campaign_name_map.get(source_campaign_id, "")
                        )
                        cost = float(r.get("cost") or 0)
                        sales = float(r.get("sales") or 0)
                        orders = float(r.get("orders") or 0)
                        clicks = float(r.get("clicks") or 0)
                        if sales <= 0 or orders < expand_min_orders or clicks < expand_min_clicks:
                            continue
                        target_acos = base_target_acos
                        rule = ad_group_map.get(source_ad_group_id)
                        if rule and rule.get("target_acos"):
                            target_acos = float(rule.get("target_acos") or base_target_acos)
                        acos_pct = cost / sales * 100 if sales > 0 else 0
                        if acos_pct > target_acos * expand_acos_factor:
                            continue
                        strong = (
                            orders >= expand_strong_orders
                            and clicks >= expand_strong_clicks
                            and acos_pct <= target_acos * expand_strong_acos_factor
                        )
                        match_types = ["EXACT"]
                        avg_cpc = cost / clicks if clicks > 0 else base_max_bid * 0.6
                        for match_type in match_types:
                            if expand_max_new is not None and len(payloads) >= expand_max_new:
                                break
                            routed_groups = []
                            if source_bucket:
                                routed_groups = keyword_harvest_routes.get(source_bucket, {}).get(match_type, [])
                            has_match_route = bool(routed_groups)
                            if not routed_groups:
                                routed_groups = [
                                    {
                                        "campaign_id": source_campaign_id,
                                        "ad_group_id": source_ad_group_id,
                                        "ad_group_name": source_ad_group_name,
                                    }
                                ]
                            for dest in routed_groups:
                                dest_campaign_id = str(dest.get("campaign_id") or source_campaign_id)
                                dest_ad_group_id = str(dest.get("ad_group_id") or source_ad_group_id)
                                if not dest_campaign_id or not dest_ad_group_id:
                                    continue
                                if allowed_campaign_ids is not None and dest_campaign_id not in allowed_campaign_ids:
                                    continue
                                if enabled_ad_groups is not None and dest_ad_group_id not in enabled_ad_groups:
                                    continue
                                if (dest_campaign_id, dest_ad_group_id, term_lower) in existing_term_any:
                                    continue
                                if (dest_campaign_id, "", term_lower) in neg_terms or (
                                    dest_campaign_id,
                                    dest_ad_group_id,
                                    term_lower,
                                ) in neg_terms:
                                    continue
                                key = (dest_campaign_id, dest_ad_group_id, term_lower, match_type)
                                if key in existing_kw:
                                    continue
                                dest_floor = adgroup_floors.get(str(dest_ad_group_id), min_bid_global)
                                dest_default = float(ad_group_default_bid.get(str(dest_ad_group_id), 0) or 0)
                                if dest_default > 0:
                                    dest_floor = max(dest_floor, min(dest_default, base_max_bid))
                                base_bid = min(base_max_bid, max(dest_floor, avg_cpc * (1.2 if strong else 1.1)))
                                payloads.append(
                                    {
                                        "campaignId": dest_campaign_id,
                                        "adGroupId": dest_ad_group_id,
                                        "state": "ENABLED",
                                        "keywordText": term,
                                        "matchType": match_type,
                                        "bid": round(base_bid, 2),
                                    }
                                )
                                existing_kw.add(key)
                                action_rows.append(
                                    {
                                        "term": term,
                                        "campaign_id": dest_campaign_id,
                                        "ad_group_id": dest_ad_group_id,
                                        "source_campaign_id": source_campaign_id,
                                        "source_ad_group_id": source_ad_group_id,
                                        "source_bucket": source_bucket,
                                        "routed": has_match_route,
                                        "bid": round(base_bid, 2),
                                        "match_type": match_type,
                                        "acos": acos_pct,
                                        "orders": orders,
                                        "clicks": clicks,
                                    }
                                )
                                break

                    if payloads:
                        ok_all = True
                        status = "模拟"
                        if is_live_mode:
                            for batch in _chunked(payloads, 100):
                                ok, _ = create_sp_keywords(session, headers, batch)
                                if not ok:
                                    ok_all = False
                            status = "已执行" if ok_all else "部分失败"

                        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        with db_write_lock():
                            for row in action_rows:
                                campaign_label = campaign_name_map.get(row["campaign_id"], row["campaign_id"])
                                source_campaign_label = campaign_name_map.get(
                                    row.get("source_campaign_id"), row.get("source_campaign_id")
                                )
                                source_label = f"{source_campaign_label}/{row.get('source_ad_group_id')}"
                                route_label = "关键词组路由" if row.get("routed") else "原广告组"
                                reason = (
                                    f"投词: {row['match_type']} | 目标 {campaign_label}/{row['ad_group_id']} | "
                                    f"来源 {source_label} | {route_label} | "
                                    f"ACOS {row['acos']:.1f}% 点击{int(row['clicks'])} 订单{int(row['orders'])}"
                                )
                                logs.append(
                                    {
                                        "时间": ts,
                                        "广告": f"关键词:{row['term']}",
                                        "类型": "投词",
                                        "动作": "SP 自动投词",
                                        "原值": 0,
                                        "新值": row["bid"],
                                        "原因": reason,
                                    }
                                )
                                conn.execute(
                                    "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                                    (
                                        ts,
                                        f"关键词:{row['term']}",
                                        "SP 自动投词",
                                        0,
                                        row["bid"],
                                        reason,
                                        status,
                                    ),
                                )
                            conn.commit()

                    if pool_enabled and pool_records and pool_daily_max > 0 and harvest_scale_mode:
                        today_prefix = datetime.now().strftime("%Y-%m-%d")
                        pool_used_today = 0
                        try:
                            row = conn.execute(
                                "SELECT COUNT(*) FROM automation_logs WHERE action_type='SP 关键词池投词' AND timestamp LIKE ?",
                                (f"{today_prefix}%",),
                            ).fetchone()
                            if row and row[0] is not None:
                                pool_used_today = int(row[0])
                        except Exception:
                            pool_used_today = 0
                        pool_remaining = max(0, pool_daily_max - pool_used_today)
                        if pool_remaining <= 0:
                            pool_records = []
                        else:
                            pool_payloads = []
                            pool_action_rows = []
                            pool_ad_groups = []
                            if enabled_ad_groups is not None:
                                pool_ad_groups = [gid for gid in enabled_ad_groups if gid in ad_group_campaign]
                            else:
                                pool_ad_groups = list(ad_group_campaign.keys())
                            pool_ad_groups = [str(gid) for gid in pool_ad_groups if str(gid).strip()]
                            if pool_ad_groups:
                                group_index = 0
                                for item in pool_records:
                                    if pool_remaining and len(pool_payloads) >= pool_remaining:
                                        break
                                    term = str(item.get("term") or "").strip()
                                    if not term:
                                        continue
                                    term_lower = str(item.get("term_lower") or term.lower())
                                    ad_group_id = pool_ad_groups[group_index % len(pool_ad_groups)]
                                    group_index += 1
                                    campaign_id = str(ad_group_campaign.get(ad_group_id, "") or "")
                                    if not campaign_id:
                                        continue
                                    if allowed_campaign_ids is not None and campaign_id not in allowed_campaign_ids:
                                        continue
                                    if (campaign_id, ad_group_id, term_lower) in existing_term_any:
                                        continue
                                    if (campaign_id, "", term_lower) in neg_terms or (
                                        campaign_id,
                                        ad_group_id,
                                        term_lower,
                                    ) in neg_terms:
                                        continue
                                    group_floor = adgroup_floors.get(str(ad_group_id), min_bid_global)
                                    base_bid = group_floor
                                    ppc_bid = float(item.get("ppc") or 0)
                                    if ppc_bid > 0:
                                        base_bid = max(group_floor, min(ppc_bid, base_max_bid))
                                    match_types = ["EXACT"]
                                    if expand_allow_broad:
                                        match_types.append("BROAD")
                                    for match_type in match_types:
                                        if pool_remaining and len(pool_payloads) >= pool_remaining:
                                            break
                                        key = (campaign_id, ad_group_id, term_lower, match_type)
                                        if key in existing_kw:
                                            continue
                                        pool_payloads.append(
                                            {
                                                "campaignId": campaign_id,
                                                "adGroupId": ad_group_id,
                                                "state": "ENABLED",
                                                "keywordText": term,
                                                "matchType": match_type,
                                                "bid": round(base_bid, 2),
                                            }
                                        )
                                        existing_kw.add(key)
                                        existing_term_any.add((campaign_id, ad_group_id, term_lower))
                                        pool_action_rows.append(
                                            {
                                                "term": term,
                                                "campaign_id": campaign_id,
                                                "ad_group_id": ad_group_id,
                                                "bid": round(base_bid, 2),
                                                "match_type": match_type,
                                                "flow": float(item.get("flow") or 0),
                                                "ppc": float(item.get("ppc") or 0),
                                                "label": str(item.get("label") or ""),
                                            }
                                        )

                            if pool_payloads:
                                ok_all = True
                                status = "模拟"
                                if is_live_mode:
                                    for batch in _chunked(pool_payloads, 100):
                                        ok, _ = create_sp_keywords(session, headers, batch)
                                        if not ok:
                                            ok_all = False
                                    status = "已执行" if ok_all else "部分失败"

                                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                with db_write_lock():
                                    for row in pool_action_rows:
                                        campaign_label = campaign_name_map.get(row["campaign_id"], row["campaign_id"])
                                        reason = (
                                            f"关键词池: {row['match_type']} | {campaign_label}/{row['ad_group_id']} | "
                                            f"flow {row['flow']:.4f} ppc {row['ppc']:.2f}"
                                        )
                                        if row["label"]:
                                            reason = f"{reason} | {row['label']}"
                                        logs.append(
                                            {
                                                "时间": ts,
                                                "广告": f"关键词:{row['term']}",
                                                "类型": "投词",
                                                "动作": "SP 关键词池投词",
                                                "原值": 0,
                                                "新值": row["bid"],
                                                "原因": reason,
                                            }
                                        )
                                        conn.execute(
                                            "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                                            (
                                                ts,
                                                f"关键词:{row['term']}",
                                                "SP 关键词池投词",
                                                0,
                                                row["bid"],
                                                reason,
                                                status,
                                            ),
                                        )
                                    conn.commit()
                    elif pool_enabled and pool_records and pool_daily_max > 0 and not harvest_scale_mode:
                        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        reason = "关键词池投词暂停：放量门控未开启"
                        logs.append(
                            {
                                "时间": ts,
                                "广告": "系统",
                                "类型": "投词",
                                "动作": "SP 关键词池投词",
                                "原值": 0,
                                "新值": 0,
                                "原因": reason,
                            }
                        )
                        with db_write_lock():
                            conn.execute(
                                "INSERT INTO automation_logs VALUES (?,?,?,?,?,?,?)",
                                (ts, "系统", "SP 关键词池投词", 0, 0, reason, "暂停"),
                            )
                            conn.commit()

    if not simple_mode:
        _run_continuous_learning(
            conn,
            logs,
            today,
            allowed_campaign_ids,
            base_target_acos,
            base_max_bid,
            base_stop_loss,
        )
    conn.commit()
    conn.close()
    return logs
