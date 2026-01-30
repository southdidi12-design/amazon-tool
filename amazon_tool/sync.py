import re
import sqlite3
import threading
import time
from datetime import datetime, timedelta

import pandas as pd
try:
    import streamlit as st
except Exception:
    class _FallbackStreamlit:
        def cache_resource(self, func):
            return func

        def experimental_singleton(self, func):
            return func

    st = _FallbackStreamlit()

from .amazon_api import get_amazon_session_and_headers, get_media_headers, get_row_value
from .config import (
    ADGROUP_MEDIA,
    AD_TYPE_SB,
    AD_TYPE_SD,
    AD_TYPE_SP,
    AUTO_SYNC_DEFAULT_DAYS,
    AUTO_SYNC_INTERVAL_SECONDS,
    AUTO_SYNC_MAX_DAYS,
    AUTO_SYNC_REFRESH_DAYS,
    AUTO_SYNC_TS_KEY,
    CAMPAIGN_MEDIA,
    DB_FILE,
    REPORT_POLL_MAX,
    REPORT_POLL_SLEEP_SECONDS,
)
from .config import get_real_today
from .db import db_write_lock, get_latest_report_date, set_sync_status, set_system_value

SYNC_LOCK = threading.Lock()

INVALID_COLUMNS_ERROR = "columns includes invalid values"
COST_KEYS = ["cost", "spend"]
FALLBACK_SALES_KEYS = [
    "sales7d",
    "sales14d",
    "sales1d",
    "sales30d",
    "sales",
    "attributedSales14d",
    "attributedSales7d",
    "attributedSales1d",
    "attributedSales14dSameSKU",
    "attributedSales30d",
]
FALLBACK_ORDER_KEYS = [
    "purchases7d",
    "purchases14d",
    "purchases1d",
    "orders",
    "attributedConversions14d",
    "attributedConversions7d",
    "attributedConversions1d",
    "attributedConversions14dSameSKU",
    "attributedUnitsOrdered14d",
]
ASIN_ID_CANDIDATES = ["advertisedAsin", "asin"]
ASIN_SKU_CANDIDATES = ["advertisedSku", "sku"]


def _dedupe(items):
    seen = set()
    out = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


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


def _resolve_columns(base_columns, allowed, required, sales_candidates, orders_candidates, cost_candidates):
    columns = [c for c in base_columns if c in allowed]
    for req in required:
        if req in allowed and req not in columns:
            columns.append(req)

    for key in cost_candidates:
        if key in allowed and key not in columns:
            columns.append(key)
            break
    for key in sales_candidates:
        if key in allowed and key not in columns:
            columns.append(key)
            break
    for key in orders_candidates:
        if key in allowed and key not in columns:
            columns.append(key)
            break

    sales_keys = [k for k in sales_candidates if k in columns]
    orders_keys = [k for k in orders_candidates if k in columns]
    return columns, sales_keys, orders_keys


def _resolve_asin_columns(base_columns, allowed, sales_candidates, orders_candidates, cost_candidates):
    columns = [c for c in base_columns if c in allowed]

    for key in ASIN_ID_CANDIDATES:
        if key in allowed and key not in columns:
            columns.append(key)
            break
    for key in ASIN_SKU_CANDIDATES:
        if key in allowed and key not in columns:
            columns.append(key)
            break

    for key in cost_candidates:
        if key in allowed and key not in columns:
            columns.append(key)
            break
    for key in sales_candidates:
        if key in allowed and key not in columns:
            columns.append(key)
            break
    for key in orders_candidates:
        if key in allowed and key not in columns:
            columns.append(key)
            break

    sales_keys = [k for k in sales_candidates if k in columns]
    orders_keys = [k for k in orders_candidates if k in columns]
    return columns, sales_keys, orders_keys


def _connect_db():
    db = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=30)
    try:
        db.execute("PRAGMA journal_mode=WAL;")
        db.execute("PRAGMA synchronous=NORMAL;")
        db.execute("PRAGMA busy_timeout=5000;")
    except Exception:
        pass
    return db


def compute_auto_sync_days():
    today = get_real_today()
    latest = get_latest_report_date()
    target_end = today - timedelta(days=1)
    if latest is None:
        return AUTO_SYNC_DEFAULT_DAYS
    missing_days = (target_end - latest).days
    if missing_days < 0:
        missing_days = 0
    if missing_days > 0:
        days = min(AUTO_SYNC_DEFAULT_DAYS, missing_days)
        days = min(days, AUTO_SYNC_MAX_DAYS)
        return max(1, days)
    return AUTO_SYNC_REFRESH_DAYS


def _get_existing_dates(table, start_str, end_str, ad_type=None):
    db = _connect_db()
    try:
        sql = f"SELECT DISTINCT date FROM {table} WHERE date >= ? AND date <= ?"
        params = [start_str, end_str]
        if ad_type and table == "campaign_reports":
            sql += " AND ad_type = ?"
            params.append(ad_type)
        rows = db.execute(sql, params).fetchall()
        return {r[0] for r in rows if r and r[0]}
    except Exception:
        return set()
    finally:
        db.close()


def _compute_sync_dates(days):
    today = get_real_today()
    lookback_days = max(days, AUTO_SYNC_DEFAULT_DAYS)
    lookback_days = min(max(1, lookback_days), AUTO_SYNC_MAX_DAYS)
    end_date = today - timedelta(days=1)
    start_date = today - timedelta(days=lookback_days)
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    existing_campaign = _get_existing_dates("campaign_reports", start_str, end_str, ad_type=AD_TYPE_SP)
    existing_asin = _get_existing_dates("asin_reports", start_str, end_str)

    dates = []
    for i in range(1, lookback_days + 1):
        d_str = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        if i <= days or d_str not in existing_campaign or d_str not in existing_asin:
            dates.append(d_str)

    seen = set()
    ordered = []
    for d_str in dates:
        if d_str in seen:
            continue
        seen.add(d_str)
        ordered.append(d_str)
    return ordered


def run_sync_task_guarded(days=7, status_box=None):
    if not SYNC_LOCK.acquire(blocking=False):
        set_sync_status("busy", "sync already running", days)
        return False
    try:
        return run_sync_task(days, status_box)
    finally:
        SYNC_LOCK.release()


def auto_sync_if_needed():
    days = compute_auto_sync_days()
    if days <= 0:
        set_sync_status("up_to_date", "", days)
        return False
    ok = run_sync_task_guarded(days, None)
    if ok:
        set_system_value(AUTO_SYNC_TS_KEY, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return ok


def auto_sync_loop():
    while True:
        try:
            auto_sync_if_needed()
        except Exception:
            pass
        time.sleep(AUTO_SYNC_INTERVAL_SECONDS)


try:
    cache_resource = st.cache_resource
except AttributeError:
    cache_resource = st.experimental_singleton


@cache_resource
def start_auto_sync():
    t = threading.Thread(target=auto_sync_loop, daemon=True)
    t.start()
    return t


def sync_campaign_list(session, headers, ad_type):
    media = CAMPAIGN_MEDIA.get(ad_type)
    if not media:
        return 0
    h = get_media_headers(headers, media)
    res = session.post(
        f"https://advertising-api.amazon.com/{ad_type.lower()}/campaigns/list",
        headers=h,
        json={},
        timeout=30,
    )
    if res.status_code != 200:
        return 0
    campaigns = res.json().get("campaigns", [])
    if not campaigns:
        return 0
    with db_write_lock():
        db = _connect_db()
        cur = db.cursor()
        for c in campaigns:
            campaign_id = c.get("campaignId", c.get("campaign_id"))
            if campaign_id is None:
                continue
            budget_obj = c.get("budget", {})
            if not isinstance(budget_obj, dict):
                budget_obj = {}
            budget = budget_obj.get("budget", c.get("budget", 0))
            budget_type = budget_obj.get("budgetType", c.get("budgetType", "DAILY"))
            cur.execute(
                """
                INSERT INTO campaign_settings
                    (campaign_id, campaign_name, ad_type, budget_type, current_budget, current_status, last_updated)
                VALUES (?,?,?,?,?,?,?)
                ON CONFLICT(campaign_id) DO UPDATE SET
                    campaign_name=excluded.campaign_name,
                    ad_type=excluded.ad_type,
                    budget_type=excluded.budget_type,
                    current_budget=excluded.current_budget,
                    current_status=excluded.current_status,
                    last_updated=excluded.last_updated
                """,
                (
                    str(campaign_id),
                    c.get("name", ""),
                    ad_type,
                    budget_type,
                    budget,
                    c.get("state", ""),
                    datetime.now().strftime("%Y-%m-%d"),
                ),
            )
        db.commit()
        db.close()
    return len(campaigns)


def sync_sp_adgroups(session, headers):
    h = get_media_headers(headers, ADGROUP_MEDIA)
    with db_write_lock():
        db = _connect_db()
        cur = db.cursor()
        try:
            next_token = None
            for _ in range(50):
                body = {"maxResults": 100, "includeExtendedDataFields": True}
                if next_token:
                    body["nextToken"] = next_token
                res = session.post(
                    "https://advertising-api.amazon.com/sp/adGroups/list",
                    headers=h,
                    json=body,
                    timeout=30,
                )
                if res.status_code != 200:
                    break
                payload = res.json()
                groups = payload.get("adGroups", [])
                if not isinstance(groups, list):
                    groups = []
                if groups:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    for g in groups:
                        ad_group_id = g.get("adGroupId", g.get("ad_group_id"))
                        if ad_group_id is None:
                            continue
                        cur.execute(
                            "INSERT OR REPLACE INTO ad_group_settings VALUES (?,?,?,?,?,?)",
                            (
                                str(ad_group_id),
                                str(g.get("campaignId", g.get("campaign_id", ""))),
                                g.get("name", ""),
                                g.get("defaultBid", 0) or 0,
                                g.get("state", ""),
                                ts,
                            ),
                        )
                    db.commit()
                next_token = payload.get("nextToken") or payload.get("next_token")
                if not next_token:
                    break
        finally:
            db.commit()
            db.close()


def sync_product_ads(session, headers):
    h = get_media_headers(headers, "application/vnd.spproductAd.v3+json")
    with db_write_lock():
        db = _connect_db()
        cur = db.cursor()
        try:
            next_token = None
            for _ in range(50):
                body = {"maxResults": 100, "includeExtendedDataFields": True}
                if next_token:
                    body["nextToken"] = next_token
                res = session.post(
                    "https://advertising-api.amazon.com/sp/productAds/list",
                    headers=h,
                    json=body,
                    timeout=30,
                )
                if res.status_code != 200:
                    break
                payload = res.json()
                ads = payload.get("productAds", [])
                if not isinstance(ads, list):
                    ads = []
                if ads:
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    for ad in ads:
                        ad_id = ad.get("adId")
                        if ad_id is None:
                            continue
                        campaign_id = ad.get("campaignId", "")
                        ad_group_id = ad.get("adGroupId", "")
                        asin = ad.get("asin", "")
                        sku = ad.get("sku", "")
                        state = ad.get("state", "")
                        serving_status = ad.get("servingStatus", "")
                        creation_date = ad.get("creationDate", ad.get("creation_date", ""))
                        last_update_date = ad.get("lastUpdateDate", ad.get("last_update_date", ""))
                        cur.execute(
                            "INSERT OR REPLACE INTO product_ads VALUES (?,?,?,?,?,?,?,?,?,?)",
                            (
                                str(ad_id),
                                str(campaign_id),
                                str(ad_group_id),
                                str(asin),
                                str(sku),
                                state,
                                serving_status,
                                creation_date,
                                last_update_date,
                                ts,
                            ),
                        )
                    db.commit()
                next_token = payload.get("nextToken") or payload.get("next_token")
                if not next_token:
                    break
        finally:
            db.commit()
            db.close()


def sync_campaign_report(
    session,
    headers,
    ad_product,
    report_type_id,
    columns,
    ad_type,
    sales_keys,
    orders_keys,
    d_str,
    allow_retry=True,
):
    try:
        req = session.post(
            "https://advertising-api.amazon.com/reporting/reports",
            headers=headers,
            json={
                "startDate": d_str,
                "endDate": d_str,
                "configuration": {
                    "adProduct": ad_product,
                    "groupBy": ["campaign"],
                    "columns": columns,
                    "reportTypeId": report_type_id,
                    "timeUnit": "DAILY",
                    "format": "GZIP_JSON",
                },
            },
        )
        if req.status_code in [200, 201, 202]:
            rid = req.json().get("reportId")
        elif "duplicate" in req.text.lower():
            rid = req.json().get("detail", "").split(":")[-1].strip()
        elif allow_retry and req.status_code == 400 and INVALID_COLUMNS_ERROR in req.text:
            allowed = _parse_allowed_columns(req.text)
            if allowed:
                sales_candidates = _dedupe(sales_keys + FALLBACK_SALES_KEYS)
                orders_candidates = _dedupe(orders_keys + FALLBACK_ORDER_KEYS)
                new_columns, new_sales, new_orders = _resolve_columns(
                    columns, allowed, ["campaignId", "campaignName"], sales_candidates, orders_candidates, COST_KEYS
                )
                if new_columns:
                    return sync_campaign_report(
                        session,
                        headers,
                        ad_product,
                        report_type_id,
                        new_columns,
                        ad_type,
                        new_sales,
                        new_orders,
                        d_str,
                        allow_retry=False,
                    )
            return False, f"{ad_product} {d_str} invalid columns"
        else:
            return False, f"{ad_product} {d_str} create failed {req.status_code}: {req.text[:200]}"
        if rid:
            for _ in range(REPORT_POLL_MAX):
                time.sleep(REPORT_POLL_SLEEP_SECONDS)
                chk = session.get(f"https://advertising-api.amazon.com/reporting/reports/{rid}", headers=headers)
                status = chk.json().get("status")
                if status == "COMPLETED":
                    url = chk.json().get("url")
                    if url:
                        data = pd.read_json(url, compression="gzip")
                        if not data.empty:
                            with db_write_lock():
                                db = _connect_db()
                                data.columns = [c.lower() for c in data.columns]
                                for _, row in data.iterrows():
                                    campaign_id = row.get("campaignid", row.get("campaign_id", ""))
                                    if not str(campaign_id).strip():
                                        continue
                                    cost = get_row_value(row, ["cost", "spend"], 0)
                                    sales = get_row_value(row, sales_keys, 0)
                                    orders = get_row_value(row, orders_keys, 0)
                                    db.execute(
                                        "INSERT OR REPLACE INTO campaign_reports (date, campaign_id, campaign_name, ad_type, cost, sales, clicks, impressions, orders) VALUES (?,?,?,?,?,?,?,?,?)",
                                        (
                                            d_str,
                                            str(campaign_id),
                                            row.get("campaignname", row.get("campaign_name", "")),
                                            ad_type,
                                            cost,
                                            sales,
                                            get_row_value(row, ["clicks"], 0),
                                            get_row_value(row, ["impressions"], 0),
                                            orders,
                                        ),
                                    )
                                db.commit()
                                db.close()
                            return True, None
                        return True, None
                    return False, f"{ad_product} {d_str} completed without url"
                if status in ["FAILED", "CANCELLED"]:
                    return False, f"{ad_product} {d_str} status {status}"
            return False, f"{ad_product} {d_str} timeout"
        return False, f"{ad_product} {d_str} missing reportId"
    except Exception as exc:
        return False, f"{ad_product} {d_str} exception: {exc}"
    return True, None


def sync_asin_report(
    session,
    headers,
    d_str,
    columns=None,
    sales_keys=None,
    orders_keys=None,
    allow_retry=True,
    group_by=None,
):
    if columns is None:
        columns = [
            "advertisedAsin",
            "advertisedSku",
            "cost",
            "sales1d",
            "clicks",
            "impressions",
            "purchases1d",
        ]
    if group_by is None:
        group_by = ["advertisedProduct"]
    if sales_keys is None:
        sales_keys = ["sales7d", "sales14d", "sales1d", "sales"]
    if orders_keys is None:
        orders_keys = ["purchases7d", "purchases14d", "purchases1d", "orders"]

    errors = []
    try:
        req = session.post(
            "https://advertising-api.amazon.com/reporting/reports",
            headers=headers,
            json={
                "startDate": d_str,
                "endDate": d_str,
                "configuration": {
                    "adProduct": "SPONSORED_PRODUCTS",
                    "groupBy": group_by,
                    "columns": columns,
                    "reportTypeId": "spAdvertisedProduct",
                    "timeUnit": "DAILY",
                    "format": "GZIP_JSON",
                },
            },
        )
        if req.status_code in [200, 201, 202]:
            rid = req.json().get("reportId")
        elif "duplicate" in req.text.lower():
            rid = req.json().get("detail", "").split(":")[-1].strip()
        elif allow_retry and req.status_code == 400 and INVALID_COLUMNS_ERROR in req.text:
            allowed = _parse_allowed_columns(req.text)
            if allowed:
                sales_candidates = _dedupe(sales_keys + FALLBACK_SALES_KEYS)
                orders_candidates = _dedupe(orders_keys + FALLBACK_ORDER_KEYS)
                new_columns, new_sales, new_orders = _resolve_asin_columns(
                    columns, allowed, sales_candidates, orders_candidates, COST_KEYS
                )
                if new_columns:
                    return sync_asin_report(
                        session,
                        headers,
                        d_str,
                        columns=new_columns,
                        sales_keys=new_sales,
                        orders_keys=new_orders,
                        allow_retry=False,
                    )
            errors.append(f"ASIN {d_str} invalid columns")
            return errors
        elif (
            allow_retry
            and req.status_code == 400
            and "groupby" in req.text.lower()
            and "allowed values" in req.text.lower()
        ):
            allowed = _parse_allowed_columns(req.text)
            if allowed:
                return sync_asin_report(
                    session,
                    headers,
                    d_str,
                    columns=columns,
                    sales_keys=sales_keys,
                    orders_keys=orders_keys,
                    allow_retry=False,
                    group_by=[allowed[0]],
                )
            return sync_asin_report(
                session,
                headers,
                d_str,
                columns=columns,
                sales_keys=sales_keys,
                orders_keys=orders_keys,
                allow_retry=False,
                group_by=["advertiser"],
            )
        else:
            rid = None
            errors.append(f"ASIN {d_str} create failed {req.status_code}: {req.text[:200]}")

        if rid:
            for _ in range(REPORT_POLL_MAX):
                time.sleep(REPORT_POLL_SLEEP_SECONDS)
                chk = session.get(f"https://advertising-api.amazon.com/reporting/reports/{rid}", headers=headers)
                status = chk.json().get("status")
                if status == "COMPLETED":
                    url = chk.json().get("url")
                    if url:
                        data = pd.read_json(url, compression="gzip")
                        if not data.empty:
                            with db_write_lock():
                                db = _connect_db()
                                data.columns = [c.lower() for c in data.columns]
                                for _, row in data.iterrows():
                                    asin = row.get("advertisedasin", row.get("asin", ""))
                                    if pd.isna(asin) or not str(asin).strip():
                                        continue
                                    sku = row.get("advertisedsku", row.get("sku", ""))
                                    if pd.isna(sku):
                                        sku = ""
                                    cost = get_row_value(row, ["cost", "spend"], 0)
                                    sales = get_row_value(row, sales_keys, 0)
                                    orders = get_row_value(row, orders_keys, 0)
                                    db.execute(
                                        "INSERT OR REPLACE INTO asin_reports VALUES (?,?,?,?,?,?,?,?)",
                                        (
                                            d_str,
                                            str(asin),
                                            str(sku),
                                            cost,
                                            sales,
                                            row.get("clicks", 0),
                                            row.get("impressions", 0),
                                            orders,
                                        ),
                                    )
                                db.commit()
                                db.close()
                    break
                if status in ["FAILED", "CANCELLED"]:
                    errors.append(f"ASIN {d_str} status {status}")
                    break
            else:
                errors.append(f"ASIN {d_str} timeout")
    except Exception as exc:
        errors.append(f"ASIN {d_str} exception: {exc}")
    return errors


def run_sync_task(days=7, status_box=None):
    session, headers = get_amazon_session_and_headers()
    if not session:
        if status_box:
            status_box.error("无配置")
        set_sync_status("no_config", "missing amazon secrets", days)
        return False
    set_sync_status("running", "", days)

    # 2. Settings (Campaign List)
    try:
        sync_campaign_list(session, headers, AD_TYPE_SP)
        sync_campaign_list(session, headers, AD_TYPE_SB)
        sync_campaign_list(session, headers, AD_TYPE_SD)
    except Exception:
        pass

    # 2.1 SP Ad Groups
    try:
        sync_sp_adgroups(session, headers)
    except Exception:
        pass

    # 2.2 Product Ads
    try:
        sync_product_ads(session, headers)
    except Exception:
        pass

    # 3. Reports
    errors = []
    dates_to_sync = _compute_sync_dates(days)
    for d_str in dates_to_sync:
        if status_box:
            status_box.text(f"📥 同步中: {d_str}")
        ok, err = sync_campaign_report(
            session,
            headers,
            "SPONSORED_PRODUCTS",
            "spCampaigns",
            ["campaignId", "campaignName", "cost", "sales1d", "clicks", "impressions", "purchases1d"],
            AD_TYPE_SP,
            ["sales7d", "sales14d", "sales1d", "sales"],
            ["purchases7d", "purchases14d", "purchases1d", "orders"],
            d_str,
        )
        if not ok and err:
            errors.append(err)
        ok, err = sync_campaign_report(
            session,
            headers,
            "SPONSORED_BRANDS",
            "sbCampaigns",
            ["campaignId", "campaignName", "cost", "clicks", "impressions", "attributedSales14d", "attributedConversions14d"],
            AD_TYPE_SB,
            ["attributedsales14d", "attributedsales7d", "sales14d", "sales7d", "sales"],
            ["attributedconversions14d", "attributedconversions7d", "attributedunitsordered14d", "orders"],
            d_str,
        )
        if not ok and err:
            errors.append(err)
        ok, err = sync_campaign_report(
            session,
            headers,
            "SPONSORED_DISPLAY",
            "sdCampaigns",
            ["campaignId", "campaignName", "cost", "clicks", "impressions", "attributedSales14d", "attributedConversions14d"],
            AD_TYPE_SD,
            ["attributedsales14d", "attributedsales7d", "sales14d", "sales7d", "sales"],
            ["attributedconversions14d", "attributedconversions7d", "attributedunitsordered14d", "orders"],
            d_str,
        )
        if not ok and err:
            errors.append(err)
        errors.extend(sync_asin_report(session, headers, d_str))

    if errors:
        set_sync_status("partial", errors[0], days)
    else:
        set_sync_status("ok", "", days)
    return True
