from __future__ import annotations

import contextlib
import os
import re
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_DIR = Path(__file__).resolve().parents[1]
DB_FILE = BASE_DIR / "hnv_erp_permanent.db"
SECRETS_FILE = BASE_DIR / ".streamlit" / "secrets.toml"
LOCK_FILE = DB_FILE.with_suffix(".write.lock")


@contextlib.contextmanager
def db_write_lock(timeout=30, poll_interval=0.2):
    start = time.time()
    fd = None
    while True:
        try:
            fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_EXCL | os.O_RDWR)
            os.write(fd, f"{os.getpid()} {time.time()}".encode("utf-8"))
            break
        except FileExistsError:
            try:
                if time.time() - LOCK_FILE.stat().st_mtime > timeout * 4:
                    LOCK_FILE.unlink()
                    continue
            except Exception:
                pass
            if time.time() - start >= timeout:
                raise TimeoutError("db write lock timeout")
            time.sleep(poll_interval)
    try:
        yield
    finally:
        try:
            if fd is not None:
                os.close(fd)
        except Exception:
            pass
        try:
            if LOCK_FILE.exists():
                LOCK_FILE.unlink()
        except Exception:
            pass

AD_TYPE_SP = "SP"
AD_TYPE_SB = "SB"
AD_TYPE_SD = "SD"

CAMPAIGN_MEDIA = {
    AD_TYPE_SP: "application/vnd.spCampaign.v3+json",
    AD_TYPE_SB: "application/vnd.sbCampaign.v3+json",
    AD_TYPE_SD: "application/vnd.sdCampaign.v3+json",
}
ADGROUP_MEDIA = "application/vnd.spAdGroup.v3+json"

AUTO_SYNC_DEFAULT_DAYS = 7
AUTO_SYNC_MAX_DAYS = 30
AUTO_SYNC_REFRESH_DAYS = 2
REPORT_POLL_MAX = 180
REPORT_POLL_SLEEP_SECONDS = 2

AUTO_SYNC_TS_KEY = "last_auto_sync_ts"
SYNC_STATUS_KEY = "last_sync_status"
SYNC_ERROR_KEY = "last_sync_error"
SYNC_DAYS_KEY = "last_sync_days"

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


def parse_amazon_secrets(path: Path) -> dict:
    section = None
    conf = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            continue
        if section != "amazon":
            continue
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip()
        if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
            val = val[1:-1]
        conf[key] = val
    return conf


def load_amazon_config() -> dict:
    conf = {}
    if SECRETS_FILE.exists():
        try:
            conf.update(parse_amazon_secrets(SECRETS_FILE))
        except Exception:
            pass

    env_map = {
        "AMAZON_CLIENT_ID": "client_id",
        "AMAZON_CLIENT_SECRET": "client_secret",
        "AMAZON_REFRESH_TOKEN": "refresh_token",
        "AMAZON_PROFILE_ID": "profile_id",
    }
    for env_key, key in env_map.items():
        val = os.getenv(env_key)
        if val:
            conf[key] = val

    missing = [k for k in ["client_id", "client_secret", "refresh_token", "profile_id"] if not conf.get(k)]
    if missing:
        raise RuntimeError("Missing amazon config: " + ", ".join(missing))
    return conf


def get_db_connection():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
    except Exception:
        pass
    return conn


def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute(
        """CREATE TABLE IF NOT EXISTS campaign_reports
           (date TEXT, campaign_id TEXT, campaign_name TEXT, ad_type TEXT DEFAULT 'SP',
            cost REAL, sales REAL, clicks INTEGER, impressions INTEGER, orders INTEGER,
            UNIQUE(date, campaign_id, ad_type))"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS asin_reports
           (date TEXT, asin TEXT, sku TEXT,
            cost REAL, sales REAL, clicks INTEGER, impressions INTEGER, orders INTEGER,
            UNIQUE(date, asin, sku))"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS product_ads
           (ad_id TEXT PRIMARY KEY, campaign_id TEXT, ad_group_id TEXT,
            asin TEXT, sku TEXT, state TEXT, serving_status TEXT,
            creation_date TEXT, last_update_date TEXT, last_synced TEXT)"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS campaign_settings
           (campaign_id TEXT PRIMARY KEY, campaign_name TEXT, ad_type TEXT DEFAULT 'SP',
            budget_type TEXT DEFAULT 'DAILY', current_budget REAL, current_status TEXT,
            last_updated TEXT, is_star INTEGER DEFAULT 0)"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS system_logs
           (key TEXT PRIMARY KEY, value TEXT)"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS ad_group_settings
           (ad_group_id TEXT PRIMARY KEY, campaign_id TEXT, ad_group_name TEXT,
            default_bid REAL, state TEXT, last_updated TEXT)"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS product_settings
           (asin TEXT, sku TEXT, daily_budget REAL, target_acos REAL, budget_flex REAL DEFAULT 0,
            is_star INTEGER DEFAULT 0, ai_enabled INTEGER DEFAULT 1, last_updated TEXT,
            PRIMARY KEY (asin, sku))"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS budget_groups
           (group_name TEXT PRIMARY KEY, total_budget REAL, last_updated TEXT)"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS budget_group_items
           (group_name TEXT, asin TEXT, sku TEXT, weight REAL, last_updated TEXT,
            PRIMARY KEY (group_name, asin, sku))"""
    )

    try:
        c.execute("ALTER TABLE campaign_settings ADD COLUMN is_star INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE campaign_settings ADD COLUMN ad_type TEXT DEFAULT 'SP'")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE campaign_settings ADD COLUMN budget_type TEXT DEFAULT 'DAILY'")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE campaign_reports ADD COLUMN ad_type TEXT DEFAULT 'SP'")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE product_settings ADD COLUMN budget_flex REAL DEFAULT 0")
    except Exception:
        pass

    conn.commit()
    conn.close()


def get_system_value(key):
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT value FROM system_logs WHERE key=?", (key,)).fetchone()
    except Exception:
        row = None
    finally:
        conn.close()
    return row[0] if row and row[0] is not None else None


def set_system_value(key, value):
    with db_write_lock():
        conn = get_db_connection()
        try:
            conn.execute("INSERT OR REPLACE INTO system_logs (key, value) VALUES (?, ?)", (key, value))
            conn.commit()
        finally:
            conn.close()


def set_sync_status(status, detail=None, days=None):
    set_system_value(SYNC_STATUS_KEY, status)
    if detail is not None:
        set_system_value(SYNC_ERROR_KEY, detail[:1000])
    if days is not None:
        set_system_value(SYNC_DAYS_KEY, str(days))


def get_latest_report_date():
    conn = get_db_connection()
    try:
        row = conn.execute("SELECT MAX(date) FROM campaign_reports").fetchone()
    except Exception:
        row = None
    finally:
        conn.close()
    if row and row[0]:
        try:
            return date.fromisoformat(row[0])
        except Exception:
            return None
    return None


def compute_sync_days():
    today = datetime.now().date()
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
    db = get_db_connection()
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
    today = datetime.now().date()
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


def get_retry_session():
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))
    return session


def get_amazon_session_and_headers(conf):
    session = get_retry_session()
    r = session.post(
        "https://api.amazon.com/auth/o2/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": conf["refresh_token"],
            "client_id": conf["client_id"],
            "client_secret": conf["client_secret"],
        },
        timeout=30,
    )
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError("Failed to obtain access token")
    headers = {
        "Authorization": f"Bearer {token}",
        "Amazon-Advertising-API-ClientId": conf["client_id"],
        "Amazon-Advertising-API-Scope": conf["profile_id"],
        "Content-Type": "application/json",
    }
    return session, headers


def get_media_headers(headers, media_type):
    h = headers.copy()
    h.update({"Content-Type": media_type, "Accept": media_type})
    return h


def get_row_value(row, keys, default=0):
    if row is None:
        return default
    for key in keys:
        if not key:
            continue
        if key in row and pd.notna(row[key]):
            return row[key]
        key_lower = str(key).lower()
        if key_lower != key and key_lower in row and pd.notna(row[key_lower]):
            return row[key_lower]
    return default


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
        db = get_db_connection()
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
        db = get_db_connection()
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
        db = get_db_connection()
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
            timeout=30,
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
                chk = session.get(
                    f"https://advertising-api.amazon.com/reporting/reports/{rid}",
                    headers=headers,
                    timeout=30,
                )
                status = chk.json().get("status")
                if status == "COMPLETED":
                    url = chk.json().get("url")
                    if url:
                        data = pd.read_json(url, compression="gzip")
                        if not data.empty:
                            with db_write_lock():
                                db = get_db_connection()
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


def sync_asin_report(session, headers, d_str, columns=None, sales_keys=None, orders_keys=None, allow_retry=True):
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
                    "groupBy": ["advertisedProduct"],
                    "columns": columns,
                    "reportTypeId": "spAdvertisedProduct",
                    "timeUnit": "DAILY",
                    "format": "GZIP_JSON",
                },
            },
            timeout=30,
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
        else:
            rid = None
            errors.append(f"ASIN {d_str} create failed {req.status_code}: {req.text[:200]}")

        if rid:
            for _ in range(REPORT_POLL_MAX):
                time.sleep(REPORT_POLL_SLEEP_SECONDS)
                chk = session.get(
                    f"https://advertising-api.amazon.com/reporting/reports/{rid}",
                    headers=headers,
                    timeout=30,
                )
                status = chk.json().get("status")
                if status == "COMPLETED":
                    url = chk.json().get("url")
                    if url:
                        data = pd.read_json(url, compression="gzip")
                        if not data.empty:
                            with db_write_lock():
                                db = get_db_connection()
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


def run_sync_task(conf, days):
    session, headers = get_amazon_session_and_headers(conf)

    try:
        sync_campaign_list(session, headers, AD_TYPE_SP)
        sync_campaign_list(session, headers, AD_TYPE_SB)
        sync_campaign_list(session, headers, AD_TYPE_SD)
    except Exception:
        pass

    try:
        sync_sp_adgroups(session, headers)
    except Exception:
        pass

    try:
        sync_product_ads(session, headers)
    except Exception:
        pass

    errors = []
    dates_to_sync = _compute_sync_dates(days)
    for d_str in dates_to_sync:
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
    set_system_value(AUTO_SYNC_TS_KEY, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return len(errors) == 0


def main():
    init_db()
    days = compute_sync_days()
    try:
        conf = load_amazon_config()
    except Exception as exc:
        set_sync_status("no_config", str(exc), days)
        print(f"Config error: {exc}")
        return 1

    set_sync_status("running", "", days)
    try:
        ok = run_sync_task(conf, days)
        if ok:
            print(f"Sync OK (days={days})")
        else:
            print(f"Sync partial (days={days})")
        return 0 if ok else 2
    except Exception as exc:
        set_sync_status("error", str(exc), days)
        print(f"Sync error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
