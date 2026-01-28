import contextlib
import os
import sqlite3
import time
from datetime import date, datetime

import pandas as pd

from pathlib import Path

from .config import (
    AD_TYPE_SP,
    DB_FILE,
    SYNC_DAYS_KEY,
    SYNC_ERROR_KEY,
    SYNC_STATUS_KEY,
)

LOCK_FILE = str(Path(DB_FILE).with_suffix(".write.lock"))


@contextlib.contextmanager
def db_write_lock(timeout=30, poll_interval=0.2):
    start = time.time()
    fd = None
    while True:
        try:
            fd = os.open(LOCK_FILE, os.O_CREAT | os.O_EXCL | os.O_RDWR)
            os.write(fd, f"{os.getpid()} {time.time()}".encode("utf-8"))
            break
        except FileExistsError:
            try:
                if time.time() - os.path.getmtime(LOCK_FILE) > timeout * 4:
                    os.remove(LOCK_FILE)
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
            if os.path.exists(LOCK_FILE):
                os.remove(LOCK_FILE)
        except Exception:
            pass


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
    # 确保表结构完整
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
            budget_type TEXT DEFAULT 'DAILY', current_budget REAL, current_status TEXT, last_updated TEXT,
            is_star INTEGER DEFAULT 0)"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS automation_logs
           (timestamp TEXT, campaign_name TEXT, action_type TEXT,
            old_value REAL, new_value REAL, reason TEXT, status TEXT)"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS ai_insights
           (campaign_id TEXT PRIMARY KEY, insight TEXT, suggest_budget REAL, update_time TEXT)"""
    )
    c.execute("""CREATE TABLE IF NOT EXISTS system_logs (key TEXT PRIMARY KEY, value TEXT)""")
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
    c.execute(
        """CREATE TABLE IF NOT EXISTS auto_negative_keywords
           (campaign_id TEXT, ad_group_id TEXT, keyword_text TEXT, match_type TEXT,
            level TEXT, source TEXT, status TEXT, created_at TEXT, last_updated TEXT,
            reason TEXT, cost REAL, sales REAL, orders INTEGER, clicks INTEGER,
            PRIMARY KEY (campaign_id, ad_group_id, keyword_text, match_type, level, source))"""
    )
    c.execute(
        """CREATE TABLE IF NOT EXISTS negative_product_targets
           (campaign_id TEXT, ad_group_id TEXT, asin TEXT, expression_type TEXT,
            level TEXT, source TEXT, status TEXT, created_at TEXT, last_updated TEXT,
            PRIMARY KEY (campaign_id, ad_group_id, asin, expression_type, level, source))"""
    )

    # 补丁：防止旧数据库没有新增字段
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
    try:
        c.execute("ALTER TABLE auto_negative_keywords ADD COLUMN reason TEXT")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE auto_negative_keywords ADD COLUMN cost REAL")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE auto_negative_keywords ADD COLUMN sales REAL")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE auto_negative_keywords ADD COLUMN orders INTEGER")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE auto_negative_keywords ADD COLUMN clicks INTEGER")
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


def get_dashboard_data(start, end):
    conn = get_db_connection()
    try:
        perf = pd.read_sql_query(
            f"""
            SELECT campaign_id, COALESCE(ad_type, 'SP') as ad_type,
                   SUM(cost) as cost, SUM(sales) as sales,
                   SUM(clicks) as clicks, SUM(impressions) as impressions, SUM(orders) as orders
            FROM campaign_reports
            WHERE date >= '{start}' AND date <= '{end}'
            GROUP BY campaign_id, ad_type
            """,
            conn,
        )
        sett = pd.read_sql_query("SELECT * FROM campaign_settings", conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

    if perf.empty:
        if sett.empty:
            return pd.DataFrame()
        m = sett.copy()
        m["cost"] = 0
        m["sales"] = 0
        m["clicks"] = 0
        m["impressions"] = 0
        m["orders"] = 0
    else:
        m = pd.merge(sett, perf, on=["campaign_id", "ad_type"], how="left")

    for c in ["cost", "sales", "clicks", "impressions", "orders", "current_budget"]:
        m[c] = m[c].fillna(0)
    if "ad_type" in m.columns:
        m["ad_type"] = m["ad_type"].fillna(AD_TYPE_SP)

    m["cpc"] = m.apply(lambda x: x["cost"] / x["clicks"] if x["clicks"] > 0 else 0, axis=1)
    m["acos"] = m.apply(lambda x: x["cost"] / x["sales"] if x["sales"] > 0 else 0, axis=1)
    m["cr"] = m.apply(lambda x: x["orders"] / x["clicks"] if x["clicks"] > 0 else 0, axis=1)
    return m


def get_asin_dashboard_data(start, end):
    conn = get_db_connection()
    try:
        perf = pd.read_sql_query(
            f"""
            SELECT asin, sku,
                   SUM(cost) as cost, SUM(sales) as sales,
                   SUM(clicks) as clicks, SUM(impressions) as impressions, SUM(orders) as orders
            FROM asin_reports
            WHERE date >= '{start}' AND date <= '{end}'
            GROUP BY asin, sku
            """,
            conn,
        )
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

    if perf.empty:
        return pd.DataFrame()

    perf["asin"] = perf["asin"].fillna("")
    perf["sku"] = perf["sku"].fillna("")
    for c in ["cost", "sales", "clicks", "impressions", "orders"]:
        perf[c] = perf[c].fillna(0)
    perf["cpc"] = perf.apply(lambda x: x["cost"] / x["clicks"] if x["clicks"] > 0 else 0, axis=1)
    perf["acos"] = perf.apply(lambda x: x["cost"] / x["sales"] if x["sales"] > 0 else 0, axis=1)
    perf["cr"] = perf.apply(lambda x: x["orders"] / x["clicks"] if x["clicks"] > 0 else 0, axis=1)
    return perf


def get_product_ads_data():
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT p.ad_id, p.asin, p.sku, p.state, p.serving_status,
                   p.campaign_id, s.campaign_name, p.ad_group_id,
                   p.creation_date, p.last_update_date, p.last_synced
            FROM product_ads p
            LEFT JOIN campaign_settings s ON p.campaign_id = s.campaign_id
            """,
            conn,
        )
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

    if df.empty:
        return pd.DataFrame()
    for c in [
        "ad_id",
        "asin",
        "sku",
        "state",
        "serving_status",
        "campaign_id",
        "campaign_name",
        "ad_group_id",
        "creation_date",
        "last_update_date",
        "last_synced",
    ]:
        df[c] = df[c].fillna("")
    return df


def get_trend_data(start, end):
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            f"""
            SELECT date, SUM(cost) as cost, SUM(sales) as sales
            FROM campaign_reports WHERE date >= '{start}' AND date <= '{end}' GROUP BY date ORDER BY date
            """,
            conn,
        )
        return df.set_index("date")
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()


def get_product_settings():
    conn = get_db_connection()
    try:
        df = pd.read_sql_query("SELECT * FROM product_settings", conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()
    if df.empty:
        return df
    df["asin"] = df["asin"].fillna("")
    df["sku"] = df["sku"].fillna("")
    df["daily_budget"] = df["daily_budget"].fillna(0.0)
    df["target_acos"] = df["target_acos"].fillna(0.0)
    df["budget_flex"] = df["budget_flex"].fillna(0.0)
    df["is_star"] = df["is_star"].fillna(0).astype(bool)
    df["ai_enabled"] = df["ai_enabled"].fillna(1).astype(bool)
    return df


def save_product_settings(settings_df):
    if settings_df is None or settings_df.empty:
        return
    with db_write_lock():
        conn = get_db_connection()
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            for _, r in settings_df.iterrows():
                asin = str(r.get("asin", "")).strip()
                if not asin:
                    continue
                sku = str(r.get("sku", "")).strip()
                daily_budget = float(r.get("daily_budget", 0) or 0)
                target_acos = float(r.get("target_acos", 0) or 0)
                budget_flex = float(r.get("budget_flex", 0) or 0)
                is_star = 1 if bool(r.get("is_star", False)) else 0
                ai_enabled = 1 if bool(r.get("ai_enabled", True)) else 0
                conn.execute(
                    """
                    INSERT OR REPLACE INTO product_settings
                        (asin, sku, daily_budget, target_acos, budget_flex, is_star, ai_enabled, last_updated)
                    VALUES (?,?,?,?,?,?,?,?)
                    """,
                    (asin, sku, daily_budget, target_acos, budget_flex, is_star, ai_enabled, ts),
                )
            conn.commit()
        finally:
            conn.close()


def get_budget_groups():
    conn = get_db_connection()
    try:
        df = pd.read_sql_query("SELECT * FROM budget_groups", conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()
    if df.empty:
        return df
    df["group_name"] = df["group_name"].fillna("")
    df["total_budget"] = df["total_budget"].fillna(0.0)
    return df


def get_budget_group_items(group_name):
    if not group_name:
        return pd.DataFrame()
    conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            "SELECT group_name, asin, sku, weight FROM budget_group_items WHERE group_name=?",
            conn,
            params=(group_name,),
        )
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()
    if df.empty:
        return df
    df["asin"] = df["asin"].fillna("")
    df["sku"] = df["sku"].fillna("")
    df["weight"] = df["weight"].fillna(0.0)
    return df


def save_budget_group(group_name, total_budget):
    if not group_name:
        return
    with db_write_lock():
        conn = get_db_connection()
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            conn.execute(
                "INSERT OR REPLACE INTO budget_groups (group_name, total_budget, last_updated) VALUES (?,?,?)",
                (group_name, float(total_budget or 0), ts),
            )
            conn.commit()
        finally:
            conn.close()


def save_budget_group_items(group_name, items_df):
    if not group_name or items_df is None:
        return
    with db_write_lock():
        conn = get_db_connection()
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            conn.execute("DELETE FROM budget_group_items WHERE group_name=?", (group_name,))
            if not items_df.empty:
                for _, r in items_df.iterrows():
                    asin = str(r.get("asin", "")).strip()
                    if not asin:
                        continue
                    sku = str(r.get("sku", "")).strip()
                    weight = float(r.get("weight", 0) or 0)
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO budget_group_items
                            (group_name, asin, sku, weight, last_updated)
                        VALUES (?,?,?,?,?)
                        """,
                        (group_name, asin, sku, weight, ts),
                    )
            conn.commit()
        finally:
            conn.close()


def get_auto_negative_keywords(source=None):
    conn = get_db_connection()
    try:
        if source:
            df = pd.read_sql_query(
                "SELECT * FROM auto_negative_keywords WHERE source=?",
                conn,
                params=(source,),
            )
        else:
            df = pd.read_sql_query("SELECT * FROM auto_negative_keywords", conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()
    if df.empty:
        return df
    for col in ["campaign_id", "ad_group_id", "keyword_text", "match_type", "level", "source", "status"]:
        if col in df.columns:
            df[col] = df[col].fillna("")
    return df


def save_auto_negative_keywords(records):
    if not records:
        return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with db_write_lock():
        conn = get_db_connection()
        try:
            for r in records:
                if not r:
                    continue
                conn.execute(
                    """
                    INSERT INTO auto_negative_keywords
                        (campaign_id, ad_group_id, keyword_text, match_type, level, source, status,
                         created_at, last_updated, reason, cost, sales, orders, clicks)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(campaign_id, ad_group_id, keyword_text, match_type, level, source)
                    DO UPDATE SET
                        status=excluded.status,
                        last_updated=excluded.last_updated,
                        reason=excluded.reason,
                        cost=excluded.cost,
                        sales=excluded.sales,
                        orders=excluded.orders,
                        clicks=excluded.clicks
                    """,
                    (
                        str(r.get("campaign_id", "") or ""),
                        str(r.get("ad_group_id", "") or ""),
                        str(r.get("keyword_text", "") or ""),
                        str(r.get("match_type", "") or ""),
                        str(r.get("level", "") or ""),
                        str(r.get("source", "") or "AI"),
                        str(r.get("status", "") or ""),
                        str(r.get("created_at", "") or ts),
                        ts,
                        str(r.get("reason", "") or ""),
                        r.get("cost", None),
                        r.get("sales", None),
                        r.get("orders", None),
                        r.get("clicks", None),
                    ),
                )
            conn.commit()
        finally:
            conn.close()


def update_auto_negative_status(rows, status):
    if not rows:
        return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with db_write_lock():
        conn = get_db_connection()
        try:
            for r in rows:
                if not r:
                    continue
                conn.execute(
                    """
                    UPDATE auto_negative_keywords
                    SET status=?, last_updated=?
                    WHERE campaign_id=? AND ad_group_id=? AND keyword_text=? AND match_type=? AND level=? AND source=?
                    """,
                    (
                        status,
                        ts,
                        str(r.get("campaign_id", "") or ""),
                        str(r.get("ad_group_id", "") or ""),
                        str(r.get("keyword_text", "") or ""),
                        str(r.get("match_type", "") or ""),
                        str(r.get("level", "") or ""),
                        str(r.get("source", "") or "AI"),
                    ),
                )
            conn.commit()
        finally:
            conn.close()


def get_negative_product_targets(source=None):
    conn = get_db_connection()
    try:
        if source:
            df = pd.read_sql_query(
                "SELECT * FROM negative_product_targets WHERE source=?",
                conn,
                params=(source,),
            )
        else:
            df = pd.read_sql_query("SELECT * FROM negative_product_targets", conn)
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()
    if df.empty:
        return df
    for col in ["campaign_id", "ad_group_id", "asin", "expression_type", "level", "source", "status"]:
        if col in df.columns:
            df[col] = df[col].fillna("")
    return df


def save_negative_product_targets(records):
    if not records:
        return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with db_write_lock():
        conn = get_db_connection()
        try:
            for r in records:
                if not r:
                    continue
                conn.execute(
                    """
                    INSERT OR REPLACE INTO negative_product_targets
                        (campaign_id, ad_group_id, asin, expression_type, level, source, status, created_at, last_updated)
                    VALUES (?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        str(r.get("campaign_id", "") or ""),
                        str(r.get("ad_group_id", "") or ""),
                        str(r.get("asin", "") or ""),
                        str(r.get("expression_type", "") or ""),
                        str(r.get("level", "") or ""),
                        str(r.get("source", "") or "manual"),
                        str(r.get("status", "") or ""),
                        str(r.get("created_at", "") or ts),
                        ts,
                    ),
                )
            conn.commit()
        finally:
            conn.close()


def update_negative_product_status(rows, status):
    if not rows:
        return
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with db_write_lock():
        conn = get_db_connection()
        try:
            for r in rows:
                if not r:
                    continue
                conn.execute(
                    """
                    UPDATE negative_product_targets
                    SET status=?, last_updated=?
                    WHERE campaign_id=? AND ad_group_id=? AND asin=? AND expression_type=? AND level=? AND source=?
                    """,
                    (
                        status,
                        ts,
                        str(r.get("campaign_id", "") or ""),
                        str(r.get("ad_group_id", "") or ""),
                        str(r.get("asin", "") or ""),
                        str(r.get("expression_type", "") or ""),
                        str(r.get("level", "") or ""),
                        str(r.get("source", "") or "manual"),
                    ),
                )
            conn.commit()
        finally:
            conn.close()
