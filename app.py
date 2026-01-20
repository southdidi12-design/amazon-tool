import streamlit as st
import requests
import pandas as pd
import sqlite3
from datetime import datetime, timedelta, date
import hashlib
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import os
import traceback
import json
import threading
import re

# === 🌟 HNV Amazon ERP - V49.0 (耐心等待版) ===
VERSION = "V49.0 (延长等待时间至3分钟)"

st.set_page_config(layout="wide", page_title=f"HNV ERP - {VERSION}")

# --- 1. 基础设施 ---
DB_FILE = 'hnv_erp_v49.db'

def get_db_connection():
    return sqlite3.connect(DB_FILE, check_same_thread=False, timeout=30)

def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS campaign_reports
                 (date TEXT, campaign_id TEXT, campaign_name TEXT,
                  cost REAL, sales REAL, clicks INTEGER, impressions INTEGER,
                  UNIQUE(date, campaign_id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS campaign_settings
                 (campaign_id TEXT PRIMARY KEY, campaign_name TEXT,
                  current_budget REAL, current_status TEXT, last_updated TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS system_logs (key TEXT PRIMARY KEY, value TEXT)''')
    conn.commit()
    return conn

def get_real_today():
    return datetime.now().date()

def get_retry_session():
    s = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def get_api_host(region):
    if "欧洲" in region: return "https://advertising-api-eu.amazon.com"
    if "远东" in region: return "https://advertising-api-fe.amazon.com"
    return "https://advertising-api.amazon.com"

# --- 2. 核心：智能下载器 (超级耐心版) ---
def smart_download_report(session, api_host, headers, d_str):
    try:
        print(f"[后台] 请求: {d_str} ... ", end="", flush=True)

        req = session.post(f"{api_host}/reporting/reports", headers=headers, json={
            "startDate": d_str, "endDate": d_str,
            "configuration": {
                "adProduct": "SPONSORED_PRODUCTS", "groupBy": ["campaign"],
                "columns": ["campaignId", "campaignName", "cost", "sales1d", "clicks", "impressions"],
                "reportTypeId": "spCampaigns", "timeUnit": "DAILY", "format": "GZIP_JSON"
            }
        }, timeout=30)

        rid = None

        # A. 正常成功
        if req.status_code in [200, 201, 202]:
            rid = req.json().get('reportId')

        # B. 重复请求 (425)
        elif req.status_code == 425 or "duplicate" in req.text.lower():
            try:
                detail = req.json().get('detail', '')
                if ':' in detail: rid = detail.split(':')[-1].strip()
            except:
                match = re.search(r'duplicate of : ([a-zA-Z0-9-]+)', req.text)
                if match: rid = match.group(1)

            if rid: print(f"♻️ 复用ID ... ", end="")
            else:
                print(f"❌ 无法提取旧ID")
                return False

        else:
            print(f"❌ 失败 ({req.status_code})")
            return False

        # === 超级耐心的下载循环 ===
        if rid:
            success = False
            rows_count = 0

            # 修改点：循环 90 次，每次 2 秒 = 3 分钟
            for i in range(90):
                time.sleep(2)

                # 打印小点点，让用户知道还在跑
                if i % 5 == 0: print(".", end="", flush=True)

                chk = session.get(f"{api_host}/reporting/reports/{rid}", headers=headers)
                status = chk.json().get('status')

                if status == 'COMPLETED':
                    url = chk.json().get('url')
                    if url:
                        data = pd.read_json(url, compression='gzip')
                        rows_count = len(data)
                        if not data.empty:
                            db = sqlite3.connect(DB_FILE, timeout=30)
                            data.columns = [c.lower() for c in data.columns]
                            for _, r in data.iterrows():
                                db.execute("INSERT OR REPLACE INTO campaign_reports VALUES (?,?,?,?,?,?,?)",
                                            (d_str, str(r['campaignid']), r.get('campaignname',''), r.get('cost',0), r.get('sales1d',0), r.get('clicks',0), r.get('impressions',0)))
                            db.execute("INSERT OR REPLACE INTO system_logs VALUES (?,?)", ('last_bg_msg', f"✅ {d_str} ({rows_count}条)"))
                            db.commit()
                            db.close()
                    success = True
                    break
                elif status == 'FAILURE':
                    print("❌ 生成失败", end="")
                    break

            if success:
                print(f" ✅ 完成 ({rows_count} 条)")
                return True
            else:
                print(" ⏳ 3分钟超时")
                return False

    except Exception as e:
        print(f"❌ 异常: {e}")
        return False

# --- 3. 后台矿工 ---
def background_history_hunter():
    while True:
        try:
            if "amazon" in st.secrets:
                conf = st.secrets["amazon"]
                region = "北美 (US/CA/MX)"
                api_host = get_api_host(region)
                session = get_retry_session()

                # Token
                try:
                    r = session.post("https://api.amazon.com/auth/o2/token", data={
                        "grant_type": "refresh_token", "refresh_token": conf["refresh_token"],
                        "client_id": conf["client_id"], "client_secret": conf["client_secret"]
                    }, timeout=20)
                except: time.sleep(60); continue

                if r.status_code != 200:
                    print(f"[后台] ❌ Token error: {r.status_code}"); time.sleep(300); continue

                token = r.json()['access_token']
                headers = {"Authorization": f"Bearer {token}", "Amazon-Advertising-API-ClientId": conf["client_id"], "Amazon-Advertising-API-Scope": conf["profile_id"], "Content-Type": "application/json"}

                today = get_real_today()

                print("\n[后台] 开始新一轮补全 (回溯60天)...")

                for i in range(1, 61):
                    target_date = today - timedelta(days=i)
                    d_str = target_date.strftime('%Y-%m-%d')

                    db = sqlite3.connect(DB_FILE, timeout=30)
                    try: count = pd.read_sql(f"SELECT COUNT(*) FROM campaign_reports WHERE date='{d_str}'", db).iloc[0,0]
                    except: count = 0
                    db.close()

                    # 策略：前3天必刷，3天后只补缺
                    if i <= 3 or count == 0:
                        smart_download_report(session, api_host, headers, d_str)
                        time.sleep(1)

                print("[后台] 本轮结束，休息 30 分钟...")
                time.sleep(1800)
        except: time.sleep(60)

@st.cache_resource
def start_history_hunter():
    t = threading.Thread(target=background_history_hunter, daemon=True)
    t.start()
    return t

# --- 4. 手动同步 ---
def sync_manual(target_date_obj, region):
    log = st.status(f"🚀 手动抓取 {target_date_obj}...", expanded=True)
    conf = st.secrets["amazon"]
    session = get_retry_session()

    try:
        r = session.post("https://api.amazon.com/auth/o2/token", data={
            "grant_type": "refresh_token", "refresh_token": conf["refresh_token"],
            "client_id": conf["client_id"], "client_secret": conf["client_secret"]
        }, timeout=30)
        token = r.json()['access_token']

        # 配置
        api_host = get_api_host(region)
        headers_v3 = {"Authorization": f"Bearer {token}", "Amazon-Advertising-API-ClientId": conf["client_id"], "Amazon-Advertising-API-Scope": conf["profile_id"], "Content-Type": "application/vnd.spCampaign.v3+json", "Accept": "application/vnd.spCampaign.v3+json"}
        try:
            list_res = session.post(f"{api_host}/sp/campaigns/list", headers=headers_v3, json={}, timeout=30)
            if list_res.status_code == 200:
                db = get_db_connection(); cur = db.cursor()
                camp_list = list_res.json().get('campaigns', [])
                ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                for camp in camp_list:
                    cur.execute("INSERT OR REPLACE INTO campaign_settings VALUES (?,?,?,?,?)",
                                (str(camp.get('campaignId')), camp.get('name'), camp.get('budget', {}).get('budget', 0), camp.get('state'), ts))
                db.commit(); db.close()
                log.write(f"✅ 更新 {len(camp_list)} 个配置")
        except: pass

        # 报表
        headers_report = {"Authorization": f"Bearer {token}", "Amazon-Advertising-API-ClientId": conf["client_id"], "Amazon-Advertising-API-Scope": conf["profile_id"], "Content-Type": "application/json"}
        d = target_date_obj.strftime('%Y-%m-%d')

        log.write(f"正在下载 {d} (请耐心等待，可能需要1-2分钟)...")
        if smart_download_report(session, api_host, headers_report, d):
            log.update(label="✅ 同步成功！", state="complete")
            time.sleep(1)
            st.rerun()
        else:
            log.update(label="❌ 同步失败 (超时或报错)", state="error")

    except Exception as e: st.error(f"Error: {e}")

# --- 5. 数据展示 ---
def get_merged_data(start, end):
    conn = get_db_connection()
    perf = pd.read_sql_query(f"SELECT campaign_id, SUM(cost) as cost, SUM(sales) as sales FROM campaign_reports WHERE date >= '{start}' AND date <= '{end}' GROUP BY campaign_id", conn)
    sett = pd.read_sql_query("SELECT * FROM campaign_settings", conn)
    try: last_msg = pd.read_sql("SELECT value FROM system_logs WHERE key='last_bg_msg'", conn).iloc[0,0]
    except: last_msg = "启动中..."
    try:
        min_d = pd.read_sql("SELECT MIN(date) FROM campaign_reports", conn).iloc[0,0]
        max_d = pd.read_sql("SELECT MAX(date) FROM campaign_reports", conn).iloc[0,0]
        db_range = f"{min_d} ~ {max_d}"
    except: db_range = "空"
    conn.close()

    if perf.empty:
        if sett.empty: return pd.DataFrame(), last_msg, db_range
        m = sett.copy(); m['cost']=0.0; m['sales']=0.0
    else:
        m = pd.merge(sett, perf, on='campaign_id', how='left')
    m['cost'] = m['cost'].fillna(0.0)
    m['sales'] = m['sales'].fillna(0.0)
    m['current_budget'] = m['current_budget'].fillna(0.0)
    m['current_status'] = m['current_status'].fillna('UNKNOWN')
    m['campaign_name'] = m['campaign_name'].fillna('Unknown')
    return m, last_msg, db_range

def ai_inference(row):
    rec="⚪ 保持"; bud=row['current_budget']
    if row['current_status']!='ENABLED': return "⏸️ 非运行", bud
    acos = row['cost']/row['sales'] if row['sales']>0 else 0
    if row['sales']>0:
        if acos>0.4: rec="📉 降价"; bud*=0.85
        elif acos<0.2: rec="🚀 拓量"; bud*=1.2
    elif row['cost']>20: rec="🛑 止损"; bud*=0.8
    return rec, round(bud, 2)

def execute_batch(token, rows, region):
    conf = st.secrets["amazon"]
    host = get_api_host(region)
    h = {"Authorization": f"Bearer {token}", "Amazon-Advertising-API-ClientId": conf["client_id"], "Amazon-Advertising-API-Scope": conf["profile_id"], "Content-Type": "application/json"}
    payload = [{"campaignId": str(r['campaign_id']), "dailyBudget": float(r['new_budget'])} for _, r in rows.iterrows()]
    try:
        return requests.put(f"{host}/v2/sp/campaigns", headers=h, json=payload, timeout=20).ok
    except: return False

# --- 6. 界面 ---
init_db()
start_history_hunter()

if 'logged_in' not in st.session_state: st.session_state.logged_in = False

if not st.session_state.logged_in:
    st.title("🔐 HNV ERP 登录")
    st.session_state.region = st.selectbox("🌍 区域", ["北美 (US/CA/MX)", "欧洲 (EU)", "远东 (JP/AU)"])
    if st.button("进入", type="primary"): st.session_state.logged_in=True; st.rerun()
else:
    smart_today = get_real_today()
    default_date = smart_today - timedelta(days=1)

    c1, c2, c3 = st.columns([2, 2, 1])
    with c1: st.title("🚀 广告看板 V49")
    with c2: display_date = st.date_input("📅 查看日期", value=default_date)
    with c3:
        if st.button("🔄 手动刷新"): sync_manual(display_date, st.session_state.region)

    end_date = display_date
    start_date = (end_date - timedelta(days=6))

    df, last_msg, db_info = get_merged_data(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

    st.success(f"🤖 **后台状态**: {last_msg} | 🗄️ **数据库**: {db_info}")

    if df.empty:
        st.warning("⏳ 正在等待数据下载... 请看黑框框里的进度点 '.'")
    else:
        st.metric("总花费 (7天累计)", f"${df['cost'].sum():.2f}")
        df['ACOS'] = df.apply(lambda x: x['cost']/x['sales'] if x['sales']>0 else 0, axis=1)
        ai_res = df.apply(ai_inference, axis=1, result_type='expand')
        df[['ai_suggestion', 'new_budget']] = ai_res
        df['审批'] = False
        df = df.sort_values(by=['cost'], ascending=False)

        edited = st.data_editor(
            df[['审批','campaign_name','current_status','cost','sales','ACOS','current_budget','ai_suggestion','new_budget','campaign_id']],
            column_config={
                "审批": st.column_config.CheckboxColumn("执行?", width="small"),
                "campaign_name": st.column_config.TextColumn("广告活动", width="medium"),
                "current_budget": st.column_config.NumberColumn("当前($)", format="$%.2f"),
                "cost": st.column_config.NumberColumn("花费", format="$%.2f"),
                "sales": st.column_config.NumberColumn("销售", format="$%.2f"),
                "ACOS": st.column_config.NumberColumn("ACOS", format="%.2f"),
                "new_budget": st.column_config.NumberColumn("建议预算($)", format="$%.2f"),
                "ai_suggestion": st.column_config.TextColumn("AI建议"),
                "campaign_id": None
            },
            hide_index=True, use_container_width=True, height=600
        )

        if st.button("🚀 批量执行"):
            to_do = edited[edited['审批']==True]
            if not to_do.empty:
                conf = st.secrets["amazon"]
                sess = get_retry_session()
                tk = sess.post("https://api.amazon.com/auth/o2/token", data={"grant_type": "refresh_token", "refresh_token": conf["refresh_token"], "client_id": conf["client_id"], "client_secret": conf["client_secret"]}).json()['access_token']
                if execute_batch(tk, to_do, st.session_state.region):
                    st.success("成功"); time.sleep(1); st.rerun()
                else: st.error("失败")