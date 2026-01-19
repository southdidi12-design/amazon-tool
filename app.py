import streamlit as st
import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta

# === 页面基本设置 ===
st.set_page_config(layout="wide", page_title="HNV 亚马逊指挥中心 V3")
st.title("🚀 HNV Amazon 广告指挥中心 (带存储版)")

# === 0. 自动创建数据文件夹 (新功能) ===
# 如果没有 'reports' 文件夹，就自动建一个，用来存 Excel/CSV
if not os.path.exists('reports'):
    os.makedirs('reports')

# === 1. 读取配置 ===
try:
    CLIENT_ID = st.secrets["amazon"]["client_id"]
    CLIENT_SECRET = st.secrets["amazon"]["client_secret"]
    REFRESH_TOKEN = st.secrets["amazon"]["refresh_token"]
    PROFILE_ID = st.secrets["amazon"]["profile_id"]
except Exception as e:
    st.error(f"❌ 配置文件读取失败: {e}")
    st.stop()

# === 2. 通用函数 ===
def get_access_token():
    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    try:
        res = requests.post(url, data=data)
        if res.status_code == 200: return res.json()['access_token']
        return None
    except: return None

# === 3. 业绩报告功能 ===
def request_report(access_token):
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    url = "https://advertising-api.amazon.com/v2/reports"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": CLIENT_ID,
        "Amazon-Advertising-API-Scope": PROFILE_ID,
        "Content-Type": "application/json"
    }
    payload = {
        "campaignType": "sponsoredProducts",
        "recordType": "campaigns",
        "reportDate": yesterday,
        "metrics": "campaignName,campaignId,impressions,clicks,cost,attributedSales1d,attributedUnitsOrdered1d"
    }
    res = requests.post(url, headers=headers, json=payload)
    if res.status_code == 202: return res.json()['reportId']
    return None

def wait_for_report(access_token, report_id):
    url = f"https://advertising-api.amazon.com/v2/reports/{report_id}"
    headers = {"Authorization": f"Bearer {access_token}", "Amazon-Advertising-API-ClientId": CLIENT_ID, "Amazon-Advertising-API-Scope": PROFILE_ID}
    status_placeholder = st.empty()
    for i in range(15):
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            status = res.json().get('status')
            status_placeholder.info(f"⏳ 报告生成中... {status} ({i*2}s)")
            if status == 'SUCCESS':
                status_placeholder.success("✅ 报告就绪！")
                return res.json().get('location')
            elif status == 'FAILURE': return None
        time.sleep(2)
    return None

def get_report_data(location_url, access_token):
    headers = {"Authorization": f"Bearer {access_token}", "Amazon-Advertising-API-ClientId": CLIENT_ID}
    res = requests.get(location_url, headers=headers)
    return res.json() if res.status_code == 200 else []

# === 4. 广告列表功能 ===
def get_campaigns_list(access_token):
    url = "https://advertising-api.amazon.com/v2/campaigns"
    headers = {"Authorization": f"Bearer {access_token}", "Amazon-Advertising-API-ClientId": CLIENT_ID, "Amazon-Advertising-API-Scope": PROFILE_ID}
    params = {"stateFilter": "enabled,paused", "count": 50}
    res = requests.get(url, headers=headers, params=params)
    return res.json() if res.status_code == 200 else []

# === 5. 主界面逻辑 (3个Tab) ===
tab1, tab2, tab3 = st.tabs(["💰 昨日业绩 (自动存)", "📂 历史数据回看", "📝 广告状态管理"])

# --- Tab 1: 业绩 (带保存功能) ---
with tab1:
    st.header("昨日本地时间销售数据")
    if st.button("🚀 获取并保存数据", key="btn_report"):
        with st.spinner('正在连接亚马逊...'):
            token = get_access_token()
            if token:
                report_id = request_report(token)
                if report_id:
                    url = wait_for_report(token, report_id)
                    if url:
                        data = get_report_data(url, token)
                        if data:
                            df = pd.DataFrame(data)
                            # 数据清洗
                            rename = {'campaignName':'广告活动','cost':'花费($)','attributedSales1d':'销售额($)','clicks':'点击'}
                            df = df.rename(columns={k:v for k,v in rename.items() if k in df.columns})
                            df = df.fillna(0)
                            
                            # 算ACOS
                            if '花费($)' in df.columns and '销售额($)' in df.columns:
                                df['ACOS'] = df.apply(lambda x: (x['花费($)']/x['销售额($)']*100) if x['销售额($)']>0 else 0, axis=1)
                                df['ACOS_Value'] = df['ACOS'] # 留一个数字版用于计算
                                df['ACOS'] = df['ACOS'].round(2).astype(str) + '%'
                                df['花费($)'] = df['花费($)'].round(2)
                                df['销售额($)'] = df['销售额($)'].round(2)
                                df = df.sort_values(by='花费($)', ascending=False)
                                
                                # === 💾 核心新功能：保存到本地 ===
                                yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                                file_name = f"reports/report_{yesterday_str}.csv"
                                df.to_csv(file_name, index=False)
                                st.success(f"✅ 数据已自动保存到: {file_name}")
                                
                                # 展示
                                t_spend = df['花费($)'].sum()
                                t_sales = df['销售额($)'].sum()
                                t_acos = (t_spend/t_sales*100) if t_sales>0 else 0
                                c1,c2,c3 = st.columns(3)
                                c1.metric("总花费", f"${t_spend}")
                                c2.metric("总销售额", f"${t_sales}")
                                c3.metric("总ACOS", f"{t_acos:.2f}%")
                                st.dataframe(df)
                            else:
                                st.warning("数据列缺失")
                        else:
                            st.warning("昨日无数据")

# --- Tab 2: 历史记录 (新功能) ---
with tab2:
    st.header("📂 历史报表回溯")
    # 扫描 reports 文件夹里的文件
    if os.path.exists('reports'):
        files = [f for f in os.listdir('reports') if f.endswith('.csv')]
        if files:
            selected_file = st.selectbox("选择要查看的历史日期:", files)
            if selected_file:
                # 读取 CSV
                history_df = pd.read_csv(f"reports/{selected_file}")
                st.write(f"### 📅 {selected_file} 的数据")
                st.dataframe(history_df, use_container_width=True)
        else:
            st.info("📭 还没有存档记录，快去 Tab 1 点击获取数据吧！")
    else:
        st.info("📭 还没有创建数据文件夹。")

# --- Tab 3: 列表管理 ---
with tab3:
    st.header("所有 SP 广告活动状态")
    if st.button("🔄 刷新列表", key="btn_list"):
        with st.spinner('正在拉取...'):
            token = get_access_token()
            if token:
                campaigns = get_campaigns_list(token)
                if campaigns:
                    df = pd.DataFrame(campaigns)
                    cols = ['name', 'state', 'dailyBudget', 'targetingType']
                    exist_cols = [c for c in cols if c in df.columns]
                    st.dataframe(df[exist_cols], use_container_width=True)