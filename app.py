import streamlit as st
import requests
import pandas as pd
import time
import os
import json
from datetime import datetime, timedelta

# === 🌟 HNV Amazon CFO - V8.0 (AI 参谋版) ===
VERSION = "V8.0 (AI 决策大脑)"

st.set_page_config(layout="wide", page_title=f"HNV Amazon {VERSION}")
st.title(f"🧠 HNV Amazon AI 广告投手 - {VERSION}")

# === 0. 基础设置 ===
if not os.path.exists('reports'): os.makedirs('reports')

# === 1. 侧边栏：策略与区域 ===
st.sidebar.header("🌍 1. 店铺区域")
region_name = st.sidebar.selectbox("选择区域:", ["北美 (美国/加拿大/墨西哥)", "欧洲", "远东"])

if "北美" in region_name: API_HOST = "https://advertising-api.amazon.com"
elif "欧洲" in region_name: API_HOST = "https://advertising-api-eu.amazon.com"
elif "远东" in region_name: API_HOST = "https://advertising-api-fe.amazon.com"

st.sidebar.markdown("---")
st.sidebar.header("🤖 2. AI 投放策略设置")

# === 用户设定的目标 ===
TARGET_ACOS = st.sidebar.slider("🎯 目标 ACOS (%)", 5, 100, 30) / 100
MAX_SPEND_NO_SALE = st.sidebar.number_input("💸 0出单最大容忍花费 ($)", value=10.0, step=1.0)
BID_AGGRESSIVENESS = st.sidebar.selectbox("🚀 调价激进程度", ["保守 (每次调5%)", "稳健 (每次调10%)", "激进 (每次调20%)"])

# 确定调价幅度
if "保守" in BID_AGGRESSIVENESS: ADJ_RATE = 0.05
elif "稳健" in BID_AGGRESSIVENESS: ADJ_RATE = 0.10
else: ADJ_RATE = 0.20

# === 2. 配置读取与 API ===
try:
    CLIENT_ID = st.secrets["amazon"]["client_id"]
    CLIENT_SECRET = st.secrets["amazon"]["client_secret"]
    REFRESH_TOKEN = st.secrets["amazon"]["refresh_token"]
    PROFILE_ID = st.secrets["amazon"]["profile_id"]
except:
    st.error("❌ 配置文件读取失败")
    st.stop()

def get_access_token():
    url = "https://api.amazon.com/auth/o2/token"
    data = {"grant_type": "refresh_token", "refresh_token": REFRESH_TOKEN, "client_id": CLIENT_ID, "client_secret": CLIENT_SECRET}
    try:
        res = requests.post(url, data=data)
        return res.json()['access_token'] if res.status_code == 200 else None
    except: return None

# === 3. 数据获取 (复用 V7 逻辑) ===
def request_report_v3(access_token):
    # 这里我们拉取过去 7 天的数据，因为调广告看一天的数据不准
    end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    url = f"{API_HOST}/reporting/reports"
    headers = {"Authorization": f"Bearer {access_token}", "Amazon-Advertising-API-ClientId": CLIENT_ID, "Amazon-Advertising-API-Scope": PROFILE_ID, "Content-Type": "application/json"}
    
    # 强制新报表策略：加入 random 因子或微调列顺序
    payload = {
        "startDate": start_date,
        "endDate": end_date,
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["campaign"],
            "columns": ["cost", "sales1d", "purchases1d", "clicks", "impressions"],
            "reportTypeId": "spCampaigns",
            "timeUnit": "SUMMARY", # 注意：我们要汇总数据来做决策
            "format": "GZIP_JSON"
        }
    }
    st.info(f"📡 正在拉取过去7天 ({start_date} ~ {end_date}) 的数据进行分析...")
    res = requests.post(url, headers=headers, json=payload)
    
    if res.status_code in [200, 202]: return res.json()['reportId']
    elif res.status_code == 425:
        try: return res.json().get('detail', '').split(':')[-1].strip()
        except: return None
    return None

def wait_and_get_data(access_token, report_id):
    url = f"{API_HOST}/reporting/reports/{report_id}"
    headers = {"Authorization": f"Bearer {access_token}", "Amazon-Advertising-API-ClientId": CLIENT_ID, "Amazon-Advertising-API-Scope": PROFILE_ID}
    
    progress = st.progress(0)
    for i in range(100):
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            status = res.json().get('status')
            progress.progress(min(i+1, 100))
            if status == 'COMPLETED':
                download_url = res.json().get('url')
                return pd.read_json(download_url, compression='gzip')
        time.sleep(2)
    return pd.DataFrame()

def get_campaign_names(access_token):
    url = f"{API_HOST}/v2/campaigns"
    headers = {"Authorization": f"Bearer {access_token}", "Amazon-Advertising-API-ClientId": CLIENT_ID, "Amazon-Advertising-API-Scope": PROFILE_ID}
    try:
        res = requests.get(url, headers=headers, params={"stateFilter": "enabled,paused", "count": 100})
        return {item['campaignId']: item['name'] for item in res.json()} if res.status_code == 200 else {}
    except: return {}

# === 4. 🧠 AI 核心算法 ===
def analyze_and_optimize(df, target_acos, max_loss):
    """
    这是 AI 的大脑：根据数据生成建议
    """
    suggestions = []
    
    for index, row in df.iterrows():
        spend = row['花费($)']
        sales = row['销售额($)']
        acos = row['ACOS_Value']
        name = row['广告活动']
        
        action = "保持"
        reason = "数据正常"
        color = "white"
        
        # 1. 🟥 止损逻辑：花费超过容忍值且 0 出单
        if sales == 0 and spend > max_loss:
            action = "🛑 强烈建议关停/否词"
            reason = f"0出单，花费已超 ${max_loss}"
            color = "#ffcccc" # 红色预警
            
        # 2. 🟨 降价逻辑：有出单，但 ACOS 高于目标
        elif sales > 0 and acos > target_acos:
            diff = acos - target_acos
            if diff > 0.2: # 高出 20%
                action = f"📉 建议降价/降预算 (大幅 -{int(ADJ_RATE*2*100)}%)"
            else:
                action = f"↘️ 建议微调降价 (-{int(ADJ_RATE*100)}%)"
            reason = f"当前 ACOS {acos*100:.1f}% > 目标 {target_acos*100:.0f}%"
            color = "#fff4cc" # 黄色警告
            
        # 3. 🟩 拓量逻辑：有出单，且 ACOS 优于目标 (表现好)
        elif sales > 0 and acos < target_acos and spend > 0:
            action = f"🚀 建议加预算/加价 (+{int(ADJ_RATE*100)}%)"
            reason = f"表现优异 (ACOS {acos*100:.1f}%)，可扩量"
            color = "#ccffcc" # 绿色利好
            
        suggestions.append({
            "广告活动": name,
            "花费": spend,
            "销售额": sales,
            "当前ACOS": f"{acos*100:.1f}%",
            "🤖 AI 建议操作": action,
            "决策理由": reason,
            "_color": color # 用于后续上色
        })
        
    return pd.DataFrame(suggestions)

# === 5. 主界面 ===
if st.button("🚀 启动 AI 诊断 (分析过去7天数据)", type="primary"):
    token = get_access_token()
    if token:
        report_id = request_report_v3(token)
        if report_id:
            raw_df = wait_and_get_data(token, report_id)
            if not raw_df.empty:
                # === 数据清洗 ===
                camp_map = get_campaign_names(token)
                if 'campaignId' in raw_df.columns:
                    raw_df['campaignName'] = raw_df['campaignId'].map(camp_map).fillna(raw_df['campaignId'].astype(str))
                
                rename = {'campaignName':'广告活动', 'cost':'花费($)', 'sales1d':'销售额($)'}
                df = raw_df.rename(columns={k:v for k,v in rename.items() if k in raw_df.columns})
                df = df.fillna(0)
                
                # 计算 ACOS 数值版 (用于计算)
                df['ACOS_Value'] = df.apply(lambda x: (x['花费($)']/x['销售额($)']) if x['销售额($)']>0 else 0, axis=1)
                
                # === 🧠 AI 开始工作 ===
                st.success("✅ 数据获取成功，AI 正在分析您的广告表现...")
                result_df = analyze_and_optimize(df, TARGET_ACOS, MAX_SPEND_NO_SALE)
                
                # === 展示结果 ===
                
                # 1. 🛑 需要紧急处理的 (红色)
                st.subheader("🚨 紧急警报 (建议立即处理)")
                urgent = result_df[result_df['_color'] == "#ffcccc"].drop(columns=['_color'])
                if not urgent.empty:
                    st.dataframe(urgent, use_container_width=True)
                else:
                    st.info("👏 很棒！没有发现严重亏损的广告活动。")

                # 2. 📉 需要优化的 (黄色)
                st.subheader("📉 优化建议 (ACOS 偏高)")
                optimize = result_df[result_df['_color'] == "#fff4cc"].drop(columns=['_color'])
                if not optimize.empty:
                    st.dataframe(optimize, use_container_width=True)

                # 3. 🚀 潜力股 (绿色)
                st.subheader("🚀 潜力爆款 (建议加注)")
                good = result_df[result_df['_color'] == "#ccffcc"].drop(columns=['_color'])
                if not good.empty:
                    st.dataframe(good, use_container_width=True)
                    
            else:
                st.warning("暂无数据")