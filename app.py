import streamlit as st
import requests
import pandas as pd
import time
import os
import json
from datetime import datetime, timedelta

# === 🌟 HNV Amazon CFO - V6.2 (耐心等待版) ===
VERSION = "V6.2 (增加等待时间)"

st.set_page_config(layout="wide", page_title=f"HNV Amazon {VERSION}")
st.title(f"🚀 HNV Amazon 广告指挥中心 - {VERSION}")

# === 0. 自动创建数据文件夹 ===
if not os.path.exists('reports'):
    os.makedirs('reports')

# === 1. 侧边栏：设置区域 ===
st.sidebar.header("⚙️ 系统设置")
region_name = st.sidebar.selectbox(
    "请选择店铺所在区域:",
    ["北美 (美国/加拿大/墨西哥)", "欧洲 (英/德/法/意/西)", "远东 (日本/澳洲/新加坡)"]
)

if "北美" in region_name:
    API_HOST = "https://advertising-api.amazon.com"
elif "欧洲" in region_name:
    API_HOST = "https://advertising-api-eu.amazon.com"
elif "远东" in region_name:
    API_HOST = "https://advertising-api-fe.amazon.com"

st.sidebar.info(f"当前连接: {API_HOST} (V3 API)")

# === 2. 读取配置 ===
try:
    CLIENT_ID = st.secrets["amazon"]["client_id"]
    CLIENT_SECRET = st.secrets["amazon"]["client_secret"]
    REFRESH_TOKEN = st.secrets["amazon"]["refresh_token"]
    PROFILE_ID = st.secrets["amazon"]["profile_id"]
except Exception as e:
    st.error(f"❌ 配置文件读取失败: {e}")
    st.stop()

# === 3. 核心功能 ===

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

def request_report_v3(access_token):
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    url = f"{API_HOST}/reporting/reports"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Amazon-Advertising-API-ClientId": CLIENT_ID,
        "Amazon-Advertising-API-Scope": PROFILE_ID,
        "Content-Type": "application/json"
    }
    payload = {
        "startDate": yesterday,
        "endDate": yesterday,
        "configuration": {
            "adProduct": "SPONSORED_PRODUCTS",
            "groupBy": ["campaign"],
            "columns": ["impressions", "clicks", "cost", "sales1d", "purchases1d"],
            "reportTypeId": "spCampaigns",
            "timeUnit": "DAILY",
            "format": "GZIP_JSON"
        }
    }
    st.info(f"📡 [V3 请求] 正在向 {region_name} 发送报表申请...")
    res = requests.post(url, headers=headers, json=payload)
    if res.status_code == 200 or res.status_code == 202: 
        report_id = res.json()['reportId']
        st.success(f"✅ 订单接收成功! ID: {report_id}")
        return report_id
    else:
        st.error(f"❌ 下单失败: {res.status_code}")
        st.code(res.text)
        return None

# 🔥 核心修改：增加了等待时间 (从30秒增加到3分钟)
def wait_for_report_v3(access_token, report_id):
    url = f"{API_HOST}/reporting/reports/{report_id}"
    headers = {"Authorization": f"Bearer {access_token}", "Amazon-Advertising-API-ClientId": CLIENT_ID, "Amazon-Advertising-API-Scope": PROFILE_ID}
    status_placeholder = st.empty()
    
    # 修改：循环 60 次，每次 3 秒 = 180秒 (3分钟)
    for i in range(60):
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            data = res.json()
            status = data.get('status')
            
            # 显示更详细的进度
            status_placeholder.info(f"⏳ 亚马逊后台处理中... 状态: {status} (已等待 {i*3} 秒)")
            
            if status == 'COMPLETED':
                status_placeholder.success("✅ 终于好啦！报表生成完毕！")
                return data.get('url')
            elif status == 'FAILURE': 
                st.error("❌ 报表生成失败，亚马逊那边出错了")
                return None
        # 休息3秒再问
        time.sleep(3)
        
    st.error("❌ 等待超过 3 分钟，亚马逊响应太慢，请稍后再试。")
    return None

def get_report_data_v3(location_url, access_token):
    try:
        return pd.read_json(location_url, compression='gzip')
    except Exception as e:
        st.error(f"❌ 数据解析失败: {e}")
        return pd.DataFrame()

def get_campaign_names_map(access_token):
    # 注意：这里用回 v2 拿名字，因为 v2 拿列表比较快且简单
    # 如果 v2 也拿不到，可能需要换 v3，但先试试混合双打
    url = f"{API_HOST}/v2/campaigns"
    headers = {"Authorization": f"Bearer {access_token}", "Amazon-Advertising-API-ClientId": CLIENT_ID, "Amazon-Advertising-API-Scope": PROFILE_ID}
    params = {"stateFilter": "enabled,paused,archived", "count": 100}
    res = requests.get(url, headers=headers, params=params)
    name_map = {}
    if res.status_code == 200:
        for item in res.json():
            name_map[item['campaignId']] = item['name']
    return name_map

# === 4. 主界面逻辑 ===
tab1, tab2 = st.tabs(["💰 昨日业绩 (V6.2)", "📂 历史数据"])

with tab1:
    st.header(f"昨日本地时间销售数据 ({VERSION})")
    st.caption(f"当前区域: {region_name}")
    
    if st.button("🚀 启动 (耐心版)", key="btn_v6_2"):
        token = get_access_token()
        if token:
            report_id = request_report_v3(token)
            if report_id:
                url = wait_for_report_v3(token, report_id)
                if url:
                    df = get_report_data_v3(url, token)
                    if not df.empty:
                        # 智能清洗
                        with st.spinner('正在同步广告活动名称...'):
                            try:
                                campaign_map = get_campaign_names_map(token)
                                if 'campaignId' in df.columns:
                                    df['campaignName'] = df['campaignId'].map(campaign_map)
                                    df['campaignName'] = df['campaignName'].fillna(df['campaignId'].astype(str))
                            except:
                                pass # 如果拿名字失败，不影响显示数据
                        
                        rename_map = {
                            'campaignName': '广告活动', 'campaign': '广告活动',
                            'cost': '花费($)', 'sales1d': '销售额($)', 
                            'purchases1d': '订单量', 'clicks': '点击', 'impressions': '曝光'
                        }
                        df = df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns})
                        df = df.fillna(0)
                        
                        if '花费($)' in df.columns and '销售额($)' in df.columns:
                            df['ACOS'] = df.apply(lambda x: (x['花费($)']/x['销售额($)']*100) if x['销售额($)']>0 else 0, axis=1)
                            df['ACOS'] = df['ACOS'].round(2).astype(str) + '%'
                            df['花费($)'] = df['花费($)'].round(2)
                            df['销售额($)'] = df['销售额($)'].round(2)
                            
                            # 整理列顺序
                            base_cols = ['广告活动', '花费($)', '销售额($)', 'ACOS', '订单量', '点击', '曝光']
                            final_cols = [c for c in base_cols if c in df.columns]
                            df = df[final_cols]
                            df = df.sort_values(by='花费($)', ascending=False)
                            
                            # 保存
                            yesterday_str = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
                            file_name = f"reports/report_{yesterday_str}.csv"
                            df.to_csv(file_name, index=False)
                            st.success(f"✅ 成功存档: {file_name}")
                            
                            # 展示
                            t_spend = df['花费($)'].sum()
                            t_sales = df['销售额($)'].sum()
                            t_acos = (t_spend/t_sales*100) if t_sales>0 else 0
                            c1,c2,c3 = st.columns(3)
                            c1.metric("总花费", f"${t_spend:.2f}")
                            c2.metric("总销售额", f"${t_sales:.2f}")
                            c3.metric("总ACOS", f"{t_acos:.2f}%")
                            st.dataframe(df)
                        else:
                            st.warning("数据列不完整")
                            st.write(df)
                    else:
                        st.warning("昨日无数据")

with tab2:
    st.header("📂 历史报表")
    if os.path.exists('reports'):
        files = [f for f in os.listdir('reports') if f.endswith('.csv')]
        if files:
            f = st.selectbox("选择日期:", files)
            if f:
                st.dataframe(pd.read_csv(f"reports/{f}"), use_container_width=True)