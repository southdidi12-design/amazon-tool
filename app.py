import streamlit as st
import requests
import pandas as pd
import time
import os
from datetime import datetime, timedelta

st.set_page_config(layout="wide", page_title="HNV 诊所")
st.title("👨‍⚕️ HNV 广告系统 - 诊断模式")

# === 0. 检查文件是否存在 ===
if os.path.exists(".streamlit/secrets.toml"):
    st.success("✅ 检测到 secrets.toml 文件存在")
else:
    st.error("❌ 找不到 .streamlit/secrets.toml 文件！请确认你把它放回去了吗？")
    st.stop()

# === 1. 读取配置 ===
try:
    CLIENT_ID = st.secrets["amazon"]["client_id"]
    CLIENT_SECRET = st.secrets["amazon"]["client_secret"]
    REFRESH_TOKEN = st.secrets["amazon"]["refresh_token"]
    PROFILE_ID = st.secrets["amazon"]["profile_id"]
    st.success(f"✅ 配置文件读取成功 (店铺ID: {PROFILE_ID})")
except Exception as e:
    st.error(f"❌ 配置文件内容有误: {e}")
    st.stop()

# === 2. 核心函数 (带详细日志) ===
def get_access_token():
    st.info("Wait... 正在尝试获取 Access Token...")
    url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    try:
        res = requests.post(url, data=data)
        if res.status_code == 200:
            st.success("✅ 成功拿到 Access Token!")
            return res.json()['access_token']
        else:
            st.error(f"❌ Token 获取失败! 状态码: {res.status_code}")
            st.code(res.text) # 把错误详情打印出来
            return None
    except Exception as e:
        st.error(f"❌ 网络请求直接报错: {e}")
        return None

def request_report(access_token):
    st.info("Wait... 正在向亚马逊申请昨日报表...")
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
    if res.status_code == 202:
        report_id = res.json()['reportId']
        st.success(f"✅ 下单成功！报表ID: {report_id}")
        return report_id
    else:
        st.error(f"❌ 申请报表失败: {res.status_code}")
        st.code(res.text)
        return None

def wait_for_report(access_token, report_id):
    url = f"https://advertising-api.amazon.com/v2/reports/{report_id}"
    headers = {"Authorization": f"Bearer {access_token}", "Amazon-Advertising-API-ClientId": CLIENT_ID, "Amazon-Advertising-API-Scope": PROFILE_ID}
    
    status_placeholder = st.empty()
    for i in range(15):
        res = requests.get(url, headers=headers)
        if res.status_code == 200:
            status = res.json().get('status')
            status_placeholder.info(f"⏳ 第 {i+1} 次查询状态: {status}")
            if status == 'SUCCESS':
                status_placeholder.success("✅ 报表生成完毕！")
                return res.json().get('location')
            elif status == 'FAILURE':
                st.error("❌ 亚马逊说报表生成失败 (FAILURE)")
                return None
        else:
            st.warning(f"查询状态时遇到小问题: {res.status_code}")
        time.sleep(2)
    st.error("❌ 等待超时了")
    return None

def get_report_data(location_url, access_token):
    st.info("⬇️ 正在下载数据...")
    headers = {"Authorization": f"Bearer {access_token}", "Amazon-Advertising-API-ClientId": CLIENT_ID}
    res = requests.get(location_url, headers=headers)
    if res.status_code == 200:
        return res.json()
    else:
        st.error(f"❌ 下载失败: {res.status_code}")
        return []

# === 主程序 ===
if st.button("🚀 点击开始全流程诊断"):
    token = get_access_token()
    if token:
        report_id = request_report(token)
        if report_id:
            download_url = wait_for_report(token, report_id)
            if download_url:
                data = get_report_data(download_url, token)
                if data:
                    st.success(f"🎉 成功拉取到 {len(data)} 条数据！")
                    df = pd.DataFrame(data)
                    st.write(df) # 直接把原始数据打印出来看看
                else:
                    st.warning("⚠️ 流程跑通了，但是返回的数据是空的 (Empty List)")