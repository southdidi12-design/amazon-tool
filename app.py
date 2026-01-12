import streamlit as st
import pandas as pd
import io
import requests
import json

# === 网页配置 ===
st.set_page_config(page_title="亚马逊广告全能王 (修复版)", layout="wide", page_icon="🛡️")
st.title("🛡️ Amazon 广告优化 (稳如泰山版)")
st.info("💡 已移除外部绘图依赖，修复了 'matplotlib' 报错问题，保证流畅运行！")

# === 侧边栏 ===
st.sidebar.header("🔑 AI 设置")
default_key = ""
deepseek_key = st.sidebar.text_input("DeepSeek API Key", value=default_key, type="password")
product_name = st.sidebar.text_input("产品名称", value="LED Makeup Mirror")

st.sidebar.header("⚙️ 竞价规则")
target_acos = st.sidebar.slider("🎯 目标 ACoS", 10, 60, 30) / 100

st.sidebar.markdown("---")
st.sidebar.info("📉 **否词策略**：\n列出所有 0 订单词，按花费从高到低排序。")

# === 上传区域 ===
st.write("---")
c1, c2 = st.columns(2)
with c1:
    file_bulk = st.file_uploader("📂 1. 拖入【批量操作表格】(Bulk)", type=['xlsx'], key="bulk")
with c2:
    file_term = st.file_uploader("📂 2. 拖入【搜索词报告】(Search Term)", type=['xlsx'], key="term")
st.write("---")

# === DeepSeek 函数 ===
def call_deepseek_analysis(api_key, product, neg_data, bid_data):
    url = "https://api.deepseek.com/chat/completions"
    prompt = f"""
    我是亚马逊卖家，产品是【{product}】。
    
    1. 【浪费资金排行榜 (0转化)】：
    {neg_data.to_string(index=False)}
    * 重点分析前3个花费最高的词，为什么不出单？（词义太宽？竞品太强？）
    * 是否建议立即否定？

    2. 【竞价优化建议】：
    {bid_data.to_string(index=False)}
    
    请用 Markdown 格式，给出直接的操作建议。
    """
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    data = {
        "model": "deepseek-chat",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.5
    }
    try:
        res = requests.post(url, headers=headers, json=data)
        if res.status_code == 200: return res.json()['choices'][0]['message']['content']
        return f"AI 报错: {res.text}"
    except Exception as e: return f"错误: {e}"

# ==========================================
# 1️⃣ 竞价优化 (Bulk File)
# ==========================================
ai_bid_summary = pd.DataFrame()

if file_bulk:
    try:
        dfs = pd.read_excel(file_bulk, sheet_name=None, engine='openpyxl')
        bulk_df = pd.DataFrame()

        for name, df in dfs.items():
            df.columns = df.columns.astype(str).str.strip()
            cols = df.columns.tolist()
            if ('花费' in cols or 'Spend' in cols) and ('关键词文本' in cols or 'Keyword Text' in cols):
                bulk_df = df
                break
        
        if bulk_df.empty:
            st.error("❌ Bulk 文件未识别到有效数据！")
        else:
            col_map = {
                '实体层级': 'Record Type', 'Record Type': 'Record Type',
                '广告活动名称（仅供参考）': 'Campaign', '广告活动名称': 'Campaign',
                '广告组名称（仅供参考）': 'Ad Group', '广告组名称': 'Ad Group',
                '匹配类型': 'Match Type', 'Match Type': 'Match Type',
                '关键词文本': 'Keyword', 'Keyword Text': 'Keyword',
                '竞价': 'Max Bid', 'Max Bid': 'Max Bid',
                '花费': 'Spend', 'Spend': 'Spend',
                '销量': 'Sales', 'Sales': 'Sales',
                '订单数量': 'Orders', 'Orders': 'Orders',
                '点击量': 'Clicks', 'Clicks': 'Clicks',
                '拓展商品投放编号': 'Targeting', '商品投放 ID': 'Targeting ID'
            }
            df_clean = bulk_df.rename(columns=col_map)
            df_clean = df_clean.loc[:, ~df_clean.columns.duplicated()]

            if 'Keyword' not in df_clean.columns: df_clean['Keyword'] = None
            df_clean['Target'] = df_clean['Keyword']
            if 'Targeting' in df_clean.columns: 
                df_clean['Target'] = df_clean['Target'].fillna(df_clean['Targeting'])

            for c in ['Spend', 'Sales', 'Orders', 'Max Bid']:
                if c in df_clean.columns: df_clean[c] = pd.to_numeric(df_clean[c], errors='coerce').fillna(0)

            df_clean['ACoS'] = df_clean['Spend'] / df_clean['Sales']
            df_clean['ACoS'] = df_clean['ACoS'].fillna(0)

            bad_bids = df_clean[(df_clean['Orders'] > 0) & (df_clean['ACoS'] > target_acos)].copy()
            bad_bids = bad_bids.sort_values(by='ACoS', ascending=False)

            st.subheader("1️⃣ 竞价优化建议 (ACoS 超标榜)")
            if not bad_bids.empty:
                show_cols = ['Campaign', 'Ad Group', 'Target', 'Match Type', 'Max Bid', 'Orders', 'Spend', 'ACoS']
                final_cols = [c for c in show_cols if c in bad_bids.columns]
                ai_bid_summary = bad_bids[final_cols].head(5)
                
                # 简单高亮 ACoS
                st.dataframe(bad_bids[final_cols].style.format({'ACoS': '{:.1%}', 'Spend': '{:.2f}', 'Max Bid': '{:.2f}'}), use_container_width=True)
            else:
                st.success("✅ 竞价控制良好。")

    except Exception as e:
        st.error(f"Bulk 读取错误: {e}")

# ==========================================
# 2️⃣ 否词排名 (Search Term Report)
# ==========================================
ai_waste_data = pd.DataFrame()

if file_term:
    try:
        term_df = pd.read_excel(file_term, engine='openpyxl')
        term_df.columns = term_df.columns.astype(str).str.strip()

        # 暴力匹配订单列
        order_col = None
        for col in term_df.columns:
            if "订单" in col or "Orders" in col:
                order_col = col
                break
        if order_col: term_df.rename(columns={order_col: 'Orders'}, inplace=True)

        st_col_map = {
            '客户搜索词': 'Search Term', 'Customer Search Term': 'Search Term',
            '广告活动名称': 'Campaign', 'Campaign Name': 'Campaign',
            '广告组名称': 'Ad Group', 'Ad Group Name': 'Ad Group',
            '花费': 'Spend', 'Spend': 'Spend',
            '点击量': 'Clicks', 'Clicks': 'Clicks'
        }
        term_df = term_df.rename(columns=st_col_map)
        term_df = term_df.loc[:, ~term_df.columns.duplicated()]

        for c in ['Spend', 'Orders', 'Clicks']:
             if c in term_df.columns: term_df[c] = pd.to_numeric(term_df[c], errors='coerce').fillna(0)

        if 'Orders' in term_df.columns:
            # 核心逻辑：0单，按花费排序
            zero_order_df = term_df[term_df['Orders'] == 0].copy()
            zero_order_df = zero_order_df.sort_values(by='Spend', ascending=False)
            
            top_waste = zero_order_df.head(20)
            ai_waste_data = top_waste.head(10)

            st.subheader("2️⃣ 浪费资金排行榜 (Top 20 0单词)")
            if not top_waste.empty:
                st.error(f"🚨 发现 {len(top_waste)} 个花费最高的不出单词！")
                
                show_cols = ['Campaign', 'Ad Group', 'Search Term', 'Spend', 'Clicks']
                final_cols = [c for c in show_cols if c in top_waste.columns]
                
                # ⚠️ 去掉了 background_gradient，改用简单的表格展示
                st.dataframe(
                    top_waste[final_cols].style.format({'Spend': '{:.2f}'}), 
                    use_container_width=True
                )
            else:
                st.info("没有 0 单的词。")
        else:
            st.error(f"❌ 找不到'订单'列。列名：{list(term_df.columns)}")

    except Exception as e:
        st.error(f"搜索词报告读取错误: {e}")

# === 3. AI 综合汇报 ===
if file_bulk and file_term:
    st.write("---")
    st.subheader("🤖 DeepSeek 毒舌诊断")
    if st.button("开始 AI 分析"):
        if not deepseek_key:
             st.error("Key 为空！")
        else:
            with st.spinner("AI 分析中..."):
                report = call_deepseek_analysis(deepseek_key, product_name, ai_waste_data, ai_bid_summary)
                st.markdown(report)

