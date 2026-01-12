import streamlit as st
import pandas as pd
import io
import requests
import json

# === 网页配置 ===
st.set_page_config(page_title="亚马逊广告全能王 (精准定位版)", layout="wide", page_icon="🎯")
st.title("🎯 Amazon 广告优化全能王 (带广告组/匹配类型)")
st.info("💡 已升级：新增【广告组】和【匹配类型】列，精准定位每一个投放！")

# === 侧边栏设置 ===
st.sidebar.header("🔑 AI 设置")
deepseek_key = st.sidebar.text_input("DeepSeek API Key", type="password")
product_name = st.sidebar.text_input("产品名称", value="LED Makeup Mirror")

st.sidebar.header("⚙️ 竞价规则")
target_acos = st.sidebar.slider("🎯 目标 ACoS", 10, 60, 30) / 100

st.sidebar.header("🛡️ 否词规则")
neg_clicks = st.sidebar.number_input("🚫 否词阈值 (点击数)", value=10, step=1)

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
    我是亚马逊卖家，产品是【{product}】。请分析数据并给出建议：
    
    1. 【待否定搜索词 (点击多但0单)】：
    {neg_data.to_string(index=False)}
    * 结合匹配类型(Match Type)和广告组，分析这些词为什么跑偏？
    * 哪些词建议精准否定？

    2. 【需降价投放 (ACoS高)】：
    {bid_data.to_string(index=False)}
    * 简述优化建议。
    
    请用 Markdown 格式，简练直接。
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

# === 1️⃣ 竞价优化 (Bulk) ===
if file_bulk:
    try:
        dfs = pd.read_excel(file_bulk, sheet_name=None, engine='openpyxl')
        bulk_df = pd.DataFrame()
        found_sheet_name = ""

        # 精准找表
        for name, df in dfs.items():
            df.columns = df.columns.astype(str).str.strip()
            cols = df.columns.tolist()
            if ('实体层级' in cols or 'Record Type' in cols) and \
               ('关键词文本' in cols or 'Keyword Text' in cols):
                bulk_df = df
                found_sheet_name = name
                break
        
        if bulk_df.empty:
            st.error("❌ 没找到数据表！请检查文件。")
        else:
            st.success(f"✅ 竞价数据来源: 【{found_sheet_name}】")

            # === 详细列名映射 (含广告组和匹配类型) ===
            col_map = {
                '实体层级': 'Record Type', 'Record Type': 'Record Type',
                '广告活动名称（仅供参考）': 'Campaign', '广告活动名称': 'Campaign',
                '广告组名称（仅供参考）': 'Ad Group', '广告组名称': 'Ad Group', # 新增
                '匹配类型': 'Match Type', 'Match Type': 'Match Type', # 新增
                '关键词文本': 'Keyword', 'Keyword Text': 'Keyword',
                '竞价': 'Max Bid', 'Max Bid': 'Max Bid',
                '花费': 'Spend', 'Spend': 'Spend',
                '销量': 'Sales', 'Sales': 'Sales',
                '订单数量': 'Orders', 'Orders': 'Orders',
                '展示量': 'Impressions', 'Impressions': 'Impressions',
                '点击量': 'Clicks', 'Clicks': 'Clicks',
                '点击率': 'CTR', 'Click-through Rate': 'CTR',
                '转化率': 'CVR', 'Conversion Rate': 'CVR',
                '拓展商品投放编号': 'Targeting', '商品投放 ID': 'Targeting ID'
            }
            df_clean = bulk_df.rename(columns=col_map)
            df_clean = df_clean.loc[:, ~df_clean.columns.duplicated()]

            # 筛选
            df_clean['Record Type'] = df_clean['Record Type'].astype(str)
            mask = df_clean['Record Type'].str.contains('关键词|Keyword|商品定向|Product Targeting', case=False, na=False)
            data_bid = df_clean[mask].copy()

            # 整理投放目标列
            if 'Keyword' not in data_bid.columns: data_bid['Keyword'] = None
            data_bid['Target'] = data_bid['Keyword']
            if 'Targeting' in data_bid.columns: 
                data_bid['Target'] = data_bid['Target'].fillna(data_bid['Targeting'])
            
            # 转数字
            num_cols = ['Spend', 'Sales', 'Orders', 'Max Bid', 'Impressions', 'Clicks', 'CTR', 'CVR']
            for c in num_cols:
                if c in data_bid.columns: data_bid[c] = pd.to_numeric(data_bid[c], errors='coerce').fillna(0)
            
            data_bid['ACoS'] = data_bid['Spend'] / data_bid['Sales']
            data_bid['ACoS'] = data_bid['ACoS'].fillna(0)

            # 找出需要优化的行
            bad_bids = data_bid[(data_bid['Orders'] > 0) & (data_bid['ACoS'] > target_acos)].copy()
            bad_bids['建议新竞价'] = bad_bids['Max Bid'] * 0.85

            st.subheader("1️⃣ 竞价优化建议 (精确到广告组)")
            if not bad_bids.empty:
                # === 关键：调整列顺序，把广告组和匹配类型放在前面 ===
                show_cols = ['Campaign', 'Ad Group', 'Target', 'Match Type', 'Max Bid', '建议新竞价', 'Orders', 'ACoS', 'Spend', 'Sales', 'CVR']
                final_cols = [c for c in show_cols if c in bad_bids.columns]
                
                st.dataframe(
                    bad_bids[final_cols].style.format({
                        'ACoS': '{:.2%}', 'CVR': '{:.2%}',
                        'Spend': '{:.2f}', 'Sales': '{:.2f}',
                        'Max Bid': '{:.2f}', '建议新竞价': '{:.2f}'
                    }),
                    use_container_width=True
                )
            else:
                st.success("✅ 竞价表现良好。")

    except Exception as e:
        st.error(f"Bulk 文件错误: {e}")

# === 2️⃣ 否词优化 (Search Term) ===
neg_ai_data = pd.DataFrame()
if file_term:
    try:
        term_df = pd.read_excel(file_term, engine='openpyxl')
        term_df.columns = term_df.columns.astype(str).str.strip()

        # === 详细列名映射 ===
        st_col_map = {
            '客户搜索词': 'Search Term', 'Customer Search Term': 'Search Term',
            '广告活动名称': 'Campaign', 'Campaign Name': 'Campaign',
            '广告组名称': 'Ad Group', 'Ad Group Name': 'Ad Group', # 新增
            '匹配类型': 'Match Type', 'Match Type': 'Match Type', # 新增
            '投放': 'Targeting', 'Targeting': 'Targeting',
            '花费': 'Spend', 'Spend': 'Spend',
            '点击量': 'Clicks', 'Clicks': 'Clicks',
            '7天总订单数(#)': 'Orders', '7天总订单数': 'Orders', '订单数量': 'Orders',
            '每次点击成本(CPC)': 'CPC',
            '广告投入产出比 (ACOS) 总计': 'ACoS'
        }
        term_df = term_df.rename(columns=st_col_map)
        term_df = term_df.loc[:, ~term_df.columns.duplicated()]
        
        for c in ['Spend', 'Orders', 'Clicks', 'CPC', 'ACoS']:
             if c in term_df.columns: term_df[c] = pd.to_numeric(term_df[c], errors='coerce').fillna(0)

        if 'Orders' in term_df.columns and 'Clicks' in term_df.columns:
            neg_candidates = term_df[(term_df['Clicks'] >= neg_clicks) & (term_df['Orders'] == 0)].copy()
            neg_candidates = neg_candidates.sort_values(by='Spend', ascending=False)
            neg_ai_data = neg_candidates.head(10)

            st.subheader("2️⃣ 否词建议 (精确到广告组)")
            if not neg_candidates.empty:
                st.error(f"🚨 发现 {len(neg_candidates)} 个无效搜索词！")
                
                # === 关键：展示列包含广告组和匹配类型 ===
                st_show_cols = ['Campaign', 'Ad Group', 'Search Term', 'Match Type', 'Clicks', 'Spend', 'CPC', 'Targeting']
                st_final_cols = [c for c in st_show_cols if c in neg_candidates.columns]

                st.dataframe(
                    neg_candidates[st_final_cols].head(50).style.format({
                        'Spend': '{:.2f}', 'CPC': '{:.2f}'
                    }),
                    use_container_width=True
                )
                
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    neg_candidates.to_excel(writer, index=False)
                st.download_button("📥 下载详细否词表", output, "Negative_Keywords_Detailed.xlsx")
            else:
                st.success("✅ 搜索词很干净。")
        else:
            st.error(f"❌ 缺少必要列！检测到的列名：{list(term_df.columns)}")

    except Exception as e:
        st.error(f"搜索词报告错误: {e}")

# === 3. AI 分析 ===
if file_bulk and file_term:
    st.write("---")
    st.subheader("🤖 DeepSeek 综合诊断")
    if st.button("开始 AI 分析"):
        if not deepseek_key:
            st.error("请在左侧填入 Key")
        else:
            ai_bid_data = pd.DataFrame()
            if 'bad_bids' in locals() and not bad_bids.empty:
                ai_bid_data = bad_bids[['Ad Group', 'Target', 'Match Type', 'ACoS', 'Spend']].head(5)
            
            with st.spinner("AI 正在分析广告组结构..."):
                report = call_deepseek_analysis(deepseek_key, product_name, neg_ai_data, ai_bid_data)
                st.markdown(report)
