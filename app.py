import streamlit as st
import pandas as pd
import io
import requests
import json

# === 网页配置 ===
st.set_page_config(page_title="亚马逊广告全能王 (策略执行版)", layout="wide", page_icon="⚖️")
st.title("⚖️ Amazon 广告优化全能王 (四大策略版)")
st.info("💡 当前执行策略：清理浪费、保护利润、降本增效、自动拓词")

# === 侧边栏设置 ===
st.sidebar.header("🔑 AI 设置")
# 默认Key已预填
default_key = "sk-55cc3f56742f4e43be099c9489e02911"
deepseek_key = st.sidebar.text_input("DeepSeek API Key", value=default_key, type="password")
product_name = st.sidebar.text_input("产品名称", value="LED Makeup Mirror")

st.sidebar.header("⚙️ 策略阈值微调")
waste_spend = st.sidebar.number_input("🗑️ 清理浪费: 花费超($)", value=20.0, step=5.0)
scale_acos = st.sidebar.slider("🚀 保护利润: ACoS 低于(%)", 5, 30, 20) / 100
scale_bid_inc = st.sidebar.number_input("📈 提价幅度", value=1.10, step=0.05, help="1.1 表示涨10%")

control_acos = st.sidebar.slider("📉 降本增效: ACoS 高于(%)", 20, 80, 40) / 100
control_bid_dec = st.sidebar.number_input("📉 降价幅度", value=0.85, step=0.05, help="0.85 表示降15%")

mining_orders = st.sidebar.number_input("⛏️ 拓词标准: 订单超过(单)", value=3, step=1)

# === 上传区域 ===
st.write("---")
c1, c2 = st.columns(2)
with c1:
    file_bulk = st.file_uploader("📂 1. 拖入【批量操作表格】(Bulk)", type=['xlsx'], key="bulk")
with c2:
    file_term = st.file_uploader("📂 2. 拖入【搜索词报告】(Search Term)", type=['xlsx'], key="term")
st.write("---")

# === DeepSeek 函数 ===
def call_deepseek_analysis(api_key, product, neg_data, bid_data, mining_data):
    url = "https://api.deepseek.com/chat/completions"
    prompt = f"""
    我是亚马逊卖家，产品是【{product}】。请根据我的四大策略分析数据：

    1. 【清理浪费 (建议否定)】：
    {neg_data.to_string(index=False)}
    * 点评这些词的不相关性。

    2. 【保护利润 & 降本增效 (竞价调整)】：
    {bid_data.to_string(index=False)}
    * 分析提价词的潜力，以及降价词的问题所在。

    3. 【拓词建议 (黑马词)】：
    {mining_data.to_string(index=False)}
    * 这些词值得打手动精准吗？为什么？

    请用 Markdown 格式，简练直接，给出专家级建议。
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

# === 1️⃣ 批量表格分析 (Scale Up & Cost Control) ===
ai_bid_summary = pd.DataFrame()

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
            st.error("❌ Bulk文件没找到数据表！请检查。")
        else:
            st.success(f"✅ 竞价策略执行中... (数据源: {found_sheet_name})")

            # 列名映射
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
                '展示量': 'Impressions', 'Impressions': 'Impressions',
                '点击量': 'Clicks', 'Clicks': 'Clicks',
                '拓展商品投放编号': 'Targeting', '商品投放 ID': 'Targeting ID'
            }
            df_clean = bulk_df.rename(columns=col_map)
            df_clean = df_clean.loc[:, ~df_clean.columns.duplicated()]

            # 筛选 Keyword / Product Targeting
            df_clean['Record Type'] = df_clean['Record Type'].astype(str)
            mask = df_clean['Record Type'].str.contains('关键词|Keyword|商品定向|Product Targeting', case=False, na=False)
            data_bid = df_clean[mask].copy()

            # 整理数据
            if 'Keyword' not in data_bid.columns: data_bid['Keyword'] = None
            data_bid['Target'] = data_bid['Keyword']
            if 'Targeting' in data_bid.columns: 
                data_bid['Target'] = data_bid['Target'].fillna(data_bid['Targeting'])

            for c in ['Spend', 'Sales', 'Orders', 'Max Bid', 'Clicks']:
                if c in data_bid.columns: data_bid[c] = pd.to_numeric(data_bid[c], errors='coerce').fillna(0)

            # 计算基础指标
            data_bid['ACoS'] = data_bid['Spend'] / data_bid['Sales']
            data_bid['ACoS'] = data_bid['ACoS'].fillna(0)
            data_bid['CVR'] = data_bid['Orders'] / data_bid['Clicks']
            data_bid['CVR'] = data_bid['CVR'].fillna(0)

            # === 计算广告组平均转化率 (Avg CVR) ===
            # 按广告组分组，计算转化率均值
            ad_group_cvr = data_bid.groupby('Ad Group')['CVR'].mean().reset_index()
            ad_group_cvr.rename(columns={'CVR': 'Avg_Group_CVR'}, inplace=True)
            
            # 合并回去
            data_bid = pd.merge(data_bid, ad_group_cvr, on='Ad Group', how='left')

            # === 策略实施 ===
            data_bid['策略动作'] = '保持'
            data_bid['建议新竞价'] = data_bid['Max Bid']

            # 规则 2: 保护利润 (Scale Up)
            # ACoS < 20% AND CVR > Avg_Group_CVR
            mask_scale = (data_bid['ACoS'] < scale_acos) & \
                         (data_bid['ACoS'] > 0) & \
                         (data_bid['CVR'] > data_bid['Avg_Group_CVR'])
            
            data_bid.loc[mask_scale, '策略动作'] = '🚀 提价扩量'
            data_bid.loc[mask_scale, '建议新竞价'] = data_bid.loc[mask_scale, 'Max Bid'] * scale_bid_inc

            # 规则 3: 降本增效 (Cost Control)
            # ACoS > 40% AND Orders > 2
            mask_control = (data_bid['ACoS'] > control_acos) & (data_bid['Orders'] > 2)
            
            data_bid.loc[mask_control, '策略动作'] = '📉 降价控制'
            data_bid.loc[mask_control, '建议新竞价'] = data_bid.loc[mask_control, 'Max Bid'] * control_bid_dec

            # 结果展示
            action_df = data_bid[data_bid['策略动作'] != '保持'].copy()
            
            # 准备给AI的数据
            ai_bid_summary = action_df[['Target', 'ACoS', 'CVR', '策略动作']].head(6)

            st.subheader("📊 竞价策略执行结果")
            if not action_df.empty:
                c_up, c_down = st.tabs(["🚀 需提价 (优质词)", "📉 需降价 (低效词)"])
                
                with c_up:
                    df_up = action_df[action_df['策略动作'].str.contains('提价')]
                    if not df_up.empty:
                        st.dataframe(df_up[['Campaign', 'Ad Group', 'Target', 'Max Bid', '建议新竞价', 'Orders', 'ACoS', 'CVR', 'Avg_Group_CVR']]
                                     .style.format({'ACoS': '{:.1%}', 'CVR': '{:.1%}', 'Avg_Group_CVR': '{:.1%}', 'Max Bid': '{:.2f}', '建议新竞价': '{:.2f}'}), 
                                     use_container_width=True)
                    else:
                        st.info("暂无满足【提价】条件的优质词。")

                with c_down:
                    df_down = action_df[action_df['策略动作'].str.contains('降价')]
                    if not df_down.empty:
                        st.dataframe(df_down[['Campaign', 'Ad Group', 'Target', 'Max Bid', '建议新竞价', 'Orders', 'ACoS']]
                                     .style.format({'ACoS': '{:.1%}', 'Max Bid': '{:.2f}', '建议新竞价': '{:.2f}'}), 
                                     use_container_width=True)
                    else:
                        st.info("暂无满足【降价】条件的词。")
            else:
                st.success("✅ 当前广告表现平稳，无需根据所设规则进行调整。")

    except Exception as e:
        st.error(f"Bulk处理错误: {e}")

# === 2️⃣ 搜索词报告分析 (Waste & Mining) ===
ai_waste_data = pd.DataFrame()
ai_mining_data = pd.DataFrame()

if file_term:
    try:
        term_df = pd.read_excel(file_term, engine='openpyxl')
        term_df.columns = term_df.columns.astype(str).str.strip()

        st_col_map = {
            '客户搜索词': 'Search Term', 'Customer Search Term': 'Search Term',
            '广告活动名称': 'Campaign', 'Campaign Name': 'Campaign',
            '广告组名称': 'Ad Group', 'Ad Group Name': 'Ad Group',
            '匹配类型': 'Match Type', 'Match Type': 'Match Type',
            '花费': 'Spend', 'Spend': 'Spend',
            '点击量': 'Clicks', 'Clicks': 'Clicks',
            '7天总订单数(#)': 'Orders', '7天总订单数': 'Orders', '订单数量': 'Orders',
            '广告投入产出比 (ACOS) 总计': 'ACoS'
        }
        term_df = term_df.rename(columns=st_col_map)
        term_df = term_df.loc[:, ~term_df.columns.duplicated()]

        for c in ['Spend', 'Orders', 'Clicks', 'ACoS']:
             if c in term_df.columns: term_df[c] = pd.to_numeric(term_df[c], errors='coerce').fillna(0)

        if 'Orders' in term_df.columns:
            # 规则 1: 清理浪费 (Negative Match)
            # Spend > $20 AND Orders = 0
            mask_waste = (term_df['Spend'] > waste_spend) & (term_df['Orders'] == 0)
            waste_df = term_df[mask_waste].copy()
            waste_df['建议操作'] = '添加否定精准'
            
            ai_waste_data = waste_df[['Search Term', 'Spend', 'Clicks']].head(5)

            # 规则 4: 拓词逻辑 (Keyword Mining)
            # Orders > 3 (且假设非精确匹配才提示，这里简单展示所有高转化词)
            mask_mining = (term_df['Orders'] >= mining_orders)
            # 排除已经是精确匹配的 (Match Type == Exact 或 -)
            # 注意：自动广告 Match Type 可能是 '-', 也可以拓。手动精准通常显示 'EXACT'
            # 这里简单起见，只要出单多，都列出来供人工审核
            mining_df = term_df[mask_mining].copy()
            mining_df['建议操作'] = '投放手动精准'
            
            ai_mining_data = mining_df[['Search Term', 'Orders', 'ACoS']].head(5)

            st.subheader("🛡️ 否词 & 拓词建议")
            t_neg, t_mine = st.tabs(["🗑️ 建议否定 (清理浪费)", "⛏️ 建议拓词 (黑马挖掘)"])

            with t_neg:
                if not waste_df.empty:
                    st.error(f"🚨 发现 {len(waste_df)} 个浪费资金的搜索词！")
                    st.dataframe(waste_df[['Campaign', 'Ad Group', 'Search Term', 'Spend', 'Clicks', '建议操作']]
                                 .style.format({'Spend': '{:.2f}'}), use_container_width=True)
                else:
                    st.success("✅ 没有发现花费超标且不出单的词。")

            with t_mine:
                if not mining_df.empty:
                    st.success(f"💎 发现 {len(mining_df)} 个高转化搜索词！")
                    st.dataframe(mining_df[['Campaign', 'Ad Group', 'Search Term', 'Orders', 'ACoS', '建议操作']], 
                                 use_container_width=True)
                else:
                    st.info(f"暂无订单数超过 {mining_orders} 的黑马词。")

    except Exception as e:
        st.error(f"搜索词报告错误: {e}")

# === 3. AI 综合汇报 ===
if file_bulk and file_term:
    st.write("---")
    st.subheader("🤖 DeepSeek 战略顾问")
    if st.button("生成战略分析报告"):
        if not deepseek_key:
             st.error("Key 为空！")
        else:
            with st.spinner("AI 正在根据四大策略分析全盘数据..."):
                report = call_deepseek_analysis(deepseek_key, product_name, ai_waste_data, ai_bid_summary, ai_mining_data)
                st.markdown(report)
