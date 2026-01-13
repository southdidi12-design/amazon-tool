import streamlit as st
import pandas as pd
import requests
import re

# === 1. 全局配置 (v3.0) ===
st.set_page_config(
    page_title="Amazon 广告指挥官 v3.0", 
    layout="wide", 
    page_icon="⚔️",
    initial_sidebar_state="expanded"
)

# 注入 CSS 美化
st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    h1 { color: #2c3e50; font-family: 'Helvetica Neue', sans-serif; }
    .stTabs [data-baseweb="tab-list"] { gap: 24px; }
    .stTabs [data-baseweb="tab"] { height: 50px; white-space: pre-wrap; background-color: white; border-radius: 5px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
    .stTabs [aria-selected="true"] { background-color: #e8f0fe; color: #1a73e8; border: 1px solid #1a73e8; }
    div[data-testid="stMetric"] { background-color: white; padding: 15px; border-radius: 10px; box-shadow: 0 2px 5px rgba(0,0,0,0.05); border: 1px solid #eee; }
</style>
""", unsafe_allow_html=True)

# === 2. 侧边栏：核心控制 ===
st.sidebar.title("⚙️ 控制中枢 v3.0")

# 🔐 安全提示：不要在代码里写死 Key，防止 GitHub 泄露
deepseek_key = st.sidebar.text_input("🔑 请输入 DeepSeek API Key", type="password", help="为了安全，Key 请每次手动输入，不要保存在代码里")
product_name = st.sidebar.text_input("📦 产品名称", value="Makeup Mirror")

st.sidebar.markdown("---")
st.sidebar.subheader("🎯 阈值设置")

# 否词规则
with st.sidebar.expander("🚫 否词/洗词规则", expanded=True):
    neg_spend_th = st.number_input("浪费: 花费 > ($)", value=5.0, step=1.0)
    neg_clicks_th = st.number_input("浪费: 点击 > (次)", value=10, step=1)

# 竞价规则
with st.sidebar.expander("💰 竞价规则", expanded=False):
    target_acos = st.slider("目标 ACoS (高于此降价)", 0.1, 1.0, 0.3)
    
# 黄金词规则
with st.sidebar.expander("🏆 黄金词规则", expanded=False):
    gold_cvr = st.slider("高转化: CVR > (%)", 5, 50, 15) / 100
    gold_acos = st.slider("低ACoS: ACoS < (%)", 5, 50, 20) / 100

# === 3. 主界面 & 数据加载 ===
st.title("⚔️ Amazon 广告指挥官 (v3.0 旗舰版)")
st.caption("🚀 数据加载完成 | API 接口就绪 | 安全模式已开启")

# 文件上传区
c1, c2 = st.columns(2)
with c1:
    file_bulk = st.file_uploader("📂 Bulk 表格 (竞价/广告位)", type=['xlsx', 'csv'], key="bulk")
with c2:
    file_term = st.file_uploader("📂 Search Term (否词/ASIN)", type=['xlsx', 'csv'], key="term")

# 数据读取函数
def load_data(file, file_type):
    if not file: return pd.DataFrame()
    try:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            if file_type == 'bulk':
                dfs = pd.read_excel(file, sheet_name=None, engine='openpyxl')
                for name, d in dfs.items():
                    if d.astype(str).apply(lambda x: x.str.contains('Keyword|关键词', case=False)).any().any():
                        return d
                return pd.DataFrame()
            else:
                df = pd.read_excel(file, engine='openpyxl')
        return df
    except Exception as e:
        st.error(f"读取错误: {e}")
        return pd.DataFrame()

df_bulk = load_data(file_bulk, 'bulk')
df_term = load_data(file_term, 'term')

# 清洗列名
if not df_bulk.empty: df_bulk.columns = df_bulk.columns.astype(str).str.strip()
if not df_term.empty: df_term.columns = df_term.columns.astype(str).str.strip()

# === 4. 功能标签页 ===
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🚫 否词清洗", 
    "💰 竞价优化", 
    "🏆 黄金挖掘", 
    "🕵️ ASIN 专项",
    "📊 广告位"
])

# --- Tab 1: 否词 ---
with tab1:
    st.subheader("🗑️ 无效流量清洗")
    if not df_term.empty:
        st_cols = {
            'search_term': next((c for c in df_term.columns if c in ["客户搜索词", "Search Term", "Customer Search Term"]), None),
            'spend': next((c for c in df_term.columns if c in ["花费", "Spend"]), None),
            'orders': next((c for c in df_term.columns if c in ["7天总订单数(#)", "订单数量", "Orders"]), None),
            'clicks': next((c for c in df_term.columns if c in ["点击量", "Clicks"]), None),
            'ad_group': next((c for c in df_term.columns if c in ["广告组名称", "Ad Group Name"]), None)
        }

        if st_cols['spend'] and st_cols['orders']:
            for c in [st_cols['spend'], st_cols['clicks'], st_cols['orders']]:
                if c: df_term[c] = pd.to_numeric(df_term[c], errors='coerce').fillna(0)
            
            mask_waste = (df_term[st_cols['orders']] == 0) & \
                         ((df_term[st_cols['spend']] >= neg_spend_th) | (df_term[st_cols['clicks']] >= neg_clicks_th))
            
            if st_cols['search_term']:
                mask_is_asin = df_term[st_cols['search_term']].astype(str).str.match(r'^[bB]0[a-zA-Z0-9]{8}$')
                waste_df = df_term[mask_waste & ~mask_is_asin].copy()
            else:
                waste_df = df_term[mask_waste].copy()
            
            waste_df = waste_df.sort_values(by=st_cols['spend'], ascending=False).head(50)

            if not waste_df.empty:
                st.error(f"🚨 发现 {len(waste_df)} 个浪费词 (花费>${neg_spend_th} 或 点击>{neg_clicks_th})")
                
                show_df = pd.DataFrame({
                    "广告组": waste_df[st_cols['ad_group']] if st_cols['ad_group'] else "未知",
                    "搜索词": waste_df[st_cols['search_term']],
                    "花费": waste_df[st_cols['spend']],
                    "点击": waste_df[st_cols['clicks']]
                })
                
                st.dataframe(
                    show_df,
                    column_config={
                        "花费": st.column_config.ProgressColumn("花费", format="$%.2f", min_value=0, max_value=max(show_df['花费'].max(), 1.0)),
                    },
                    use_container_width=True
                )
                
                if st.button("🤖 AI 分析不相关词", key="ai_neg"):
                    if deepseek_key:
                        with st.spinner("AI 正在扫描语义..."):
                            prompt = f"我是亚马逊卖家，产品【{product_name}】。请分析以下0转化搜索词，找出与产品完全不相关的词：\n{show_df[['搜索词', '花费']].to_string(index=False)}"
                            try:
                                res = requests.post("https://api.deepseek.com/chat/completions", 
                                                    headers={"Authorization": f"Bearer {deepseek_key}"}, 
                                                    json={"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}]})
                                st.markdown(res.json()['choices'][0]['message']['content'])
                            except: st.error("AI 连接失败，请检查 Key")
                    else: st.warning("请在左侧侧边栏输入 API Key")
            else: st.success("✅ 没有发现明显浪费。")
        else: st.warning("Search Term 表格缺少关键列。")
    else: st.info("请先上传 Search Term 表格")

# --- Tab 2: 竞价 ---
with tab2:
    st.subheader("📉 高 ACoS 降价建议")
    if not df_bulk.empty:
        bk_cols = {
            'entity': next((c for c in df_bulk.columns if c in ["实体层级", "Record Type"]), None),
            'kw': next((c for c in df_bulk.columns if c in ["关键词文本", "Keyword Text"]), None),
            'bid': next((c for c in df_bulk.columns if c in ["竞价", "Keyword Bid"]), None),
            'spend': next((c for c in df_bulk.columns if c in ["花费", "Spend"]), None),
            'sales': next((c for c in df_bulk.columns if c in ["销量", "Sales"]), None),
            'orders': next((c for c in df_bulk.columns if c in ["订单数量", "Orders"]), None),
            'camp': next((c for c in df_bulk.columns if c in ["广告活动名称", "Campaign Name"]), None),
        }

        if bk_cols['entity'] and bk_cols['kw']:
            df_kws = df_bulk[df_bulk[bk_cols['entity']].astype(str).str.contains('Keyword|关键词', case=False, na=False)].copy()
            for c in [bk_cols['spend'], bk_cols['sales'], bk_cols['orders'], bk_cols['bid']]:
                if c: df_kws[c] = pd.to_numeric(df_kws[c], errors='coerce').fillna(0)
            
            if bk_cols['spend'] and bk_cols['sales']:
                df_kws['ACoS'] = df_kws.apply(lambda x: x[bk_cols['spend']]/x[bk_cols['sales']] if x[bk_cols['sales']]>0 else 0, axis=1)
                
            bad_kws = df_kws[(df_kws[bk_cols['orders']] > 0) & (df_kws['ACoS'] > target_acos)].sort_values(by='ACoS', ascending=False).head(100)
            
            if not bad_kws.empty:
                st.dataframe(
                    pd.DataFrame({
                        "关键词": bad_kws[bk_cols['kw']],
                        "当前竞价": bad_kws[bk_cols['bid']],
                        "建议竞价": bad_kws[bk_cols['bid']] * 0.85,
                        "ACoS": bad_kws['ACoS'],
                        "花费": bad_kws[bk_cols['spend']]
                    }),
                    column_config={
                        "当前竞价": st.column_config.NumberColumn(format="$%.2f"),
                        "建议竞价": st.column_config.NumberColumn(format="$%.2f"),
                        "ACoS": st.column_config.ProgressColumn("ACoS", format="%.2f", min_value=0, max_value=max(bad_kws['ACoS'].max(), 1.0)),
                    },
                    use_container_width=True
                )
            else: st.success("✅ 竞价控制良好。")
        else: st.warning("Bulk 表格格式不正确。")
    else: st.info("请先上传 Bulk 表格")

# --- Tab 3: 黄金词 ---
with tab3:
    st.subheader("🏆 黄金词挖掘 (利润款)")
    st.write(f"筛选：转化率 > {gold_cvr*100}% 且 ACoS < {gold_acos*100}%")
    if not df_bulk.empty and 'df_kws' in locals():
        mask_gold = (df_kws[bk_cols['orders']] >= 2) & (df_kws['ACoS'] > 0) & (df_kws['ACoS'] < gold_acos)
        gold_df = df_kws[mask_gold].sort_values(by=bk_cols['sales'], ascending=False).head(50)
        
        if not gold_df.empty:
            st.dataframe(
                pd.DataFrame({
                    "关键词": gold_df[bk_cols['kw']],
                    "当前竞价": gold_df[bk_cols['bid']],
                    "建议竞价": gold_df[bk_cols['bid']] * 1.2,
                    "ACoS": gold_df['ACoS'],
                    "销售额": gold_df[bk_cols['sales']]
                }),
                column_config={
                    "建议竞价": st.column_config.NumberColumn(format="$%.2f", help="建议 Bid+"),
                    "ACoS": st.column_config.ProgressColumn("ACoS", format="%.2f", max_value=0.5),
                },
                use_container_width=True
            )
        else: st.info("未发现黄金词，建议放宽条件。")
    else: st.info("请先上传 Bulk 表格")

# --- Tab 4: ASIN ---
with tab4:
    st.subheader("🕵️ ASIN 流量分析")
    if not df_term.empty and st_cols['search_term']:
        df_term['is_asin'] = df_term[st_cols['search_term']].astype(str).str.match(r'^[bB]0[a-zA-Z0-9]{8}$')
        df_asin = df_term[df_term['is_asin']].copy()
        
        if not df_asin.empty:
            c_bad, c_good = st.columns(2)
            with c_bad:
                st.error("❌ 垃圾 ASIN (高费0单)")
                st.dataframe(df_asin[(df_asin[st_cols['orders']]==0) & (df_asin[st_cols['spend']]>3)][[st_cols['search_term'], st_cols['spend']]], use_container_width=True)
            with c_good:
                st.success("✅ 优质 ASIN (低价出单)")
                st.dataframe(df_asin[(df_asin[st_cols['orders']]>0)][[st_cols['search_term'], st_cols['spend'], st_cols['orders']]], use_container_width=True)
        else: st.info("没有发现 ASIN 数据。")
    else: st.info("请先上传 Search Term 表格")

# --- Tab 5: 广告位 ---
with tab5:
    st.subheader("📊 广告位表现")
    if not df_bulk.empty:
        c_place = next((c for c in df_bulk.columns if c in ["广告位", "Placement"]), None)
        c_p_spend = next((c for c in df_bulk.columns if c in ["花费", "Spend"]), None)
        c_p_sales = next((c for c in df_bulk.columns if c in ["销量", "Sales"]), None)
        
        if c_place and c_p_spend and c_p_sales:
            try:
                df_p = df_bulk[df_bulk[c_place].notna() & (df_bulk[c_place] != '')].copy()
                p_summary = df_p.groupby(c_place)[[c_p_spend, c_p_sales]].sum().reset_index()
                p_summary['ACoS'] = p_summary.apply(lambda x: x[c_p_spend]/x[c_p_sales] if x[c_p_sales]>0 else 0, axis=1)
                st.dataframe(
                    p_summary, 
                    column_config={"ACoS": st.column_config.ProgressColumn("ACoS", format="%.2f", max_value=1.0)}, 
                    use_container_width=True
                )
            except: st.info("无法解析广告位数据。")
        else: st.info("Bulk 文件不包含广告位信息。")
    else: st.info("请先上传 Bulk 表格")