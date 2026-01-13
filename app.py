import streamlit as st
import pandas as pd
import requests
import re

# === 1. 全局配置 (v4.0) ===
st.set_page_config(
    page_title="Amazon 广告指挥官 v4.0", 
    layout="wide", 
    page_icon="📊",
    initial_sidebar_state="expanded"
)

# 注入 CSS
st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    h1 { color: #2c3e50; font-family: 'Helvetica Neue', sans-serif; }
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] { height: 50px; background-color: white; border-radius: 5px; box-shadow: 0 1px 2px rgba(0,0,0,0.1); }
    .stTabs [aria-selected="true"] { background-color: #e8f0fe; color: #1a73e8; border: 2px solid #1a73e8; }
    div[data-testid="stMetric"] { background-color: white; padding: 15px; border-radius: 8px; border: 1px solid #ddd; }
</style>
""", unsafe_allow_html=True)

# === 2. 侧边栏 ===
st.sidebar.title("⚙️ 控制中枢 v4.0")
deepseek_key = st.sidebar.text_input("🔑 DeepSeek Key", type="password", help="安全模式：Key 不会保存")
product_name = st.sidebar.text_input("📦 产品名称", value="Makeup Mirror")

st.sidebar.markdown("---")
# 规则设置
with st.sidebar.expander("🚫 否词规则", expanded=True):
    neg_spend_th = st.number_input("花费 > ($)", 5.0, step=1.0)
    neg_clicks_th = st.number_input("点击 > (次)", 10, step=1)
with st.sidebar.expander("💰 竞价规则", expanded=False):
    target_acos = st.slider("目标 ACoS", 0.1, 1.0, 0.3)
with st.sidebar.expander("🏆 黄金词规则", expanded=False):
    gold_cvr = st.slider("CVR > (%)", 5, 50, 15) / 100
    gold_acos = st.slider("ACoS < (%)", 5, 50, 20) / 100

# === 3. 主界面 ===
st.title("📊 Amazon 广告指挥官 (v4.0 图表版)")
st.caption("🚀 数据可视化 | 关联购买分析 | 智能诊断")

c1, c2 = st.columns(2)
with c1:
    file_bulk = st.file_uploader("📂 Bulk 表格 (用于竞价/图表)", type=['xlsx', 'csv'], key="bulk")
with c2:
    file_term = st.file_uploader("📂 Search Term (用于否词/关联)", type=['xlsx', 'csv'], key="term")

# 数据读取
def load_data(file, ftype):
    if not file: return pd.DataFrame()
    try:
        if file.name.endswith('.csv'): df = pd.read_csv(file)
        else:
            if ftype == 'bulk':
                dfs = pd.read_excel(file, sheet_name=None, engine='openpyxl')
                for _, d in dfs.items():
                    if d.astype(str).apply(lambda x: x.str.contains('Keyword|关键词', case=False)).any().any(): return d
                return pd.DataFrame()
            else: df = pd.read_excel(file, engine='openpyxl')
        return df
    except: return pd.DataFrame()

df_bulk = load_data(file_bulk, 'bulk')
df_term = load_data(file_term, 'term')

# 清洗列名
if not df_bulk.empty: df_bulk.columns = df_bulk.columns.astype(str).str.strip()
if not df_term.empty: df_term.columns = df_term.columns.astype(str).str.strip()

# === 4. 功能标签 ===
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈 数据看板",     # 新增
    "🚫 否词清洗", 
    "💰 竞价优化", 
    "🏆 黄金挖掘", 
    "💫 关联分析",     # 新增替代复购率
    "🕵️ ASIN 专项"
])

# --- 预处理 Bulk 数据 (给图表用) ---
if not df_bulk.empty:
    bk_cols = {
        'entity': next((c for c in df_bulk.columns if c in ["实体层级", "Record Type"]), None),
        'kw': next((c for c in df_bulk.columns if c in ["关键词文本", "Keyword Text"]), None),
        'bid': next((c for c in df_bulk.columns if c in ["竞价", "Keyword Bid"]), None),
        'spend': next((c for c in df_bulk.columns if c in ["花费", "Spend"]), None),
        'sales': next((c for c in df_bulk.columns if c in ["销量", "Sales"]), None),
        'orders': next((c for c in df_bulk.columns if c in ["订单数量", "Orders"]), None),
        'clicks': next((c for c in df_bulk.columns if c in ["点击量", "Clicks"]), None),
        'camp': next((c for c in df_bulk.columns if c in ["广告活动名称", "Campaign Name"]), None),
    }
    if bk_cols['entity'] and bk_cols['kw']:
        df_kws = df_bulk[df_bulk[bk_cols['entity']].astype(str).str.contains('Keyword|关键词', case=False, na=False)].copy()
        for c in [bk_cols['spend'], bk_cols['sales'], bk_cols['orders'], bk_cols['clicks'], bk_cols['bid']]:
            if c: df_kws[c] = pd.to_numeric(df_kws[c], errors='coerce').fillna(0)
        if bk_cols['spend'] and bk_cols['sales']:
            df_kws['ACoS'] = df_kws.apply(lambda x: x[bk_cols['spend']]/x[bk_cols['sales']] if x[bk_cols['sales']]>0 else 0, axis=1)

# --- Tab 1: 数据看板 (可视化) ---
with tab1:
    st.subheader("📈 广告账户透视")
    if not df_bulk.empty and 'df_kws' in locals():
        # 1. 核心指标卡
        total_spend = df_kws[bk_cols['spend']].sum()
        total_sales = df_kws[bk_cols['sales']].sum()
        total_acos = total_spend / total_sales if total_sales > 0 else 0
        
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("总花费", f"${total_spend:,.2f}")
        m2.metric("总销售额", f"${total_sales:,.2f}")
        m3.metric("综合 ACoS", f"{total_acos:.2%}", delta_color="inverse" if total_acos > target_acos else "normal")
        m4.metric("出单关键词数", f"{len(df_kws[df_kws[bk_cols['orders']]>0])}")

        st.markdown("---")
        
        # 2. 气泡图分析
        st.markdown("##### 🔍 关键词矩阵分析 (Spend vs Sales)")
        st.caption("横轴：花费 | 纵轴：销售额 | 大小：点击量 | 颜色：ACoS (越红越差)")
        
        # 准备图表数据
        chart_data = df_kws[df_kws[bk_cols['spend']] > 0].copy()
        if not chart_data.empty:
            st.scatter_chart(
                chart_data,
                x=bk_cols['spend'],
                y=bk_cols['sales'],
                size=bk_cols['clicks'],
                color='ACoS', # 自动渐变色
                height=500,
                use_container_width=True
            )
            st.info("💡 **怎么看这张图？**\n- **左上角 (低费高产)**：黄金区，这些词要加预算！\n- **右下角 (高费低产)**：灾难区，这些词要降价或否定！\n- **右上角 (高费高产)**：主力词，关注 ACoS 是否在红线以下。")
    else:
        st.info("请上传 Bulk 表格以生成图表。")

# --- Tab 2: 否词 ---
with tab2:
    st.subheader("🗑️ 否词清洗")
    if not df_term.empty:
        st_cols = {
            'term': next((c for c in df_term.columns if c in ["客户搜索词", "Search Term", "Customer Search Term"]), None),
            'spend': next((c for c in df_term.columns if c in ["花费", "Spend"]), None),
            'orders': next((c for c in df_term.columns if c in ["7天总订单数(#)", "订单数量", "Orders"]), None),
            'clicks': next((c for c in df_term.columns if c in ["点击量", "Clicks"]), None),
            'other_sales': next((c for c in df_term.columns if c in ["7天内其他SKU销售量(#)", "Other SKU Sales"]), None), # 关联销售
            'ad_sales': next((c for c in df_term.columns if c in ["7天内广告SKU销售量(#)", "Advertised SKU Sales"]), None)
        }
        if st_cols['spend'] and st_cols['orders']:
            for c in [st_cols['spend'], st_cols['clicks'], st_cols['orders']]:
                if c: df_term[c] = pd.to_numeric(df_term[c], errors='coerce').fillna(0)
            
            mask = (df_term[st_cols['orders']] == 0) & ((df_term[st_cols['spend']] >= neg_spend_th) | (df_term[st_cols['clicks']] >= neg_clicks_th))
            waste_df = df_term[mask].sort_values(by=st_cols['spend'], ascending=False).head(50)
            
            if not waste_df.empty:
                st.dataframe(waste_df[[st_cols['term'], st_cols['spend'], st_cols['clicks']]], use_container_width=True)
                if st.button("🤖 AI 分析", key="ai_n"):
                    if deepseek_key:
                        prompt = f"产品【{product_name}】。找出以下0转化词中的不相关词：\n{waste_df[[st_cols['term'], st_cols['spend']]].to_string(index=False)}"
                        try:
                            res = requests.post("https://api.deepseek.com/chat/completions", headers={"Authorization": f"Bearer {deepseek_key}"}, json={"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}]})
                            st.markdown(res.json()['choices'][0]['message']['content'])
                        except: st.error("Key 错误或网络问题")
                    else: st.warning("请填 Key")
            else: st.success("没有发现浪费词。")

# --- Tab 3: 竞价 ---
with tab3:
    st.subheader("📉 降价建议")
    if not df_bulk.empty and 'df_kws' in locals():
        bad_kws = df_kws[(df_kws[bk_cols['orders']] > 0) & (df_kws['ACoS'] > target_acos)].sort_values(by='ACoS', ascending=False).head(50)
        if not bad_kws.empty:
            show = bad_kws[[bk_cols['kw'], bk_cols['bid'], 'ACoS', bk_cols['spend']]].copy()
            show['建议竞价'] = show[bk_cols['bid']] * 0.85
            st.dataframe(show, column_config={"ACoS": st.column_config.ProgressColumn(format="%.2f")}, use_container_width=True)
        else: st.success("竞价健康。")

# --- Tab 4: 黄金词 ---
with tab4:
    st.subheader("🏆 黄金词")
    if not df_bulk.empty and 'df_kws' in locals():
        gold_df = df_kws[(df_kws[bk_cols['orders']] >= 2) & (df_kws['ACoS'] > 0) & (df_kws['ACoS'] < gold_acos)].sort_values(by=bk_cols['sales'], ascending=False).head(50)
        if not gold_df.empty:
            st.dataframe(gold_df[[bk_cols['kw'], bk_cols['bid'], 'ACoS', bk_cols['sales']]], use_container_width=True)
        else: st.info("暂无黄金词。")

# --- Tab 5: 关联分析 (新功能) ---
with tab5:
    st.subheader("💫 关联购买 (光环效应)")
    st.caption("分析：顾客点了广告后，没有买广告商品，反而买了店里其他商品的情况。")
    
    if not df_term.empty:
        # 尝试查找关联列
        c_halo = next((c for c in df_term.columns if "其他SKU" in c or "Other SKU" in c), None)
        c_ad = next((c for c in df_term.columns if "广告SKU" in c or "Advertised SKU" in c), None)
        
        if c_halo and c_ad:
            df_term[c_halo] = pd.to_numeric(df_term[c_halo], errors='coerce').fillna(0)
            df_term[c_ad] = pd.to_numeric(df_term[c_ad], errors='coerce').fillna(0)
            
            total_halo = df_term[c_halo].sum()
            total_direct = df_term[c_ad].sum()
            
            if total_halo + total_direct > 0:
                halo_rate = total_halo / (total_halo + total_direct)
                
                c1, c2, c3 = st.columns(3)
                c1.metric("🎯 直接订单 (广告品)", f"{int(total_direct)}")
                c2.metric("💫 关联订单 (其他品)", f"{int(total_halo)}")
                c3.metric("📈 关联购买率 (Halo Rate)", f"{halo_rate:.1%}")
                
                # 找出最能带货的词
                st.markdown("#### 🛍️ 最强“带货”搜索词 (Halo Kings)")
                halo_terms = df_term[df_term[c_halo] > 0].sort_values(by=c_halo, ascending=False).head(20)
                
                if not halo_terms.empty:
                    st.dataframe(
                        halo_terms[[st_cols['term'], c_halo, c_ad, st_cols['spend']]],
                        column_config={
                            c_halo: st.column_config.ProgressColumn("关联销量", format="%d", min_value=0, max_value=max(halo_terms[c_halo].max(), 1)),
                        },
                        use_container_width=True
                    )
                    st.info("💡 **策略建议**：这些词虽然可能直接转化一般，但能给全店带来销量！不要轻易否定，甚至可以用来给新品引流。")
                else:
                    st.info("数据中未发现明显的关联购买行为。")
            else:
                st.info("没有检测到订单数据。")
        else:
            st.warning("Search Term 表格缺少 '7天内其他SKU销售量' 列，无法分析关联购买。")
    else:
        st.info("请先上传 Search Term 表格。")

# --- Tab 6: ASIN ---
with tab6:
    st.subheader("🕵️ ASIN 专项")
    if not df_term.empty and st_cols['term']:
        df_term['is_asin'] = df_term[st_cols['term']].astype(str).str.match(r'^[bB]0[a-zA-Z0-9]{8}$')
        df_asin = df_term[df_term['is_asin']]
        if not df_asin.empty:
            st.dataframe(df_asin[[st_cols['term'], st_cols['spend'], st_cols['orders']]], use_container_width=True)
        else: st.info("无 ASIN 数据。")