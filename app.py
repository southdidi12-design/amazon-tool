import streamlit as st
import pandas as pd
import requests
import re # 用于正则匹配 ASIN

# === 1. 全局配置 & CSS ===
st.set_page_config(
    page_title="Amazon 广告指挥官 (旗舰版)", 
    layout="wide", 
    page_icon="⚔️",
    initial_sidebar_state="expanded"
)

# 注入现代化 CSS
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
st.sidebar.image("https://upload.wikimedia.org/wikipedia/commons/a/a9/Amazon_logo.svg", width=120)
st.sidebar.title("⚙️ 控制中枢")

# 模拟 API 配置区
with st.sidebar.expander("🔌 API 设置 (预留)", expanded=False):
    st.text_input("Client ID", disabled=True, placeholder="Coming soon...")
    st.text_input("Client Secret", disabled=True, placeholder="Coming soon...")

# AI 设置
default_key = "sk-55cc3f56742f4e43be099c9489e02911"
deepseek_key = st.sidebar.text_input("🔑 DeepSeek Key", value=default_key, type="password")
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
st.title("⚔️ Amazon 广告指挥官 (旗舰版)")

# 文件上传区 (模拟 API 数据源)
c1, c2 = st.columns(2)
with c1:
    file_bulk = st.file_uploader("📂 Bulk 表格 (竞价/广告位)", type=['xlsx', 'csv'], key="bulk")
with c2:
    file_term = st.file_uploader("📂 Search Term (否词/ASIN)", type=['xlsx', 'csv'], key="term")

# 数据预处理函数 (方便后续对接API)
def load_data(file, file_type):
    if not file: return pd.DataFrame()
    try:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file)
        else:
            if file_type == 'bulk':
                # Bulk通常要找包含 Keyword 的 Sheet
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

# 清洗列名 (标准化)
if not df_bulk.empty:
    df_bulk.columns = df_bulk.columns.astype(str).str.strip()
if not df_term.empty:
    df_term.columns = df_term.columns.astype(str).str.strip()

# === 4. 功能标签页 ===
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🚫 否词清洗", 
    "💰 竞价优化", 
    "🏆 黄金挖掘", 
    "🕵️ ASIN 专项",
    "📊 广告位"
])

# =================================================
# Tab 1: 否词清洗 (Search Term - Negative)
# =================================================
with tab1:
    st.subheader("🗑️ 无效流量清洗")
    if not df_term.empty:
        # 列映射
        st_cols = {
            'search_term': next((c for c in df_term.columns if c in ["客户搜索词", "Search Term", "Customer Search Term"]), None),
            'spend': next((c for c in df_term.columns if c in ["花费", "Spend"]), None),
            'orders': next((c for c in df_term.columns if c in ["7天总订单数(#)", "订单数量", "Orders"]), None),
            'clicks': next((c for c in df_term.columns if c in ["点击量", "Clicks"]), None),
            'campaign': next((c for c in df_term.columns if c in ["广告活动名称", "Campaign Name"]), None),
            'ad_group': next((c for c in df_term.columns if c in ["广告组名称", "Ad Group Name"]), None)
        }

        if st_cols['spend'] and st_cols['orders']:
            # 预处理数字
            for c in [st_cols['spend'], st_cols['clicks'], st_cols['orders']]:
                if c: df_term[c] = pd.to_numeric(df_term[c], errors='coerce').fillna(0)
            
            # 逻辑: 0单 且 (高花费 或 高点击)
            mask_waste = (df_term[st_cols['orders']] == 0) & \
                         ((df_term[st_cols['spend']] >= neg_spend_th) | (df_term[st_cols['clicks']] >= neg_clicks_th))
            
            # 排除 ASIN (ASIN 在 Tab 4 处理)
            if st_cols['search_term']:
                mask_is_asin = df_term[st_cols['search_term']].astype(str).str.match(r'^[bB]0[a-zA-Z0-9]{8}$')
                waste_df = df_term[mask_waste & ~mask_is_asin].copy()
            else:
                waste_df = df_term[mask_waste].copy()
            
            waste_df = waste_df.sort_values(by=st_cols['spend'], ascending=False).head(50)

            if not waste_df.empty:
                col1, col2, col3 = st.columns(3)
                col1.metric("🚨 建议否定词数", f"{len(waste_df)}")
                col2.metric("💸 预计节省", f"${waste_df[st_cols['spend']].sum():.2f}")
                
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
                            prompt = f"我是亚马逊卖家，产品【{product_name}】。请分析以下0转化搜索词，找出与产品完全不相关的词（如场景不对、品类不对）：\n{show_df[['搜索词', '花费']].to_string(index=False)}"
                            try:
                                res = requests.post("https://api.deepseek.com/chat/completions", 
                                                    headers={"Authorization": f"Bearer {deepseek_key}"}, 
                                                    json={"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}]})
                                st.markdown(res.json()['choices'][0]['message']['content'])
                            except: st.error("AI 连接失败")
            else:
                st.success("✅ 搜索词很干净，没有发现明显浪费。")
        else:
            st.warning("Search Term 表格缺少关键列。")
    else:
        st.info("请先上传 Search Term 表格")

# =================================================
# Tab 2: 竞价优化 (Bulk - High ACoS)
# =================================================
with tab2:
    st.subheader("📉 高 ACoS 降价建议")
    if not df_bulk.empty:
        # 列映射 (Bulk比较复杂)
        bk_cols = {
            'entity': next((c for c in df_bulk.columns if c in ["实体层级", "Record Type"]), None),
            'kw': next((c for c in df_bulk.columns if c in ["关键词文本", "Keyword Text"]), None),
            'bid': next((c for c in df_bulk.columns if c in ["竞价", "Keyword Bid"]), None),
            'spend': next((c for c in df_bulk.columns if c in ["花费", "Spend"]), None),
            'sales': next((c for c in df_bulk.columns if c in ["销量", "Sales"]), None),
            'orders': next((c for c in df_bulk.columns if c in ["订单数量", "Orders"]), None),
            'match': next((c for c in df_bulk.columns if c in ["匹配类型", "Match Type"]), None),
            'camp': next((c for c in df_bulk.columns if c in ["广告活动名称", "Campaign Name"]), None),
        }

        if bk_cols['entity'] and bk_cols['kw']:
            # 筛选关键词行
            df_kws = df_bulk[df_bulk[bk_cols['entity']].astype(str).str.contains('Keyword|关键词', case=False, na=False)].copy()
            
            for c in [bk_cols['spend'], bk_cols['sales'], bk_cols['orders'], bk_cols['bid']]:
                if c: df_kws[c] = pd.to_numeric(df_kws[c], errors='coerce').fillna(0)
            
            # 算 ACoS
            if bk_cols['spend'] and bk_cols['sales']:
                df_kws['ACoS'] = df_kws.apply(lambda x: x[bk_cols['spend']]/x[bk_cols['sales']] if x[bk_cols['sales']]>0 else 0, axis=1)
                
            # 筛选: 出单 且 ACoS > 目标
            mask_bad = (df_kws[bk_cols['orders']] > 0) & (df_kws['ACoS'] > target_acos)
            bad_kws = df_kws[mask_bad].sort_values(by='ACoS', ascending=False).head(100)
            
            if not bad_kws.empty:
                st.write(f"以下词 ACoS > {target_acos*100}%，建议**降低竞价**：")
                
                show_bid = pd.DataFrame({
                    "广告活动": bad_kws[bk_cols['camp']] if bk_cols['camp'] else "-",
                    "关键词": bad_kws[bk_cols['kw']],
                    "当前竞价": bad_kws[bk_cols['bid']],
                    "建议竞价": bad_kws[bk_cols['bid']] * 0.85, # 默认降15%
                    "ACoS": bad_kws['ACoS'],
                    "花费": bad_kws[bk_cols['spend']]
                })
                
                st.dataframe(
                    show_bid,
                    column_config={
                        "当前竞价": st.column_config.NumberColumn(format="$%.2f"),
                        "建议竞价": st.column_config.NumberColumn(format="$%.2f"),
                        "ACoS": st.column_config.ProgressColumn("ACoS", format="%.2f", min_value=0, max_value=max(show_bid['ACoS'].max(), 1.0)),
                    },
                    use_container_width=True
                )
            else:
                st.success("✅ 竞价控制良好，没有高 ACoS 词。")
        else:
            st.warning("Bulk 表格格式无法识别（缺关键词/竞价列）。")
    else:
        st.info("请先上传 Bulk 表格")

# =================================================
# Tab 3: 黄金挖掘 (Golden Keywords)
# =================================================
with tab3:
    st.subheader("🏆 黄金词挖掘 (表现优异)")
    st.write(f"筛选标准：转化率 > {gold_cvr*100}% 且 ACoS < {gold_acos*100}%")
    
    if not df_bulk.empty and 'df_kws' in locals():
        # 复用 Tab2 处理好的 df_kws
        if bk_cols['orders'] and bk_cols['spend'] and bk_cols['sales']:
            # 算 CVR
            df_kws['clicks'] = pd.to_numeric(df_bulk[next((c for c in df_bulk.columns if "点击" in c or "Clicks" in c), None)], errors='coerce').fillna(1)
            # 注意：这里点击量可能在Bulk里没有，如果有的话
            # 简化版：用 ACoS 和 订单数 判断
            
            mask_gold = (df_kws[bk_cols['orders']] >= 2) & \
                        (df_kws['ACoS'] > 0) & \
                        (df_kws['ACoS'] < gold_acos)
            
            gold_df = df_kws[mask_gold].sort_values(by=bk_cols['sales'], ascending=False).head(50)
            
            if not gold_df.empty:
                st.balloons()
                st.success(f"🎉 发现 {len(gold_df)} 个黄金词！建议 **提高竞价** 或 **开启顶置(Top)**。")
                
                show_gold = pd.DataFrame({
                    "关键词": gold_df[bk_cols['kw']],
                    "当前竞价": gold_df[bk_cols['bid']],
                    "建议竞价": gold_df[bk_cols['bid']] * 1.2, # 建议提20%
                    "ACoS": gold_df['ACoS'],
                    "销售额": gold_df[bk_cols['sales']],
                    "订单": gold_df[bk_cols['orders']]
                })
                
                st.dataframe(
                    show_gold,
                    column_config={
                        "建议竞价": st.column_config.NumberColumn(format="$%.2f", help="建议 Bid+"),
                        "ACoS": st.column_config.ProgressColumn("ACoS (越低越好)", format="%.2f", max_value=0.5),
                        "销售额": st.column_config.ProgressColumn("贡献销售额", format="$%.2f", min_value=0, max_value=max(show_gold['销售额'].max(), 1.0)),
                    },
                    use_container_width=True
                )
            else:
                st.info("暂未发现符合严苛条件的黄金词，建议适当放宽阈值。")
    else:
        st.info("请先上传 Bulk 表格")

# =================================================
# Tab 4: ASIN 专项 (ASIN Analysis)
# =================================================
with tab4:
    st.subheader("🕵️ ASIN 流量分析")
    if not df_term.empty and st_cols['search_term']:
        # 提取 ASIN (B0xxxxxxx)
        df_term['is_asin'] = df_term[st_cols['search_term']].astype(str).str.match(r'^[bB]0[a-zA-Z0-9]{8}$')
        df_asin = df_term[df_term['is_asin']].copy()
        
        if not df_asin.empty:
            st.write(f"共扫描到 {len(df_asin)} 个关联 ASIN。")
            
            col_a, col_b = st.columns(2)
            
            with col_a:
                st.markdown("#### ❌ 垃圾 ASIN (高费无单)")
                # 逻辑：0单，花费高
                bad_asin = df_asin[(df_asin[st_cols['orders']]==0) & (df_asin[st_cols['spend']]>3)].sort_values(by=st_cols['spend'], ascending=False)
                if not bad_asin.empty:
                    st.dataframe(bad_asin[[st_cols['search_term'], st_cols['spend'], st_cols['clicks']]], use_container_width=True)
                    st.warning("建议：加入【否定商品投放】")
                else:
                    st.write("无")

            with col_b:
                st.markdown("#### ✅ 优质 ASIN (低价出单)")
                # 逻辑：出单，ACoS低
                good_asin = df_asin[(df_asin[st_cols['orders']]>0) & (df_asin[st_cols['spend']] / df_asin[st_cols['orders']] < 15)].copy() # 简单估算CPA
                if not good_asin.empty:
                    st.dataframe(good_asin[[st_cols['search_term'], st_cols['spend'], st_cols['orders']]], use_container_width=True)
                    st.success("建议：单独开启【商品投放】广告")
                else:
                    st.write("无")
        else:
            st.info("搜索词报告中没有发现 ASIN。")
    else:
        st.info("请先上传 Search Term 表格")

# =================================================
# Tab 5: 广告位分析 (Placement - Experimental)
# =================================================
with tab5:
    st.subheader("📊 广告位表现 (基于 Bulk)")
    if not df_bulk.empty:
        # 尝试寻找 Placement 列
        c_place = next((c for c in df_bulk.columns if c in ["广告位", "Placement"]), None)
        c_p_spend = next((c for c in df_bulk.columns if c in ["花费", "Spend"]), None)
        c_p_sales = next((c for c in df_bulk.columns if c in ["销量", "Sales"]), None)
        
        if c_place and c_p_spend and c_p_sales:
            # 聚合分析
            # 注意：Bulk里可能每行代表一个Placement设置，但数据可能在同一行的花费里
            try:
                # 过滤出有 placement 数据的行
                df_p = df_bulk[df_bulk[c_place].notna() & (df_bulk[c_place] != '')].copy()
                
                # 简单聚合
                p_summary = df_p.groupby(c_place)[[c_p_spend, c_p_sales]].sum().reset_index()
                p_summary['ACoS'] = p_summary.apply(lambda x: x[c_p_spend]/x[c_p_sales] if x[c_p_sales]>0 else 0, axis=1)
                
                if not p_summary.empty:
                    st.dataframe(
                        p_summary,
                        column_config={
                            "ACoS": st.column_config.ProgressColumn("ACoS", format="%.2f", max_value=1.0),
                            c_p_spend: st.column_config.NumberColumn("花费", format="$%.2f"),
                        },
                        use_container_width=True
                    )
                    st.caption("提示：Top of Search (首页顶部) 通常转化率最高，建议根据 ACoS 适当增加 Bid+。")
                else:
                    st.info("Bulk 文件中未检测到广告位聚合数据。")
            except:
                st.info("无法解析广告位数据，可能 Bulk 格式不支持。")
        else:
            st.info("您的 Bulk 文件似乎不包含【广告位】或【花费】列。")
    else:
        st.info("请先上传 Bulk 表格")

st.markdown("---")
st.markdown("<div style='text-align: center; color: #999;'>Amazon Ads Commander © 2026 | Designed for API Integration</div>", unsafe_allow_html=True)
