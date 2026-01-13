import streamlit as st
import pandas as pd
import requests
import json
import os
from datetime import datetime

# === 1. 全局配置 ===
st.set_page_config(
    page_title="Amazon AI 指挥官 (v5.11 闪电版)", 
    layout="wide", 
    page_icon="⚡",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    div[data-testid="stMetric"] { background-color: white; border: 1px solid #ddd; padding: 10px; border-radius: 8px; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stButton>button { width: 100%; border-radius: 4px; }
    .ai-thought { background-color: #f1f3f4; padding: 10px; border-radius: 5px; font-size: 13px; margin-top: 5px;}
</style>
""", unsafe_allow_html=True)

# === 2. 核心逻辑 ===
DATA_FILE = "deepseek_cot_data.jsonl"

# A. ⚡ 人工瞬杀 (不调 API，极速保存)
def save_manual_label(term, spend, clicks, orders, action):
    # 构造一条“人工专家”数据
    train_data = {
        "messages": [
            {"role": "system", "content": "PPC专家"},
            {"role": "user", "content": f"词:{term}, 费:{spend}, 单:{orders}"},
            {"role": "assistant", "content": f"【人工专家裁决】\n经验判断：数据表现极差或语义明显不符。\n-> 操作: {action}"}
        ]
    }
    with open(DATA_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(train_data, ensure_ascii=False) + "\n")
    st.toast(f"⚡ 已瞬杀: {term}")

# B. 🤖 AI 深度思考 (调 API，慢但详细)
def generate_and_save_ai_thought(api_key, term, spend, clicks, orders, user_intent):
    if not api_key: return None
    
    # 自动计算 CPC
    cpc = spend / clicks if clicks > 0 else 0
    prompt = f"""
    你是一个冷酷的亚马逊广告数据分析师。产品: Makeup Mirror。对象: "{term}"。
    请输出 JSON，包含 "reasoning" 和 "action"。
    
    【reasoning 格式】
    [数据] 花费:${spend} | 点击:{clicks} | CPC:${cpc:.2f} | 订单:{orders}
    逻辑：1.CPC高否? 2.统计显著性? 3.结论。
    
    我的倾向: {user_intent}。
    """

    try:
        with st.spinner(f"⏳ AI 思考中..."):
            res = requests.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": f"Bearer {api_key}"},
                json={"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}], "temperature": 0.5, "response_format": {"type": "json_object"}}
            )
            if res.status_code == 200:
                ai_json = json.loads(res.json()['choices'][0]['message']['content'])
                # 保存
                train_data = {
                    "messages": [
                        {"role": "system", "content": "PPC数据分析师"},
                        {"role": "user", "content": f"词:{term}, 费:{spend}, 单:{orders}"},
                        {"role": "assistant", "content": f"{ai_json.get('reasoning')}\n-> 操作: {ai_json.get('action')}"}
                    ]
                }
                with open(DATA_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(train_data, ensure_ascii=False) + "\n")
                return ai_json.get('reasoning')
    except: return None

# === 3. 侧边栏 ===
st.sidebar.title("⚡ 控制台 v5.11")
default_key = "sk-55cc3f56742f4e43be099c9489e02911"
deepseek_key = st.sidebar.text_input("🔑 DeepSeek Key", value=default_key, type="password")
product_name = st.sidebar.text_input("📦 产品名称", value="Makeup Mirror")

st.sidebar.markdown("---")
# 阈值
with st.sidebar.expander("⚙️ 规则设置", expanded=True):
    target_acos = st.slider("目标 ACoS", 0.1, 1.0, 0.3)
    gold_acos = st.slider("黄金词 ACoS 上限", 0.1, 1.0, 0.2)

if os.path.exists(DATA_FILE):
    with open(DATA_FILE, "r", encoding="utf-8") as f: count = sum(1 for _ in f)
    st.sidebar.metric("📚 已积累教材", f"{count} 条")
    with open(DATA_FILE, "r", encoding="utf-8") as f: st.sidebar.download_button("📥 下载训练数据", f, file_name="finetune_fast.jsonl")

# === 4. 主界面 ===
st.title("⚡ Amazon AI 指挥官 (v5.11 闪电瞬杀版)")
st.caption("🚀 左键瞬杀(人工) | 右键求助(AI) | 效率与深度兼得")

c1, c2 = st.columns(2)
with c1:
    file_bulk = st.file_uploader("📂 1. 上传 Bulk 表格", type=['xlsx', 'csv'], key="bulk")
with c2:
    file_term = st.file_uploader("📂 2. 上传 Search Term 表格", type=['xlsx', 'csv'], key="term")

# 智能读取 Bulk
def smart_load_bulk(file):
    if not file: return pd.DataFrame()
    try:
        if file.name.endswith('.csv'): return pd.read_csv(file)
        dfs = pd.read_excel(file, sheet_name=None, engine='openpyxl')
        for sheet_name, df in dfs.items():
            cols = df.columns.astype(str).tolist()
            if any(x in cols for x in ['实体层级', 'Record Type']) and any(x in cols for x in ['关键词文本', 'Keyword Text', '投放']):
                st.toast(f"✅ 定位数据表: {sheet_name}")
                return df
        return pd.DataFrame()
    except: return pd.DataFrame()

df_bulk = smart_load_bulk(file_bulk)

# Search Term 读取
def load_simple(file):
    if not file: return pd.DataFrame()
    try:
        if file.name.endswith('.csv'): return pd.read_csv(file)
        return pd.read_excel(file, engine='openpyxl')
    except: return pd.DataFrame()

df_term = load_simple(file_term)

if not df_bulk.empty: df_bulk.columns = df_bulk.columns.astype(str).str.strip()
if not df_term.empty: df_term.columns = df_term.columns.astype(str).str.strip()

# 全局数据预处理
bulk_ready = False
df_kws = pd.DataFrame()
bk_cols = {}

if not df_bulk.empty:
    cols = df_bulk.columns
    bk_cols['spend'] = '花费'
    bk_cols['sales'] = next((c for c in ['销量', '销售额', '7天总销售额', 'Sales'] if c in cols), None)
    bk_cols['clicks'] = '点击量'
    bk_cols['entity'] = '实体层级'
    bk_cols['kw'] = next((c for c in ['关键词文本', '投放'] if c in cols), None)
    bk_cols['bid'] = next((c for c in ['竞价', 'Keyword Bid'] if c in cols), None)
    bk_cols['orders'] = '订单数量'

    if bk_cols['entity'] and bk_cols['kw'] and bk_cols['sales'] and bk_cols['spend']:
        df_kws = df_bulk[df_bulk[bk_cols['entity']].astype(str).str.contains('Keyword|关键词|Targeting', case=False, na=False)].copy()
        for c in [bk_cols['spend'], bk_cols['sales'], bk_cols['clicks'], bk_cols['bid'], bk_cols['orders']]:
            if c: df_kws[c] = pd.to_numeric(df_kws[c], errors='coerce').fillna(0)
        df_kws['ACoS'] = df_kws.apply(lambda x: x[bk_cols['spend']]/x[bk_cols['sales']] if x[bk_cols['sales']]>0 else 0, axis=1)
        bulk_ready = True

# === 5. 功能标签页 ===
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "⚡ 快速清洗", "📈 数据看板", "💰 竞价优化", "🏆 黄金词", "💫 关联分析"
])

# --- Tab 1: 快速清洗 (核心改动) ---
with tab1:
    st.subheader("⚡ 快速清洗 (Manual Speed Mode)")
    st.info("💡 提示：左边按钮是【瞬杀】（不经过AI，极速保存），右边按钮是【求助】（让AI分析）。")
    
    if not df_term.empty:
        c_term = '客户搜索词'
        c_spend = '花费'
        c_orders = '7天总订单数(#)'
        c_clicks = '点击量'
        
        if c_term in df_term.columns:
            df_term[c_spend] = pd.to_numeric(df_term[c_spend], errors='coerce').fillna(0)
            df_term[c_orders] = pd.to_numeric(df_term[c_orders], errors='coerce').fillna(0)
            df_term[c_clicks] = pd.to_numeric(df_term[c_clicks], errors='coerce').fillna(0)
            
            mask = (df_term[c_orders] == 0) & (df_term[c_spend] > 0)
            review_df = df_term[mask].sort_values(by=c_spend, ascending=False).head(20) # 多显示一点
            
            if not review_df.empty:
                for idx, row in review_df.iterrows():
                    # 布局优化：更紧凑
                    with st.expander(f"📝 {row[c_term]} (Spend: ${row[c_spend]:.2f})", expanded=True):
                        c1, c2, c3 = st.columns([1, 1, 3])
                        
                        # 1. 瞬杀按钮 (红)
                        with c1:
                            if st.button("⚡ 瞬杀 (直接否)", key=f"fast_kill_{idx}", type="primary"):
                                save_manual_label(row[c_term], row[c_spend], row[c_clicks], 0, "Negative")
                        
                        # 2. 瞬留按钮 (绿)
                        with c2:
                            if st.button("👀 瞬留 (直接留)", key=f"fast_keep_{idx}"):
                                save_manual_label(row[c_term], row[c_spend], row[c_clicks], 0, "Keep")
                        
                        # 3. AI 求助区 (灰)
                        with c3:
                            if st.button("🤖 拿不准，问AI", key=f"ask_ai_{idx}"):
                                reasoning = generate_and_save_ai_thought(deepseek_key, row[c_term], row[c_spend], row[c_clicks], 0, "Unknown")
                                if reasoning: st.session_state[f"ai_res_{idx}"] = reasoning
                            
                            # 显示 AI 结果
                            if f"ai_res_{idx}" in st.session_state:
                                st.markdown(f"""<div class="ai-thought">{st.session_state[f"ai_res_{idx}"]}</div>""", unsafe_allow_html=True)

            else: st.success("没有发现高花费0转化的词。")
    else: st.info("请上传 Search Term 表格")

# --- Tab 2-5 (保持不变) ---
with tab2:
    st.subheader("📈 账户透视")
    if bulk_ready:
        t_spend = df_kws[bk_cols['spend']].sum()
        t_sales = df_kws[bk_cols['sales']].sum()
        m1, m2 = st.columns(2)
        m1.metric("总花费", f"${t_spend:,.2f}")
        m2.metric("总销售额", f"${t_sales:,.2f}")
        chart_data = df_kws[df_kws[bk_cols['spend']]>0]
        st.scatter_chart(chart_data, x=bk_cols['spend'], y=bk_cols['sales'], size=bk_cols['clicks'], color='ACoS')
    else: st.info("等待 Bulk 数据...")

with tab3:
    st.subheader("💰 竞价优化")
    if bulk_ready:
        bad_kws = df_kws[(df_kws[bk_cols['orders']] > 0) & (df_kws['ACoS'] > target_acos)].sort_values(by='ACoS', ascending=False).head(50)
        if not bad_kws.empty:
            show_df = bad_kws[[bk_cols['kw'], bk_cols['bid'], 'ACoS', bk_cols['spend'], bk_cols['sales']]].copy()
            show_df['建议竞价'] = show_df[bk_cols['bid']] * 0.8
            st.dataframe(show_df, column_config={"ACoS": st.column_config.ProgressColumn(format="%.2f", max_value=2)}, use_container_width=True)
        else: st.success("竞价健康")
    else: st.info("等待 Bulk 数据")

with tab4:
    st.subheader("🏆 黄金词")
    if bulk_ready:
        gold_df = df_kws[(df_kws[bk_cols['orders']] >= 2) & (df_kws['ACoS'] > 0) & (df_kws['ACoS'] < gold_acos)].sort_values(by=bk_cols['sales'], ascending=False).head(50)
        if not gold_df.empty:
            show_df = gold_df[[bk_cols['kw'], bk_cols['bid'], 'ACoS', bk_cols['sales']]].copy()
            show_df['建议竞价'] = show_df[bk_cols['bid']] * 1.2
            st.dataframe(show_df, column_config={"ACoS": st.column_config.ProgressColumn(format="%.2f", max_value=0.5)}, use_container_width=True)
        else: st.info("暂无黄金词")
    else: st.info("等待 Bulk 数据")

with tab5:
    st.subheader("💫 关联分析")
    if not df_term.empty:
        c_halo = '7天内其他SKU销售量(#)'
        if c_halo in df_term.columns:
            df_term[c_halo] = pd.to_numeric(df_term[c_halo], errors='coerce').fillna(0)
            halo = df_term[df_term[c_halo]>0].sort_values(by=c_halo, ascending=False).head(20)
            if not halo.empty: st.dataframe(halo[['客户搜索词', c_halo, '花费']], use_container_width=True)
            else: st.info("无关联订单")