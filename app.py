import streamlit as st
import pandas as pd
import requests
import json
import os
from datetime import datetime

# === 1. 全局配置 ===
st.set_page_config(
    page_title="Amazon AI 指挥官 (v5.0 终极版)", 
    layout="wide", 
    page_icon="🧠",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    div[data-testid="stMetric"] { background-color: white; border: 1px solid #ddd; padding: 10px; border-radius: 8px; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] { background-color: white; border-radius: 4px; }
    .stButton>button { width: 100%; border-radius: 4px; }
</style>
""", unsafe_allow_html=True)

# === 2. 核心：训练数据记录器 (你的教材本) ===
DATA_FILE = "deepseek_training_data.jsonl"

def save_training_example(term, spend, clicks, orders, action, reason):
    """保存你的决策，用于未来微调 DeepSeek"""
    # 1. 题目 (User)
    user_prompt = f"分析亚马逊搜索词：'{term}'。数据：花费${spend}, 点击{clicks}, 订单{orders}。"
    # 2. 答案 (Assistant)
    assistant_reply = f"建议：{action}。原因：{reason}"
    
    # 3. 格式化 (DeepSeek Jsonl)
    data = {
        "messages": [
            {"role": "system", "content": "你是一个精通Amazon PPC的运营专家。"},
            {"role": "user", "content": user_prompt},
            {"role": "assistant", "content": assistant_reply}
        ]
    }
    
    with open(DATA_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, ensure_ascii=False) + "\n")
    
    st.toast(f"✅ 已记入教材：{term} -> {action}")

# === 3. 侧边栏 ===
st.sidebar.title("🧠 控制台 v5.0")
deepseek_key = st.sidebar.text_input("🔑 DeepSeek Key", type="password")
product_name = st.sidebar.text_input("📦 产品名称", value="Makeup Mirror")

st.sidebar.markdown("---")
# 训练数据下载区
if os.path.exists(DATA_FILE):
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        count = sum(1 for _ in f)
    st.sidebar.metric("📚 已积累教材", f"{count} 条")
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        st.sidebar.download_button("📥 下载训练数据", f, file_name="deepseek_finetune.jsonl")
else:
    st.sidebar.info("暂无训练数据，快去'否词清洗'里点击按钮吧！")

st.sidebar.markdown("---")
with st.sidebar.expander("⚙️ 阈值设置", expanded=False):
    neg_spend_th = st.number_input("否词花费阈值", 5.0)
    target_acos = st.slider("目标 ACoS", 0.1, 1.0, 0.3)

# === 4. 主界面 & 数据加载 ===
st.title("🧠 Amazon AI 指挥官 (v5.0 终极合体版)")
st.caption("🚀 数据可视化 | 智能诊断 | **AI 模型训练 (数据积累中)**")

c1, c2 = st.columns(2)
with c1:
    file_bulk = st.file_uploader("📂 Bulk 表格 (竞价/图表)", type=['xlsx', 'csv'], key="bulk")
with c2:
    file_term = st.file_uploader("📂 Search Term (否词/训练)", type=['xlsx', 'csv'], key="term")

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

if not df_bulk.empty: df_bulk.columns = df_bulk.columns.astype(str).str.strip()
if not df_term.empty: df_term.columns = df_term.columns.astype(str).str.strip()

# === 5. 功能区 ===
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 数据看板", 
    "🧠 交互式清洗 (训练)", 
    "💰 竞价优化", 
    "🏆 黄金挖掘", 
    "💫 关联分析"
])

# 预处理
if not df_bulk.empty:
    bk_cols = {
        'entity': next((c for c in df_bulk.columns if c in ["实体层级", "Record Type"]), None),
        'kw': next((c for c in df_bulk.columns if c in ["关键词文本", "Keyword Text"]), None),
        'bid': next((c for c in df_bulk.columns if c in ["竞价", "Keyword Bid"]), None),
        'spend': next((c for c in df_bulk.columns if c in ["花费", "Spend"]), None),
        'sales': next((c for c in df_bulk.columns if c in ["销量", "Sales"]), None),
        'orders': next((c for c in df_bulk.columns if c in ["订单数量", "Orders"]), None),
        'clicks': next((c for c in df_bulk.columns if c in ["点击量", "Clicks"]), None),
    }
    if bk_cols['entity'] and bk_cols['kw']:
        df_kws = df_bulk[df_bulk[bk_cols['entity']].astype(str).str.contains('Keyword|关键词', case=False, na=False)].copy()
        for c in [bk_cols['spend'], bk_cols['sales'], bk_cols['orders'], bk_cols['clicks'], bk_cols['bid']]:
            if c: df_kws[c] = pd.to_numeric(df_kws[c], errors='coerce').fillna(0)
        if bk_cols['spend'] and bk_cols['sales']:
            df_kws['ACoS'] = df_kws.apply(lambda x: x[bk_cols['spend']]/x[bk_cols['sales']] if x[bk_cols['sales']]>0 else 0, axis=1)

# --- Tab 1: 看板 (v4.2的功能) ---
with tab1:
    st.subheader("📈 账户透视")
    if not df_bulk.empty and 'df_kws' in locals():
        t_spend = df_kws[bk_cols['spend']].sum()
        t_sales = df_kws[bk_cols['sales']].sum()
        m1, m2 = st.columns(2)
        m1.metric("总花费", f"${t_spend:,.2f}")
        m2.metric("总销售额", f"${t_sales:,.2f}")
        
        chart_data = df_kws[df_kws[bk_cols['spend']]>0].copy()
        if not chart_data.empty:
            st.scatter_chart(chart_data, x=bk_cols['spend'], y=bk_cols['sales'], size=bk_cols['clicks'], color='ACoS', height=400)
            st.info("💡 **左上角**是金矿，**右下角**是垃圾。")
    else: st.info("请上传 Bulk 表格。")

# --- Tab 2: 交互式清洗 (v5.0 核心) ---
with tab2:
    st.subheader("🧠 交互式清洗 (一边干活，一边训练AI)")
    st.markdown("👉 **你的每一次点击，都在教 DeepSeek 怎么做运营。**")
    
    if not df_term.empty:
        st_cols = {
            'term': next((c for c in df_term.columns if c in ["客户搜索词", "Search Term", "Customer Search Term"]), None),
            'spend': next((c for c in df_term.columns if c in ["花费", "Spend"]), None),
            'orders': next((c for c in df_term.columns if c in ["7天总订单数(#)", "订单数量", "Orders"]), None),
            'clicks': next((c for c in df_term.columns if c in ["点击量", "Clicks"]), None),
        }
        
        if st_cols['spend'] and st_cols['term']:
            for c in [st_cols['spend'], st_cols['clicks'], st_cols['orders']]:
                if c: df_term[c] = pd.to_numeric(df_term[c], errors='coerce').fillna(0)
            
            # 筛选出 0订单 且 有花费 的词 (最需要判断的词)
            mask = (df_term[st_cols['orders']] == 0) & (df_term[st_cols['spend']] > 0)
            review_df = df_term[mask].sort_values(by=st_cols['spend'], ascending=False).head(20)
            
            if not review_df.empty:
                for index, row in review_df.iterrows():
                    with st.expander(f"📝 {row[st_cols['term']]} (花费: ${row[st_cols['spend']]:.2f})", expanded=True):
                        c1, c2, c3, c4 = st.columns(4)
                        term = row[st_cols['term']]
                        sp = row[st_cols['spend']]
                        cl = row[st_cols['clicks']]
                        od = row[st_cols['orders']]
                        
                        # 按钮区 - 点击即保存
                        with c1:
                            if st.button("❌ 否定 (精准)", key=f"nex_{index}"):
                                save_training_example(term, sp, cl, od, "Negative Exact", "高花费0转化，词义不符")
                        with c2:
                            if st.button("🚫 否定 (词组)", key=f"nph_{index}"):
                                save_training_example(term, sp, cl, od, "Negative Phrase", "完全不相关流量")
                        with c3:
                            if st.button("👀 再观察一下", key=f"wait_{index}"):
                                save_training_example(term, sp, cl, od, "Keep", "数据量还不够，暂不处理")
                        with c4:
                            if st.button("🤖 AI 怎么看?", key=f"ask_{index}"):
                                if deepseek_key:
                                    prompt = f"分析词'{term}'，花费{sp}，点击{cl}，0单。是不是不相关？"
                                    try:
                                        res = requests.post("https://api.deepseek.com/chat/completions", headers={"Authorization": f"Bearer {deepseek_key}"}, json={"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}]})
                                        st.info(res.json()['choices'][0]['message']['content'])
                                    except: st.error("网络/Key错误")
                                else: st.warning("请填Key")
            else: st.success("🎉 太棒了！没有发现明显的浪费词。")
        else: st.error("缺少必要列")
    else: st.info("请上传 Search Term 表格")

# --- Tab 3/4/5: 其他功能 (保留 v4.2) ---
with tab3:
    st.subheader("📉 竞价优化")
    if not df_bulk.empty and 'df_kws' in locals():
        bad = df_kws[(df_kws[bk_cols['orders']]>0) & (df_kws['ACoS']>target_acos)].head(20)
        if not bad.empty: st.dataframe(bad[[bk_cols['kw'], 'ACoS', bk_cols['spend']]], use_container_width=True)
        else: st.success("竞价健康")

with tab4:
    st.subheader("🏆 黄金挖掘")
    if not df_bulk.empty and 'df_kws' in locals():
        gold = df_kws[(df_kws[bk_cols['orders']]>=2) & (df_kws['ACoS']<0.2)].head(20)
        if not gold.empty: st.dataframe(gold[[bk_cols['kw'], 'ACoS', bk_cols['sales']]], use_container_width=True)
        else: st.info("无黄金词")

with tab5:
    st.subheader("💫 关联分析")
    st.info("这里是光环效应分析区 (同 v4.2)")