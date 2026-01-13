import streamlit as st
import pandas as pd
import requests
import json
import os
from datetime import datetime

# === 1. 全局配置 ===
st.set_page_config(
    page_title="Amazon AI 指挥官 (v5.3 智能版)", 
    layout="wide", 
    page_icon="🧬",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    div[data-testid="stMetric"] { background-color: white; border: 1px solid #ddd; padding: 10px; border-radius: 8px; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stButton>button { width: 100%; border-radius: 4px; }
    .stAlert { padding: 10px; }
</style>
""", unsafe_allow_html=True)

# === 2. 核心逻辑 ===
DATA_FILE = "deepseek_cot_data.jsonl"

def generate_and_save_ai_thought(api_key, term, spend, clicks, orders, user_intent):
    if not api_key:
        st.error("❌ 需要 API Key")
        return None
    prompt = f"我是亚马逊运营。产品Makeup Mirror。分析词'{term}'，花费${spend}，点击{clicks}，订单{orders}。输出JSON：reasoning, action。倾向：{user_intent}。"
    try:
        with st.spinner(f"🧠 AI 分析中..."):
            res = requests.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": f"Bearer {api_key}"},
                json={"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}], "temperature": 0.7, "response_format": {"type": "json_object"}}
            )
            if res.status_code == 200:
                ai_json = json.loads(res.json()['choices'][0]['message']['content'])
                data = {"messages": [{"role": "system", "content": "PPC专家"}, {"role": "user", "content": f"词:{term},费:{spend}"}, {"role": "assistant", "content": f"逻辑:{ai_json.get('reasoning')}\n建议:{ai_json.get('action')}"}]}
                with open(DATA_FILE, "a", encoding="utf-8") as f: f.write(json.dumps(data, ensure_ascii=False) + "\n")
                st.toast("✅ 已保存")
                return ai_json.get('reasoning')
    except Exception as e: st.error(f"Error: {e}")

# === 3. 侧边栏 ===
st.sidebar.title("🧬 控制台 v5.3")
default_key = "sk-55cc3f56742f4e43be099c9489e02911"
deepseek_key = st.sidebar.text_input("🔑 DeepSeek Key", value=default_key, type="password")
product_name = st.sidebar.text_input("📦 产品名称", value="Makeup Mirror")

if os.path.exists(DATA_FILE):
    with open(DATA_FILE, "r", encoding="utf-8") as f: count = sum(1 for _ in f)
    st.sidebar.metric("📚 已积累教材", f"{count} 条")
    with open(DATA_FILE, "r", encoding="utf-8") as f: st.sidebar.download_button("📥 下载数据", f, file_name="finetune.jsonl")

# === 4. 主界面 ===
st.title("🧬 Amazon AI 指挥官 (v5.3 智能读取版)")
st.caption("🚀 修复 Bulk 表头识别问题 | 自动锁定数据行")

c1, c2 = st.columns(2)
with c1:
    file_bulk = st.file_uploader("📂 1. 上传 Bulk 表格 (支持带杂乱表头)", type=['xlsx', 'csv'], key="bulk")
with c2:
    file_term = st.file_uploader("📂 2. 上传 Search Term (已验证成功)", type=['xlsx', 'csv'], key="term")

# 🔥🔥🔥 核心升级：智能读取函数 🔥🔥🔥
def smart_load_bulk(file):
    if not file: return pd.DataFrame()
    try:
        # 1. 如果是 CSV，通常比较规范，直接读
        if file.name.endswith('.csv'):
            return pd.read_csv(file)
        
        # 2. 如果是 Excel，很可能有 metadata 干扰
        # 先盲读前 20 行，不设表头
        df_preview = pd.read_excel(file, header=None, nrows=20, engine='openpyxl')
        
        # 寻找真正的表头行 (包含 'Record Type' 或 'Entity' 的那一行)
        header_row_idx = None
        for i, row in df_preview.iterrows():
            row_str = row.astype(str).str.lower().tolist()
            # 只要这一行里有 record type 或者 entity，就认定它是表头
            if any('record type' in s or 'entity' in s or '实体层级' in s for s in row_str):
                header_row_idx = i
                break
        
        # 如果找到了，就从那一行重新读
        if header_row_idx is not None:
            st.toast(f"✅ 智能定位：在第 {header_row_idx+1} 行发现表头，正在解析...")
            file.seek(0) # 重置文件指针
            return pd.read_excel(file, header=header_row_idx, engine='openpyxl')
        else:
            # 没找到，就硬读第一行
            file.seek(0)
            return pd.read_excel(file, engine='openpyxl')

    except Exception as e:
        st.error(f"智能读取失败: {e}")
        return pd.DataFrame()

# 使用新函数读取
df_bulk = smart_load_bulk(file_bulk)

# Search Term 之前读成功了，保持简单读取
def load_simple(file):
    if not file: return pd.DataFrame()
    try:
        if file.name.endswith('.csv'): return pd.read_csv(file)
        return pd.read_excel(file, engine='openpyxl')
    except: return pd.DataFrame()

df_term = load_simple(file_term)

# 清洗列名
if not df_bulk.empty: df_bulk.columns = df_bulk.columns.astype(str).str.strip()
if not df_term.empty: df_term.columns = df_term.columns.astype(str).str.strip()

# === 解析 Bulk 列 ===
bk_cols = {}
if not df_bulk.empty:
    # 模糊匹配，增加容错率
    cols = df_bulk.columns
    bk_cols = {
        'entity': next((c for c in cols if c in ["实体层级", "Record Type", "Entity"]), None),
        'kw': next((c for c in cols if c in ["关键词文本", "Keyword Text", "Keyword", "Targeting", "Targeting Expression"]), None),
        'bid': next((c for c in cols if c in ["竞价", "Keyword Bid", "Bid"]), None),
        'spend': next((c for c in cols if c in ["花费", "Spend"]), None),
        'sales': next((c for c in cols if c in ["销量", "Sales"]), None),
        'orders': next((c for c in cols if c in ["订单数量", "Orders"]), None),
        'clicks': next((c for c in cols if c in ["点击量", "Clicks"]), None),
    }

    if bk_cols['entity'] and bk_cols['kw']:
        try:
            # 筛选关键词行
            df_kws = df_bulk[df_bulk[bk_cols['entity']].astype(str).str.contains('Keyword|关键词|Targeting', case=False, na=False)].copy()
            for c in [bk_cols['spend'], bk_cols['sales'], bk_cols['orders'], bk_cols['clicks'], bk_cols['bid']]:
                if c: df_kws[c] = pd.to_numeric(df_kws[c], errors='coerce').fillna(0)
            if bk_cols['spend'] and bk_cols['sales']:
                df_kws['ACoS'] = df_kws.apply(lambda x: x[bk_cols['spend']]/x[bk_cols['sales']] if x[bk_cols['sales']]>0 else 0, axis=1)
        except: pass

# === 5. 功能区 ===
tab1, tab2, tab3 = st.tabs(["🧠 AI 自动标注", "📈 看板", "💰 竞价"])

with tab1:
    st.subheader("🧠 训练数据积累")
    if not df_term.empty:
        st_term_col = next((c for c in df_term.columns if c in ["客户搜索词", "Search Term", "Customer Search Term"]), None)
        st_spend_col = next((c for c in df_term.columns if c in ["花费", "Spend"]), None)
        
        if st_term_col and st_spend_col:
            st.success(f"✅ Search Term 数据就绪: {len(df_term)} 行")
            df_term[st_spend_col] = pd.to_numeric(df_term[st_spend_col], errors='coerce').fillna(0)
            
            # 筛选高花费0转化
            st_orders_col = next((c for c in df_term.columns if c in ["订单数量", "Orders", "7 Day Total Orders"]), None)
            if st_orders_col:
                df_term[st_orders_col] = pd.to_numeric(df_term[st_orders_col], errors='coerce').fillna(0)
                mask = (df_term[st_orders_col] == 0) & (df_term[st_spend_col] > 0)
                review_df = df_term[mask].sort_values(by=st_spend_col, ascending=False).head(10)
                
                st.write("👇 重点审查以下“浪费钱”的词：")
                for idx, row in review_df.iterrows():
                    with st.expander(f"{row[st_term_col]} (Cost: ${row[st_spend_col]:.2f})"):
                        c1, c2 = st.columns(2)
                        with c1: 
                            if st.button("❌ 否定 (AI生成理由)", key=f"ai_neg_{idx}"):
                                generate_and_save_ai_thought(deepseek_key, row[st_term_col], row[st_spend_col], 0, 0, "Negative")
                        with c2:
                             if st.button("👀 观察 (AI生成理由)", key=f"ai_kp_{idx}"):
                                generate_and_save_ai_thought(deepseek_key, row[st_term_col], row[st_spend_col], 0, 0, "Keep")
            else:
                st.warning("Search Term 表格缺订单列")
        else:
            st.warning("Search Term 表格缺关键列")
    else:
        st.info("请上传 Search Term 表格")

with tab2:
    st.subheader("📈 账户透视")
    if not df_bulk.empty:
        if 'df_kws' in locals() and not df_kws.empty:
            # 成功展示
            st.success(f"✅ 成功解析 Bulk 数据：共 {len(df_kws)} 个关键词")
            
            # 1. 核心指标
            t_spend = df_kws[bk_cols['spend']].sum()
            t_sales = df_kws[bk_cols['sales']].sum()
            t_acos = t_spend / t_sales if t_sales > 0 else 0
            
            m1, m2, m3 = st.columns(3)
            m1.metric("总花费", f"${t_spend:,.2f}")
            m2.metric("总销售额", f"${t_sales:,.2f}")
            m3.metric("综合 ACoS", f"{t_acos:.2%}")
            
            # 2. 气泡图
            st.markdown("#### 🔍 关键词分布图 (Spend vs Sales)")
            chart_data = df_kws[df_kws[bk_cols['spend']] > 0]
            if not chart_data.empty:
                st.scatter_chart(chart_data, x=bk_cols['spend'], y=bk_cols['sales'], size=bk_cols['clicks'], color='ACoS')
            else:
                st.info("数据中没有花费大于0的词。")
        else:
            st.error("❌ 依然找不到关键词列。")
            st.write("诊断信息：以下是我们找到的列名，请检查是否包含 'Keyword Text' 或 'Targeting'：")
            st.code(list(df_bulk.columns))
            st.dataframe(df_bulk.head(3))
    else:
        st.info("请上传 Bulk 表格")

with tab3:
    st.subheader("💰 竞价优化")
    if 'df_kws' in locals() and not df_kws.empty:
        bad = df_kws[(df_kws[bk_cols['orders']]>0) & (df_kws['ACoS']>0.3)].sort_values(by='ACoS', ascending=False).head(20)
        if not bad.empty:
            st.dataframe(bad[[bk_cols['kw'], bk_cols['bid'], 'ACoS', bk_cols['spend']]], use_container_width=True)
        else:
            st.success("竞价控制完美，无高ACoS词。")
    else:
        st.info("等待 Bulk 数据...")