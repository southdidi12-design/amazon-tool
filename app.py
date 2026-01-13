import streamlit as st
import pandas as pd
import requests
import json
import os
from datetime import datetime

# === 1. 全局配置 ===
st.set_page_config(
    page_title="Amazon AI 训练师 (v5.2 诊断版)", 
    layout="wide", 
    page_icon="🩺",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    div[data-testid="stMetric"] { background-color: white; border: 1px solid #ddd; padding: 10px; border-radius: 8px; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stButton>button { width: 100%; border-radius: 4px; }
    .stAlert { padding: 10px; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

# === 2. 核心逻辑 ===
DATA_FILE = "deepseek_cot_data.jsonl"

def generate_and_save_ai_thought(api_key, term, spend, clicks, orders, user_intent):
    if not api_key:
        st.error("❌ 需要 API Key")
        return None
    prompt = f"我是亚马逊运营。产品Makeup Mirror。分析词'{term}'，花费${spend}，点击{clicks}，订单{orders}。请输出JSON：1. reasoning(分析过程) 2. action(建议操作)。我的倾向是：{user_intent}。"
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
st.sidebar.title("🩺 控制台 v5.2")
default_key = "sk-55cc3f56742f4e43be099c9489e02911"
deepseek_key = st.sidebar.text_input("🔑 DeepSeek Key", value=default_key, type="password")
product_name = st.sidebar.text_input("📦 产品名称", value="Makeup Mirror")

if os.path.exists(DATA_FILE):
    with open(DATA_FILE, "r", encoding="utf-8") as f: count = sum(1 for _ in f)
    st.sidebar.metric("📚 已积累教材", f"{count} 条")
    with open(DATA_FILE, "r", encoding="utf-8") as f: st.sidebar.download_button("📥 下载数据", f, file_name="finetune.jsonl")

# === 4. 主界面 ===
st.title("🩺 Amazon AI 训练师 (v5.2 诊断修复版)")

c1, c2 = st.columns(2)
with c1:
    file_bulk = st.file_uploader("📂 1. 上传 Bulk 表格 (必须含 Record Type)", type=['xlsx', 'csv'], key="bulk")
with c2:
    file_term = st.file_uploader("📂 2. 上传 Search Term (用于训练)", type=['xlsx', 'csv'], key="term")

def load_data(file):
    if not file: return pd.DataFrame()
    try:
        if file.name.endswith('.csv'): return pd.read_csv(file)
        else: return pd.read_excel(file, engine='openpyxl') # 简化读取逻辑，先读进来再说
    except Exception as e:
        st.error(f"文件读取失败: {e}")
        return pd.DataFrame()

df_bulk = load_data(file_bulk)
df_term = load_data(file_term)

# 清洗列名
if not df_bulk.empty: df_bulk.columns = df_bulk.columns.astype(str).str.strip()
if not df_term.empty: df_term.columns = df_term.columns.astype(str).str.strip()

# === 诊断逻辑 (新增) ===
bk_cols = {}
if not df_bulk.empty:
    # 尝试寻找关键列
    bk_cols = {
        'entity': next((c for c in df_bulk.columns if c in ["实体层级", "Record Type", "Entity"]), None),
        'kw': next((c for c in df_bulk.columns if c in ["关键词文本", "Keyword Text", "Keyword"]), None),
        'bid': next((c for c in df_bulk.columns if c in ["竞价", "Keyword Bid", "Bid"]), None),
        'spend': next((c for c in df_bulk.columns if c in ["花费", "Spend"]), None),
        'sales': next((c for c in df_bulk.columns if c in ["销量", "Sales"]), None),
        'orders': next((c for c in df_bulk.columns if c in ["订单数量", "Orders"]), None),
        'clicks': next((c for c in df_bulk.columns if c in ["点击量", "Clicks"]), None),
    }
    
    # 核心处理：如果找到了 Entity 列，才生成 df_kws
    if bk_cols['entity'] and bk_cols['kw']:
        try:
            df_kws = df_bulk[df_bulk[bk_cols['entity']].astype(str).str.contains('Keyword|关键词', case=False, na=False)].copy()
            for c in [bk_cols['spend'], bk_cols['sales'], bk_cols['orders'], bk_cols['clicks'], bk_cols['bid']]:
                if c: df_kws[c] = pd.to_numeric(df_kws[c], errors='coerce').fillna(0)
            if bk_cols['spend'] and bk_cols['sales']:
                df_kws['ACoS'] = df_kws.apply(lambda x: x[bk_cols['spend']]/x[bk_cols['sales']] if x[bk_cols['sales']]>0 else 0, axis=1)
        except Exception as e:
            st.error(f"数据处理错误: {e}")

# === 5. 功能区 ===
tab1, tab2, tab3 = st.tabs(["🧠 AI 自动标注", "📈 看板 (诊断中...)", "💰 竞价"])

with tab1:
    st.subheader("🧠 训练数据积累")
    if not df_term.empty:
        # Search Term 处理逻辑
        st_term_col = next((c for c in df_term.columns if c in ["客户搜索词", "Search Term", "Customer Search Term"]), None)
        st_spend_col = next((c for c in df_term.columns if c in ["花费", "Spend"]), None)
        
        if st_term_col and st_spend_col:
            st.success(f"✅ Search Term 表格读取成功！包含 {len(df_term)} 行。")
            # ... (简化的按钮显示代码，保持之前逻辑) ...
            mask = (pd.to_numeric(df_term[st_spend_col], errors='coerce') > 0)
            review_df = df_term[mask].head(5)
            for idx, row in review_df.iterrows():
                if st.button(f"分析: {row[st_term_col]}", key=f"btn_{idx}"):
                    generate_and_save_ai_thought(deepseek_key, row[st_term_col], row[st_spend_col], 0, 0, "Check")
        else:
            st.warning(f"⚠️ Search Term 表格缺少关键列。当前列名：{list(df_term.columns)}")
    else:
        st.info("请上传 Search Term 表格")

with tab2:
    st.subheader("📈 账户透视 (诊断模式)")
    
    if not df_bulk.empty:
        if 'df_kws' in locals() and not df_kws.empty:
            # 正常显示图表
            st.success("✅ Bulk 表格解析完美！")
            st.scatter_chart(df_kws[df_kws[bk_cols['spend']]>0], x=bk_cols['spend'], y=bk_cols['sales'], size=bk_cols['clicks'], color='ACoS')
        else:
            # 🚨 诊断报错区
            st.error("❌ 表格已上传，但无法生成图表。原因如下：")
            
            # 检查1: 是否缺少关键列？
            missing_cols = []
            if not bk_cols.get('entity'): missing_cols.append("Record Type (实体层级)")
            if not bk_cols.get('kw'): missing_cols.append("Keyword Text (关键词文本)")
            
            if missing_cols:
                st.warning(f"⚠️ 你的表格里缺少这些列名：{missing_cols}")
                st.write("👉 **当前表格里的列名有：**")
                st.code(list(df_bulk.columns))
                st.info("💡 提示：请检查你是不是上传了 Search Term 报表？图表功能必须用 **Bulk Operation File (批量操作表格)**。")
            else:
                st.warning("⚠️ 列名都对，但筛选 'Keyword' 行时为空。请检查 'Record Type' 列的内容。")
                st.write("前5行数据预览：")
                st.dataframe(df_bulk.head())
    else:
        st.info("请在左侧上传 Bulk 表格。")

with tab3:
    st.write("竞价功能区")