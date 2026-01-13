import streamlit as st
import pandas as pd
import requests
import json
import os
from datetime import datetime

# === 1. 全局配置 (改个图标确保你能看出区别) ===
st.set_page_config(
    page_title="Amazon AI (v5.13 最终版)", 
    layout="wide", 
    page_icon="🔥", # 换成火
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main { background-color: #fff5f5; } /* 微微泛红的背景，证明是新版 */
    div[data-testid="stMetric"] { background-color: white; border: 1px solid #ddd; padding: 10px; border-radius: 8px; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stButton>button { width: 100%; border-radius: 4px; }
    .ai-thought { background-color: #fff; padding: 10px; border: 1px solid #eee; border-radius: 5px; font-size: 13px; margin-top: 5px;}
    
    /* 重点：ASIN 链接样式 */
    .asin-link { 
        font-size: 16px; 
        color: #d93025; /* 红色链接 */
        font-weight: bold; 
        text-decoration: none;
        padding: 5px 0;
        display: block;
    }
</style>
""", unsafe_allow_html=True)

# === 2. 核心逻辑 ===
DATA_FILE = "deepseek_cot_data.jsonl"

def save_manual_label(term, spend, clicks, orders, action):
    train_data = {
        "messages": [
            {"role": "system", "content": "PPC专家"},
            {"role": "user", "content": f"词:{term}, 费:{spend}, 单:{orders}"},
            {"role": "assistant", "content": f"【人工裁决】\n-> 操作: {action}"}
        ]
    }
    with open(DATA_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(train_data, ensure_ascii=False) + "\n")
    st.toast(f"⚡ 已处理: {term}")

def generate_and_save_ai_thought(api_key, term, spend, clicks, orders, user_intent):
    if not api_key: return None
    cpc = spend / clicks if clicks > 0 else 0
    prompt = f"""
    分析师角色。产品: Makeup Mirror。对象: "{term}"。
    输出 JSON (reasoning, action)。
    数据: 花费${spend}, 点击{clicks}, CPC ${cpc:.2f}, 订单{orders}。
    逻辑: 1.CPC? 2.点击量显著性? 3.意图?
    倾向: {user_intent}。
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
                train_data = {
                    "messages": [{"role": "user", "content": f"词:{term}"}, {"role": "assistant", "content": f"{ai_json.get('reasoning')}\n-> {ai_json.get('action')}"}]
                }
                with open(DATA_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(train_data, ensure_ascii=False) + "\n")
                return ai_json.get('reasoning')
    except: return None

# === 3. 侧边栏 ===
st.sidebar.title("🔥 v5.13 验证版") # 标题改了
default_key = "sk-55cc3f56742f4e43be099c9489e02911"
deepseek_key = st.sidebar.text_input("🔑 DeepSeek Key", value=default_key, type="password")
product_name = st.sidebar.text_input("📦 产品名称", value="Makeup Mirror")

st.sidebar.markdown("---")
if os.path.exists(DATA_FILE):
    with open(DATA_FILE, "r", encoding="utf-8") as f: count = sum(1 for _ in f)
    st.sidebar.metric("📚 数据量", f"{count} 条")
    with open(DATA_FILE, "r", encoding="utf-8") as f: st.sidebar.download_button("📥 下载", f, file_name="finetune.jsonl")

# === 4. 主界面 ===
st.title("🔥 Amazon AI 指挥官 (v5.13 强制刷新版)")
st.caption("🔴 如果背景不是微微泛红，说明你的网页没更新！请点右下角 Manage app -> Reboot app")

c1, c2 = st.columns(2)
with c1:
    file_bulk = st.file_uploader("📂 1. Bulk", type=['xlsx', 'csv'], key="bulk")
with c2:
    file_term = st.file_uploader("📂 2. Search Term", type=['xlsx', 'csv'], key="term")

# 读取逻辑
def smart_load_bulk(file):
    if not file: return pd.DataFrame()
    try:
        if file.name.endswith('.csv'): return pd.read_csv(file)
        dfs = pd.read_excel(file, sheet_name=None, engine='openpyxl')
        for sheet_name, df in dfs.items():
            cols = df.columns.astype(str).tolist()
            if any(x in cols for x in ['实体层级', 'Record Type']) and any(x in cols for x in ['关键词文本', 'Keyword Text', '投放']):
                st.toast(f"✅ Bulk OK: {sheet_name}")
                return df
        return pd.DataFrame()
    except: return pd.DataFrame()

df_bulk = smart_load_bulk(file_bulk)

def load_simple(file):
    if not file: return pd.DataFrame()
    try:
        if file.name.endswith('.csv'): return pd.read_csv(file)
        return pd.read_excel(file, engine='openpyxl')
    except: return pd.DataFrame()

df_term = load_simple(file_term)

if not df_bulk.empty: df_bulk.columns = df_bulk.columns.astype(str).str.strip()
if not df_term.empty: df_term.columns = df_term.columns.astype(str).str.strip()

# 全局预处理
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
    "⚡ 透视清洗 (New)", "📈 看板", "💰 竞价", "🏆 黄金", "💫 关联"
])

# --- Tab 1: 快速清洗 (透视版) ---
with tab1:
    st.subheader("⚡ 快速清洗 (透视版 v5.13)")
    
    if not df_term.empty:
        # 强制使用你提供的列名
        c_term = '客户搜索词'
        c_spend = '花费'
        c_orders = '7天总订单数(#)'
        c_clicks = '点击量' # 确保你的表里有这一列
        
        if c_term in df_term.columns:
            df_term[c_spend] = pd.to_numeric(df_term[c_spend], errors='coerce').fillna(0)
            df_term[c_orders] = pd.to_numeric(df_term[c_orders], errors='coerce').fillna(0)
            df_term[c_clicks] = pd.to_numeric(df_term[c_clicks], errors='coerce').fillna(0)
            
            mask = (df_term[c_orders] == 0) & (df_term[c_spend] > 0)
            review_df = df_term[mask].sort_values(by=c_spend, ascending=False).head(20)
            
            if not review_df.empty:
                for idx, row in review_df.iterrows():
                    # 🔥🔥🔥 这里是新的显示逻辑 🔥🔥🔥
                    term_val = str(row[c_term])
                    spend_val = row[c_spend]
                    clicks_val = row[c_clicks]
                    cpc_val = spend_val / clicks_val if clicks_val > 0 else 0
                    
                    # 标题里必须有 | 竖线，且有 CPC
                    label = f"📝 {term_val} | 💸 ${spend_val:.2f} | 🖱️ {int(clicks_val)}次 | CPC ${cpc_val:.2f}"
                    
                    with st.expander(label, expanded=True):
                        # 🔥 ASIN 链接
                        if term_val.lower().startswith("b0"):
                            st.markdown(f"🔗 <a href='https://www.amazon.com/dp/{term_val}' target='_blank' class='asin-link'>👉 点击跳转到亚马逊: {term_val}</a>", unsafe_allow_html=True)
                        
                        c1, c2, c3 = st.columns([1, 1, 3])
                        with c1:
                            if st.button("⚡ 瞬杀", key=f"kill_{idx}", type="primary"):
                                save_manual_label(term_val, spend_val, clicks_val, 0, "Negative")
                        with c2:
                            if st.button("👀 瞬留", key=f"keep_{idx}"):
                                save_manual_label(term_val, spend_val, clicks_val, 0, "Keep")
                        with c3:
                            if st.button("🤖 问AI", key=f"ask_{idx}"):
                                reasoning = generate_and_save_ai_thought(deepseek_key, term_val, spend_val, clicks_val, 0, "Unknown")
                                if reasoning: st.session_state[f"ai_{idx}"] = reasoning
                            if f"ai_{idx}" in st.session_state:
                                st.markdown(f"""<div class="ai-thought">{st.session_state[f"ai_{idx}"]}</div>""", unsafe_allow_html=True)
            else: st.success("无高费0单词")
    else: st.info("请上传 Search Term")

# --- Tab 2-5 (略，复用) ---
with tab2:
    if bulk_ready:
        st.scatter_chart(df_kws[df_kws[bk_cols['spend']]>0], x=bk_cols['spend'], y=bk_cols['sales'], size=bk_cols['clicks'], color='ACoS')
    else: st.info("Wait for Bulk")

with tab3:
    if bulk_ready:
        bad = df_kws[(df_kws[bk_cols['orders']]>0) & (df_kws['ACoS']>0.3)].head(20)
        if not bad.empty: st.dataframe(bad, use_container_width=True)