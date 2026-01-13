import streamlit as st
import pandas as pd
import requests
import json
import os
from datetime import datetime

# === 1. 全局配置 ===
st.set_page_config(
    page_title="Amazon AI 训练师 (v5.1 懒人版)", 
    layout="wide", 
    page_icon="🧠",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    div[data-testid="stMetric"] { background-color: white; border: 1px solid #ddd; padding: 10px; border-radius: 8px; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stButton>button { width: 100%; border-radius: 4px; }
</style>
""", unsafe_allow_html=True)

# === 2. 核心：AI 自动思考并记录 (CoT 生成器) ===
DATA_FILE = "deepseek_cot_data.jsonl"

def generate_and_save_ai_thought(api_key, term, spend, clicks, orders, user_intent):
    """
    1. 调用 DeepSeek 生成深度思考
    2. 将思考过程 + 结论 保存为训练数据
    """
    if not api_key:
        st.error("❌ 需要 API Key 才能生成 AI 思考！")
        return None

    # 1. 构造发给 AI 的提示词 (Prompt)
    prompt = f"""
    我是亚马逊运营。产品是 Makeup Mirror。
    请分析搜索词："{term}"。
    数据：花费 ${spend}, 点击 {clicks}, 订单 {orders}。
    
    请输出一个 JSON 格式的回答，包含两个字段：
    1. "reasoning": 详细的分析思考过程（先分析数据，再分析语义相关性，最后得出结论）。
    2. "action": 建议操作（Negative Exact / Negative Phrase / Keep / Increase Bid）。
    
    我的预判倾向是：{user_intent} (请参考我的倾向，但如果有理有据可以反驳)
    """

    try:
        # 2. 调用 API
        with st.spinner(f"🧠 AI 正在深度分析 '{term}' ..."):
            res = requests.post(
                "https://api.deepseek.com/chat/completions",
                headers={"Authorization": f"Bearer {api_key}"},
                json={
                    "model": "deepseek-chat",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.7, 
                    "response_format": {"type": "json_object"} 
                }
            )
            
            if res.status_code == 200:
                ai_content = res.json()['choices'][0]['message']['content']
                ai_json = json.loads(ai_content)
                
                reasoning = ai_json.get("reasoning", "AI 未提供详情")
                action = ai_json.get("action", "Unknown")

                # 3. 构造成训练数据格式
                train_data = {
                    "messages": [
                        {"role": "system", "content": "你是一个精通 Amazon PPC 的专家，你的回答必须包含深度的数据分析和逻辑推理。"},
                        {"role": "user", "content": f"分析词: {term}, 花费: ${spend}, 点击: {clicks}, 订单: {orders}"},
                        {"role": "assistant", "content": f"分析逻辑：{reasoning}\n\n建议操作：【{action}】"}
                    ]
                }

                # 4. 保存文件
                with open(DATA_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(train_data, ensure_ascii=False) + "\n")
                
                st.toast(f"✅ 已保存思考路径！\nAI 观点: {reasoning[:30]}...")
                return reasoning
            else:
                st.error(f"API 报错: {res.text}")
    except Exception as e:
        st.error(f"网络错误: {e}")

# === 3. 侧边栏 ===
st.sidebar.title("🧠 控制台 v5.1")

# 🔥🔥🔥 你的 Key 已经预填在这里了 🔥🔥🔥
default_key = "sk-55cc3f56742f4e43be099c9489e02911"
deepseek_key = st.sidebar.text_input("🔑 DeepSeek Key", value=default_key, type="password")

product_name = st.sidebar.text_input("📦 产品名称", value="Makeup Mirror")

st.sidebar.markdown("---")
# 训练数据下载区
if os.path.exists(DATA_FILE):
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        count = sum(1 for _ in f)
    st.sidebar.metric("📚 已积累 CoT 教材", f"{count} 条")
    with open(DATA_FILE, "r", encoding="utf-8") as f:
        st.sidebar.download_button("📥 下载带思考的数据", f, file_name="deepseek_cot_finetune.jsonl")
else:
    st.sidebar.info("暂无数据，快去让 AI 思考吧！")

# === 4. 主界面 ===
st.title("🧠 Amazon AI 训练师 (v5.1 懒人版)")
st.caption("🚀 内置 API Key | 点击按钮生成深度分析 | 自动积累高质量教材")

c1, c2 = st.columns(2)
with c1:
    file_bulk = st.file_uploader("📂 Bulk 表格 (图表)", type=['xlsx', 'csv'], key="bulk")
with c2:
    file_term = st.file_uploader("📂 Search Term (训练核心)", type=['xlsx', 'csv'], key="term")

# 数据读取工具
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
tab1, tab2, tab3, tab4 = st.tabs(["🧠 AI 自动标注 (核心)", "📈 看板", "💰 竞价", "🏆 黄金词"])

# --- Tab 1: AI 自动标注 (Core) ---
with tab1:
    st.subheader("🧠 思维链 (CoT) 数据生产车间")
    st.info("💡 现在不需要输 Key 了！直接点击下面的按钮，AI 就会开始工作。")
    
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
            
            # 筛选：0订单 & 有花费
            mask = (df_term[st_cols['orders']] == 0) & (df_term[st_cols['spend']] > 0)
            review_df = df_term[mask].sort_values(by=st_cols['spend'], ascending=False).head(20)
            
            if not review_df.empty:
                for index, row in review_df.iterrows():
                    with st.expander(f"📝 {row[st_cols['term']]} (花费: ${row[st_cols['spend']]:.2f})", expanded=True):
                        col1, col2, col3 = st.columns([1, 1, 3])
                        
                        term = row[st_cols['term']]
                        sp = row[st_cols['spend']]
                        cl = row[st_cols['clicks']]
                        od = row[st_cols['orders']]
                        
                        # 按钮逻辑：你给个大方向，AI 负责写详细逻辑
                        with col1:
                            if st.button("❌ 生成‘否定’逻辑", key=f"gen_neg_{index}", type="primary"):
                                reason = generate_and_save_ai_thought(deepseek_key, term, sp, cl, od, "Negative")
                                if reason: st.success(f"已存逻辑: {reason}")
                        
                        with col2:
                            if st.button("✨ 生成‘保留’逻辑", key=f"gen_keep_{index}"):
                                reason = generate_and_save_ai_thought(deepseek_key, term, sp, cl, od, "Keep")
                                if reason: st.success(f"已存逻辑: {reason}")
                                
                        with col3:
                            st.caption("👈 点击按钮，DeepSeek 就会帮你写出分析过程，并存入后台。")

            else: st.success("没有发现明显的浪费词。")
        else: st.error("缺少必要列")
    else: st.info("请先上传 Search Term 表格")

# --- Tab 2: 看板 ---
with tab2:
    st.subheader("📈 账户透视")
    if not df_bulk.empty and 'df_kws' in locals():
        # 预处理 Bulk
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

            st.scatter_chart(df_kws[df_kws[bk_cols['spend']]>0], x=bk_cols['spend'], y=bk_cols['sales'], size=bk_cols['clicks'], color='ACoS', height=400)
    else: st.info("请上传 Bulk 表格。")

# --- Tab 3/4 ---
with tab3:
    st.subheader("📉 竞价优化")
    if not df_bulk.empty and 'df_kws' in locals():
        target_acos = 0.3 # 默认值
        bad = df_kws[(df_kws[bk_cols['orders']]>0) & (df_kws['ACoS']>target_acos)].head(20)
        if not bad.empty: st.dataframe(bad[[bk_cols['kw'], 'ACoS', bk_cols['spend']]], use_container_width=True)
    else: st.info("请上传 Bulk 表格")

with tab4:
    st.subheader("🏆 黄金挖掘")
    if not df_bulk.empty and 'df_kws' in locals():
        gold = df_kws[(df_kws[bk_cols['orders']]>=2) & (df_kws['ACoS']<0.2)].head(20)
        if not gold.empty: st.dataframe(gold[[bk_cols['kw'], 'ACoS', bk_cols['sales']]], use_container_width=True)
    else: st.info("请上传 Bulk 表格")