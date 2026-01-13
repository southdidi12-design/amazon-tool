import streamlit as st
import pandas as pd
import requests
import json
import os
import io

# === 1. 全局配置 ===
st.set_page_config(page_title="Amazon AI 侦探 (v5.4)", layout="wide", page_icon="🕵️")

st.markdown("""
<style>
    .main { background-color: #f8f9fa; }
    .stAlert { padding: 10px; border-radius: 5px; }
    div[data-testid="stExpander"] { background-color: white; border: 1px solid #ddd; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

st.title("🕵️ Amazon AI 侦探 (v5.4 Debug版)")
st.warning("🚧 这个版本专门用来‘抓’列名。请上传文件，然后看下面的红色或黄色提示信息。")

# === 2. 侧边栏 ===
st.sidebar.title("控制台")
deepseek_key = st.sidebar.text_input("DeepSeek Key", value="sk-55cc3f56742f4e43be099c9489e02911", type="password")

# === 3. 智能读取函数 (增强版) ===
def smart_load(file):
    if not file: return None, "未上传"
    try:
        # 1. 尝试直接读
        if file.name.endswith('.csv'):
            return pd.read_csv(file), "CSV模式"
        
        # 2. Excel 智能寻找表头
        df_preview = pd.read_excel(file, header=None, nrows=20, engine='openpyxl')
        header_idx = None
        
        # 扫描前20行，寻找包含关键词的行
        for i, row in df_preview.iterrows():
            row_str = row.astype(str).str.lower().tolist()
            # 只要包含这些词之一，就认为是表头
            if any(k in s for k in ['record type', 'entity', 'campaign name', 'spend', '花费', 'customer search term'] for s in row_str):
                header_idx = i
                break
        
        file.seek(0)
        if header_idx is not None:
            return pd.read_excel(file, header=header_idx, engine='openpyxl'), f"自动定位到第 {header_idx+1} 行做表头"
        else:
            return pd.read_excel(file, engine='openpyxl'), "默认第一行做表头"
            
    except Exception as e:
        return None, str(e)

# === 4. 界面与诊断 ===
c1, c2 = st.columns(2)

# --- 左侧：Bulk 诊断 ---
with c1:
    st.header("📂 1. Bulk 表格区")
    file_bulk = st.file_uploader("上传 Bulk 文件", type=['xlsx', 'csv'], key="bulk")
    
    if file_bulk:
        df_bulk, msg = smart_load(file_bulk)
        if df_bulk is not None and not df_bulk.empty:
            st.success(f"读取成功 ({msg})")
            st.info("👇 **系统读到的列名如下 (请复制这些发给我):**")
            st.code(list(df_bulk.columns))
            
            # 尝试找关键词列
            kw_col = next((c for c in df_bulk.columns if "keyword" in str(c).lower() or "targeting" in str(c).lower() or "关键词" in str(c)), None)
            
            if kw_col:
                st.success(f"✅ 找到关键词列: {kw_col}")
            else:
                st.error("❌ 没找到关键词列！(Looking for: Keyword Text, Targeting, ...)")
                st.write("前 3 行数据预览：")
                st.dataframe(df_bulk.head(3))
        else:
            st.error(f"读取失败: {msg}")

# --- 右侧：Search Term 诊断 ---
with c2:
    st.header("📂 2. Search Term 区")
    file_term = st.file_uploader("上传 ST 文件", type=['xlsx', 'csv'], key="term")
    
    if file_term:
        df_term, msg = smart_load(file_term)
        if df_term is not None and not df_term.empty:
            st.success(f"读取成功 ({msg})")
            st.info("👇 **系统读到的列名如下 (请复制这些发给我):**")
            st.code(list(df_term.columns))
            
            # 尝试模糊匹配寻找订单列
            # 只要列名里包含 "order" 或 "订单"，就抓出来
            order_cols = [c for c in df_term.columns if "order" in str(c).lower() or "订单" in str(c)]
            
            if order_cols:
                st.success(f"✅ 找到疑似订单列: {order_cols}")
                # 自动选第一个当做订单列
                real_order_col = order_cols[0]
                
                # 简单展示数据，证明能用
                st.write(f"正在使用 '{real_order_col}' 列的数据：")
                df_term[real_order_col] = pd.to_numeric(df_term[real_order_col], errors='coerce').fillna(0)
                st.metric("总订单数", int(df_term[real_order_col].sum()))
            else:
                st.error("❌ 依然找不到订单列！(Looking for: Order, 订单...)")
        else:
            st.error(f"读取失败: {msg}")

# === 5. 临时功能区 (验证能否运行) ===
st.divider()
st.subheader("🛠️ 功能验证")
if 'df_term' in locals() and df_term is not None and 'real_order_col' in locals():
    st.write("✅ Search Term 数据已就绪，AI 训练功能可用：")
    c_spend = next((c for c in df_term.columns if "spend" in str(c).lower() or "花费" in str(c)), None)
    c_term = next((c for c in df_term.columns if "search term" in str(c).lower() or "搜索词" in str(c)), None)
    
    if c_spend and c_term:
        mask = (df_term[real_order_col] == 0) & (df_term[c_spend] > 0)
        sample = df_term[mask].head(3)
        for i, row in sample.iterrows():
            st.button(f"❌ 否定: {row[c_term]}", key=f"btn_{i}")
    else:
        st.warning("虽然找到了订单列，但还没找到花费或搜索词列。请看上面的列名列表。")