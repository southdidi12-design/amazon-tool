#!/bin/zsh

set -e
cd "$(dirname "$0")"

echo "正在启动 HNV 广告看板..."
echo "请勿关闭此窗口..."

if [[ -f "db_path.txt" ]]; then
  export HNV_DB_FILE="$(head -n 1 db_path.txt | tr -d '\r')"
  if [[ -n "$HNV_DB_FILE" ]]; then
    echo "使用共享数据库: $HNV_DB_FILE"
  fi
fi

export STREAMLIT_BROWSER_GATHER_USAGE_STATS=false

pick_python() {
  if [[ -x ".venv/bin/python" ]]; then
    echo ".venv/bin/python"
    return
  fi
  if [[ -x ".venv_mac/bin/python" ]]; then
    echo ".venv_mac/bin/python"
    return
  fi
  if command -v python3 >/dev/null 2>&1; then
    echo "python3"
    return
  fi
  if command -v python >/dev/null 2>&1; then
    echo "python"
    return
  fi
  echo ""
}

PY_CMD="$(pick_python)"
if [[ -z "$PY_CMD" ]]; then
  echo "❌ 未找到 Python，请先安装 Python 3。"
  read -r "?按回车退出..."
  exit 1
fi

if ! "$PY_CMD" -c "import streamlit" >/dev/null 2>&1; then
  echo "检测到缺少 streamlit，正在自动安装依赖到本地环境 .venv_mac ..."
  if [[ ! -x ".venv_mac/bin/python" ]]; then
    "$PY_CMD" -m venv .venv_mac
  fi
  .venv_mac/bin/python -m pip install --upgrade pip
  .venv_mac/bin/python -m pip install -r requirements.txt
  PY_CMD=".venv_mac/bin/python"
fi

(
  if command -v nc >/dev/null 2>&1; then
    for _ in {1..40}; do
      for port in {8501..8510}; do
        if nc -z localhost "$port" >/dev/null 2>&1; then
          open "http://localhost:$port" >/dev/null 2>&1
          exit 0
        fi
      done
      sleep 1
    done
  fi
  open "http://localhost:8501" >/dev/null 2>&1
) &
exec "$PY_CMD" -m streamlit run app.py --server.headless true --browser.gatherUsageStats false
