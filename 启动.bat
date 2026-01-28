@echo off
:: 1. 强制切换到当前文件夹 (防止路径错乱)
cd /d "%~dp0"

:: 2. 告诉用户正在启动
echo 正在启动 HNV 广告看板...
echo 请勿关闭此窗口...

:: 3. 使用 python -m 启动 (比直接用 streamlit 更稳定)
python -m streamlit run app.py

:: 4. 如果出错，停下来让用户看清楚
if %errorlevel% neq 0 (
    echo.
    echo ==============================
    echo ❌ 启动失败！请截图发给 AI。
    echo ==============================
    pause
)