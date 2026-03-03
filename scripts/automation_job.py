from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
SECRETS_FILE = BASE_DIR / ".streamlit" / "secrets.toml"
sys.path.insert(0, str(BASE_DIR))

from amazon_tool.automation import run_optimization_logic  # noqa: E402
from amazon_tool.config import (  # noqa: E402
    AUTO_AI_ENABLED_KEY,
    AUTO_AI_LAST_RUN_KEY,
    AUTO_AI_LIVE_KEY,
    AUTO_AI_MAX_BID_KEY,
    AUTO_MODEL_AUTO_TRAIN_ENABLED_KEY,
    AUTO_MODEL_LAST_TRAIN_DATE_KEY,
    AUTO_MODEL_LAST_TRAIN_STATUS_KEY,
    AUTO_MODEL_TRAIN_DAYS_KEY,
    AUTO_AI_STOP_LOSS_KEY,
    AUTO_AI_TARGET_ACOS_KEY,
    AUTO_NEGATIVE_ACOS_MULT_KEY,
    AUTO_NEGATIVE_CLICKS_KEY,
    AUTO_NEGATIVE_DAYS_KEY,
    AUTO_NEGATIVE_ENABLED_KEY,
    AUTO_NEGATIVE_LEVEL_KEY,
    AUTO_NEGATIVE_MATCH_KEY,
    AUTO_NEGATIVE_SPEND_KEY,
)
from amazon_tool.db import get_system_value, init_db, set_system_value  # noqa: E402
from amazon_tool.ml_autopilot import save_recommendations_csv, train_and_save_model  # noqa: E402


def _get_float_setting(key, default):
    val = get_system_value(key)
    if val is None:
        return default
    try:
        return float(val)
    except Exception:
        return default


def _get_bool_setting(key, default):
    val = get_system_value(key)
    if val is None:
        return default
    try:
        return str(val).strip() in ["1", "true", "True", "yes", "YES", "on", "ON"]
    except Exception:
        return default


def _get_int_setting(key, default):
    val = get_system_value(key)
    if val is None:
        return default
    try:
        return int(float(val))
    except Exception:
        return default


def _parse_secret_value(path, keys):
    if not path.exists():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            continue
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        if key not in keys:
            continue
        val = val.strip()
        if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
            val = val[1:-1]
        return val
    return None


def get_deepseek_key():
    env_val = os.getenv("DEEPSEEK_API_KEY") or os.getenv("DEEPSEEK_KEY")
    if env_val:
        return env_val
    return _parse_secret_value(SECRETS_FILE, {"deepseek_api_key", "deepseek_key"})


def maybe_auto_train_model():
    enabled = _get_bool_setting(AUTO_MODEL_AUTO_TRAIN_ENABLED_KEY, True)
    if not enabled:
        return

    today = datetime.now().strftime("%Y-%m-%d")
    last_train_date = (get_system_value(AUTO_MODEL_LAST_TRAIN_DATE_KEY) or "").strip()
    if last_train_date == today:
        return

    train_days = max(30, min(365, _get_int_setting(AUTO_MODEL_TRAIN_DAYS_KEY, 180)))
    try:
        result = train_and_save_model(days=train_days)
        rec_path = save_recommendations_csv()
        set_system_value(AUTO_MODEL_LAST_TRAIN_DATE_KEY, today)
        set_system_value(
            AUTO_MODEL_LAST_TRAIN_STATUS_KEY,
            f"ok {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} rows={result.rows} campaigns={result.campaigns} mae={result.mae:.4f} rec={rec_path}",
        )
        print(
            f"Auto model train done: samples={result.rows}, campaigns={result.campaigns}, mae={result.mae:.4f}, days={train_days}"
        )
    except Exception as exc:
        set_system_value(
            AUTO_MODEL_LAST_TRAIN_STATUS_KEY,
            f"error {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {exc}",
        )
        print(f"Auto model train failed: {exc}")


def main():
    init_db()
    maybe_auto_train_model()
    if not _get_bool_setting(AUTO_AI_ENABLED_KEY, False):
        print("Auto pilot disabled.")
        return 0

    target_acos = _get_float_setting(AUTO_AI_TARGET_ACOS_KEY, 25.0)
    max_bid = _get_float_setting(AUTO_AI_MAX_BID_KEY, 2.5)
    stop_loss = _get_float_setting(AUTO_AI_STOP_LOSS_KEY, 15.0)
    live_mode = _get_bool_setting(AUTO_AI_LIVE_KEY, False)
    deepseek_key = get_deepseek_key()

    auto_neg_config = {
        "enabled": _get_bool_setting(AUTO_NEGATIVE_ENABLED_KEY, False),
        "level": get_system_value(AUTO_NEGATIVE_LEVEL_KEY) or "adgroup",
        "match": get_system_value(AUTO_NEGATIVE_MATCH_KEY) or "NEGATIVE_EXACT",
        "spend": _get_float_setting(AUTO_NEGATIVE_SPEND_KEY, 3.0),
        "clicks": _get_float_setting(AUTO_NEGATIVE_CLICKS_KEY, 8.0),
        "acos_mult": _get_float_setting(AUTO_NEGATIVE_ACOS_MULT_KEY, 1.5),
        "days": _get_float_setting(AUTO_NEGATIVE_DAYS_KEY, 7.0),
    }

    logs = run_optimization_logic(
        target_acos,
        max_bid,
        stop_loss,
        live_mode,
        deepseek_key,
        auto_negative_config=auto_neg_config,
    )
    set_system_value(AUTO_AI_LAST_RUN_KEY, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    mode_label = "LIVE" if live_mode else "DRY"
    print(f"Auto pilot done: {len(logs)} actions ({mode_label})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
