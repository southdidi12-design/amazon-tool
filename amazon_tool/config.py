import os
from datetime import datetime
from pathlib import Path

# === 🌟 HNV Amazon ERP - V72.0 (完全体整合版) ===
VERSION = "V72.0 (All-In-One Complete)"

BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_FILE = BASE_DIR / "hnv_erp_permanent.db"


def _resolve_db_file():
    raw = os.getenv("HNV_DB_FILE", "").strip()
    if not raw:
        db_path = DEFAULT_DB_FILE
    else:
        db_path = Path(raw)
        if not db_path.is_absolute():
            db_path = (BASE_DIR / db_path).resolve()
    return str(db_path)


DB_FILE = _resolve_db_file()
AD_TYPE_SP = "SP"
AD_TYPE_SB = "SB"
AD_TYPE_SD = "SD"
MIN_BUDGET = 1.0
MIN_BID = 0.02

AUTO_SYNC_DEFAULT_DAYS = 7
AUTO_SYNC_MAX_DAYS = 30
AUTO_SYNC_INTERVAL_SECONDS = 60 * 60 * 3
AUTO_SYNC_TS_KEY = "last_auto_sync_ts"
SYNC_STATUS_KEY = "last_sync_status"
SYNC_ERROR_KEY = "last_sync_error"
SYNC_DAYS_KEY = "last_sync_days"
REPORT_POLL_MAX = 180
REPORT_POLL_SLEEP_SECONDS = 2
AUTO_SYNC_REFRESH_DAYS = 2
AUTO_AI_ENABLED_KEY = "auto_ai_enabled"
AUTO_AI_TARGET_ACOS_KEY = "auto_ai_target_acos"
AUTO_AI_MAX_BID_KEY = "auto_ai_max_bid"
AUTO_AI_HARVEST_MIN_ORDERS_KEY = "auto_ai_harvest_min_orders"
AUTO_AI_MIN_BID_KEY = "auto_ai_min_bid"
AUTO_AI_MIN_BID_CLOSE_KEY = "auto_ai_min_bid_close"
AUTO_AI_MIN_BID_LOOSE_KEY = "auto_ai_min_bid_loose"
AUTO_AI_MIN_BID_SUB_KEY = "auto_ai_min_bid_sub"
AUTO_AI_MIN_BID_COMP_KEY = "auto_ai_min_bid_comp"
AUTO_AI_MAX_UP_PCT_KEY = "auto_ai_max_up_pct"
AUTO_AI_BASELINE_MIN_KEY = "auto_ai_baseline_min_bid"
AUTO_AI_MEMORY_FLOOR_RATIO_KEY = "auto_ai_memory_floor_ratio"
AUTO_KEYWORD_POOL_ENABLED_KEY = "auto_keyword_pool_enabled"
AUTO_KEYWORD_POOL_PATH_KEY = "auto_keyword_pool_path"
AUTO_KEYWORD_POOL_DAILY_MAX_KEY = "auto_keyword_pool_daily_max"
AUTO_KEYWORD_POOL_NEG_CLICKS_KEY = "auto_keyword_pool_neg_clicks"
AUTO_KEYWORD_POOL_NEG_ORDERS_KEY = "auto_keyword_pool_neg_orders"
AUTO_KEYWORD_POOL_MIN_FLOW_KEY = "auto_keyword_pool_min_flow"
AUTO_AI_STOP_LOSS_KEY = "auto_ai_stop_loss"
AUTO_AI_LIVE_KEY = "auto_ai_live"
AUTO_AI_LAST_RUN_KEY = "auto_ai_last_run"
AUTO_AI_LEARNING_ENABLED_KEY = "auto_ai_learning_enabled"
AUTO_AI_LEARNING_RATE_KEY = "auto_ai_learning_rate"
AUTO_AI_LEARNING_LAST_DATE_KEY = "auto_ai_learning_last_date"
AUTO_AI_LEARNING_NOTE_KEY = "auto_ai_learning_note"
AUTO_AI_CAMPAIGN_WHITELIST = [
    "176597893951887",  # existing managed campaign (3305)
    "294775544176956",
    "311991007657500",
    "342291596465765",
    "345874865906860",
    "349858316916812",
    "380577349173047",
    "417056838439826",
    "434286851324199",
    "444508631053390",
    "467301712320201",
    "494059472666157",
    "508946103570095",
    "515049186715013",
    "538987412589483",
    # Added 3305 campaigns to bring high-ACOS non-managed traffic under AI control
    "477404577922959",
    "558893454246057",
    "277059222932077",
    "292545543361874",
]
AUTO_AI_CAMPAIGN_DAILY_BUDGET = 10.0
AUTO_NEGATIVE_ENABLED_KEY = "auto_negative_enabled"
AUTO_NEGATIVE_LEVEL_KEY = "auto_negative_level"
AUTO_NEGATIVE_MATCH_KEY = "auto_negative_match"
AUTO_NEGATIVE_SPEND_KEY = "auto_negative_spend"
AUTO_NEGATIVE_CLICKS_KEY = "auto_negative_clicks"
AUTO_NEGATIVE_ACOS_MULT_KEY = "auto_negative_acos_mult"
AUTO_NEGATIVE_DAYS_KEY = "auto_negative_days"
AUTO_NEGATIVE_LAST_RUN_KEY = "auto_negative_last_run"
AUTO_NEGATIVE_PROTECT_KEY = "auto_negative_protect_keywords"
AUTO_NEGATIVE_PROTECT_MODE_KEY = "auto_negative_protect_mode"

CAMPAIGN_MEDIA = {
    AD_TYPE_SP: "application/vnd.spCampaign.v3+json",
    AD_TYPE_SB: "application/vnd.sbCampaign.v3+json",
    AD_TYPE_SD: "application/vnd.sdCampaign.v3+json",
}
ADGROUP_MEDIA = "application/vnd.spAdGroup.v3+json"
KEYWORD_MEDIA = "application/vnd.spKeyword.v3+json"
TARGETING_MEDIA = "application/vnd.spTargetingClause.v3+json"
NEGATIVE_KEYWORD_MEDIA = "application/vnd.spNegativeKeyword.v3+json"
CAMPAIGN_NEGATIVE_KEYWORD_MEDIA = "application/vnd.spCampaignNegativeKeyword.v3+json"
NEGATIVE_TARGET_MEDIA = "application/vnd.spNegativeTargetingClause.v2+json"


def _parse_env_list(value):
    items = []
    for chunk in str(value or "").replace(",", "\n").splitlines():
        chunk = chunk.strip()
        if chunk:
            items.append(chunk)
    return items


def get_auto_ai_campaign_whitelist():
    env_val = os.getenv("AUTO_AI_CAMPAIGN_WHITELIST")
    if env_val:
        return _parse_env_list(env_val)
    return AUTO_AI_CAMPAIGN_WHITELIST


def get_auto_ai_campaign_daily_budget():
    env_val = os.getenv("AUTO_AI_CAMPAIGN_DAILY_BUDGET")
    if env_val:
        try:
            return float(env_val)
        except Exception:
            return AUTO_AI_CAMPAIGN_DAILY_BUDGET
    return AUTO_AI_CAMPAIGN_DAILY_BUDGET


def get_real_today():
    return datetime.now().date()

