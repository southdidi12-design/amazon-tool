import os
from datetime import datetime
from pathlib import Path

# === 🌟 HNV Amazon ERP - V72.0 (完全体整合版) ===
VERSION = "V72.0 (All-In-One Complete)"

BASE_DIR = Path(__file__).resolve().parents[1]
DB_FILE = str(BASE_DIR / "hnv_erp_permanent.db")
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
AUTO_AI_STOP_LOSS_KEY = "auto_ai_stop_loss"
AUTO_AI_LIVE_KEY = "auto_ai_live"
AUTO_AI_LAST_RUN_KEY = "auto_ai_last_run"
AUTO_AI_CAMPAIGN_WHITELIST = ["3305-自动-2025-10-13 (AB)测试"]
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
