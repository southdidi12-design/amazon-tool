import os
from pathlib import Path

import pandas as pd
import requests
try:
    import streamlit as st
except Exception:
    class _FallbackStreamlit:
        secrets = {}

    st = _FallbackStreamlit()
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import (
    ADGROUP_MEDIA,
    CAMPAIGN_MEDIA,
    CAMPAIGN_NEGATIVE_KEYWORD_MEDIA,
    KEYWORD_MEDIA,
    NEGATIVE_KEYWORD_MEDIA,
    NEGATIVE_TARGET_MEDIA,
    TARGETING_MEDIA,
)

BASE_DIR = Path(__file__).resolve().parents[1]
SECRETS_FILE = BASE_DIR / ".streamlit" / "secrets.toml"


def parse_amazon_secrets(path):
    section = None
    conf = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            section = line[1:-1].strip()
            continue
        if section != "amazon":
            continue
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip()
        if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
            val = val[1:-1]
        conf[key] = val
    return conf


def load_amazon_config():
    conf = {}
    try:
        conf = dict(st.secrets["amazon"])
    except Exception:
        conf = {}

    if SECRETS_FILE.exists():
        try:
            conf.update(parse_amazon_secrets(SECRETS_FILE))
        except Exception:
            pass

    env_map = {
        "AMAZON_CLIENT_ID": "client_id",
        "AMAZON_CLIENT_SECRET": "client_secret",
        "AMAZON_REFRESH_TOKEN": "refresh_token",
        "AMAZON_PROFILE_ID": "profile_id",
    }
    for env_key, key in env_map.items():
        val = os.getenv(env_key)
        if val:
            conf[key] = val

    missing = [k for k in ["client_id", "client_secret", "refresh_token", "profile_id"] if not conf.get(k)]
    if missing:
        return None
    return conf


def get_retry_session():
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1)))
    return session


def get_amazon_session_and_headers():
    conf = load_amazon_config()
    if not conf:
        return None, None
    session = get_retry_session()
    try:
        r = session.post(
            "https://api.amazon.com/auth/o2/token",
            data={
                "grant_type": "refresh_token",
                "refresh_token": conf["refresh_token"],
                "client_id": conf["client_id"],
                "client_secret": conf["client_secret"],
            },
        )
        token = r.json().get("access_token")
        if not token:
            return None, None
        headers = {
            "Authorization": f"Bearer {token}",
            "Amazon-Advertising-API-ClientId": conf["client_id"],
            "Amazon-Advertising-API-Scope": conf["profile_id"],
            "Content-Type": "application/json",
        }
    except Exception:
        return None, None
    return session, headers


def get_media_headers(headers, media_type):
    h = headers.copy()
    h.update({"Content-Type": media_type, "Accept": media_type})
    return h


def _post_list(session, headers, url, media_type, payload_key):
    h = get_media_headers(headers, media_type)
    items = []
    next_token = None
    for _ in range(50):
        body = {"maxResults": 100, "includeExtendedDataFields": True}
        if next_token:
            body["nextToken"] = next_token
        res = session.post(url, headers=h, json=body, timeout=30)
        if res.status_code != 200:
            return items, res.status_code
        payload = res.json()
        batch = payload.get(payload_key, [])
        if not isinstance(batch, list):
            batch = []
        items.extend(batch)
        next_token = payload.get("nextToken") or payload.get("next_token")
        if not next_token:
            break
    return items, 200


def list_sp_campaigns(session, headers, include_extended=False):
    media = CAMPAIGN_MEDIA.get("SP")
    if not media:
        return []
    h = get_media_headers(headers, media)
    body = {}
    if include_extended:
        body["includeExtendedDataFields"] = True
    res = session.post(
        "https://advertising-api.amazon.com/sp/campaigns/list",
        headers=h,
        json=body,
        timeout=30,
    )
    if res.status_code != 200:
        return []
    return res.json().get("campaigns", []) or []


def list_sp_keywords(session, headers):
    items, status = _post_list(
        session,
        headers,
        "https://advertising-api.amazon.com/sp/keywords/list",
        KEYWORD_MEDIA,
        "keywords",
    )
    if status == 200:
        return items
    return []


def list_sp_targets(session, headers):
    items, status = _post_list(
        session,
        headers,
        "https://advertising-api.amazon.com/sp/targets/list",
        TARGETING_MEDIA,
        "targets",
    )
    if status == 200 and items:
        return items
    items, status = _post_list(
        session,
        headers,
        "https://advertising-api.amazon.com/sp/targetingClauses/list",
        TARGETING_MEDIA,
        "targetingClauses",
    )
    if status == 200:
        return items
    return []


def get_row_value(row, keys, default=0):
    if row is None:
        return default
    for key in keys:
        if not key:
            continue
        if key in row and pd.notna(row[key]):
            return row[key]
        key_lower = str(key).lower()
        if key_lower != key and key_lower in row and pd.notna(row[key_lower]):
            return row[key_lower]
    return default


def update_campaign_budget(session, headers, ad_type, campaign_id, new_budget, budget_type):
    media = CAMPAIGN_MEDIA.get(ad_type)
    if not media:
        return False, "unsupported ad_type"
    h = get_media_headers(headers, media)
    if not isinstance(budget_type, str) or not budget_type.strip():
        budget_type = "DAILY"
    payload = {
        "campaigns": [
            {"campaignId": str(campaign_id), "budget": {"budgetType": budget_type, "budget": round(new_budget, 2)}}
        ]
    }
    res = session.put(
        f"https://advertising-api.amazon.com/{ad_type.lower()}/campaigns",
        headers=h,
        json=payload,
        timeout=30,
    )
    if res.status_code in [200, 207]:
        return True, res.json()
    fallback = [{"campaignId": str(campaign_id), "budget": {"budgetType": budget_type, "budget": round(new_budget, 2)}}]
    res2 = session.put(
        f"https://advertising-api.amazon.com/{ad_type.lower()}/campaigns",
        headers=h,
        json=fallback,
        timeout=30,
    )
    if res2.status_code in [200, 207]:
        return True, res2.json()
    return False, f"{res.status_code} {res.text}"


def update_sp_adgroup_bids(session, headers, updates):
    if not updates:
        return True, None
    h = get_media_headers(headers, ADGROUP_MEDIA)
    payload = {"adGroups": updates}
    res = session.put("https://advertising-api.amazon.com/sp/adGroups", headers=h, json=payload, timeout=30)
    if res.status_code in [200, 207]:
        return True, res.json()
    res2 = session.put("https://advertising-api.amazon.com/sp/adGroups", headers=h, json=updates, timeout=30)
    if res2.status_code in [200, 207]:
        return True, res2.json()
    return False, f"{res.status_code} {res.text}"


def update_sp_keyword_bids(session, headers, updates):
    if not updates:
        return True, None
    h = get_media_headers(headers, KEYWORD_MEDIA)
    payload = {"keywords": updates}
    res = session.put("https://advertising-api.amazon.com/sp/keywords", headers=h, json=payload, timeout=30)
    if res.status_code in [200, 207]:
        return True, res.json()
    res2 = session.put("https://advertising-api.amazon.com/sp/keywords", headers=h, json=updates, timeout=30)
    if res2.status_code in [200, 207]:
        return True, res2.json()
    return False, f"{res.status_code} {res.text}"


def update_sp_target_bids(session, headers, updates):
    if not updates:
        return True, None
    h = get_media_headers(headers, TARGETING_MEDIA)

    targets = [u for u in updates if "targetId" in u]
    clauses = [u for u in updates if "targetingClauseId" in u]
    errors = []

    if targets:
        payload = {"targets": targets}
        res = session.put("https://advertising-api.amazon.com/sp/targets", headers=h, json=payload, timeout=30)
        if res.status_code not in [200, 207]:
            res2 = session.put("https://advertising-api.amazon.com/sp/targets", headers=h, json=targets, timeout=30)
            if res2.status_code not in [200, 207]:
                errors.append(f"{res.status_code} {res.text}")

    if clauses:
        payload = {"targetingClauses": clauses}
        res = session.put(
            "https://advertising-api.amazon.com/sp/targetingClauses", headers=h, json=payload, timeout=30
        )
        if res.status_code not in [200, 207]:
            res2 = session.put(
                "https://advertising-api.amazon.com/sp/targetingClauses", headers=h, json=clauses, timeout=30
            )
            if res2.status_code not in [200, 207]:
                errors.append(f"{res.status_code} {res.text}")

    if errors:
        return False, " | ".join(errors)
    return True, None


def update_sp_campaign_bidding(session, headers, updates):
    if not updates:
        return True, None
    media = CAMPAIGN_MEDIA.get("SP")
    if not media:
        return False, "missing campaign media"
    h = get_media_headers(headers, media)
    payload = {"campaigns": updates}
    res = session.put("https://advertising-api.amazon.com/sp/campaigns", headers=h, json=payload, timeout=30)
    if res.status_code in [200, 207]:
        return True, res.json()
    res2 = session.put("https://advertising-api.amazon.com/sp/campaigns", headers=h, json=updates, timeout=30)
    if res2.status_code in [200, 207]:
        return True, res2.json()
    return False, f"{res.status_code} {res.text}"


def _normalize_negative_match_type(match_type):
    if not match_type:
        return "NEGATIVE_PHRASE"
    mt = str(match_type).strip()
    if mt in ["NEGATIVE_EXACT", "NEGATIVE_PHRASE"]:
        return mt
    lowered = mt.lower()
    if lowered in ["negativeexact", "negative_exact"]:
        return "NEGATIVE_EXACT"
    if lowered in ["negativephrase", "negative_phrase"]:
        return "NEGATIVE_PHRASE"
    if lowered in ["exact", "phrase"]:
        return f"NEGATIVE_{lowered.upper()}"
    return mt


def _convert_negative_match_type_lower(match_type):
    mt = _normalize_negative_match_type(match_type)
    if mt == "NEGATIVE_EXACT":
        return "negativeExact"
    if mt == "NEGATIVE_PHRASE":
        return "negativePhrase"
    return mt


def list_sp_negative_keywords(session, headers):
    items, status = _post_list(
        session,
        headers,
        "https://advertising-api.amazon.com/sp/negativeKeywords/list",
        NEGATIVE_KEYWORD_MEDIA,
        "negativeKeywords",
    )
    if status == 200:
        return items
    return []


def list_sp_campaign_negative_keywords(session, headers):
    items, status = _post_list(
        session,
        headers,
        "https://advertising-api.amazon.com/sp/campaignNegativeKeywords/list",
        CAMPAIGN_NEGATIVE_KEYWORD_MEDIA,
        "campaignNegativeKeywords",
    )
    if status == 200:
        return items
    return []


def create_sp_negative_keywords(session, headers, keywords, campaign_level=False):
    if not keywords:
        return True, None
    media = CAMPAIGN_NEGATIVE_KEYWORD_MEDIA if campaign_level else NEGATIVE_KEYWORD_MEDIA
    endpoint = (
        "https://advertising-api.amazon.com/sp/campaignNegativeKeywords"
        if campaign_level
        else "https://advertising-api.amazon.com/sp/negativeKeywords"
    )
    payload_key = "campaignNegativeKeywords" if campaign_level else "negativeKeywords"
    payload = {payload_key: keywords}
    h = get_media_headers(headers, media)
    res = session.post(endpoint, headers=h, json=payload, timeout=30)
    if res.status_code in [200, 207]:
        return True, res.json()

    # matchType fallback to legacy lower case
    fallback = []
    for k in keywords:
        k = dict(k)
        k["matchType"] = _convert_negative_match_type_lower(k.get("matchType"))
        fallback.append(k)
    res2 = session.post(endpoint, headers=h, json={payload_key: fallback}, timeout=30)
    if res2.status_code in [200, 207]:
        return True, res2.json()
    return False, f"{res.status_code} {res.text}"


def delete_sp_negative_keywords(session, headers, keyword_ids, campaign_level=False):
    if not keyword_ids:
        return True, None
    media = CAMPAIGN_NEGATIVE_KEYWORD_MEDIA if campaign_level else NEGATIVE_KEYWORD_MEDIA
    endpoint = (
        "https://advertising-api.amazon.com/sp/campaignNegativeKeywords/delete"
        if campaign_level
        else "https://advertising-api.amazon.com/sp/negativeKeywords/delete"
    )
    payload = {"keywordIdFilter": {"include": [str(k) for k in keyword_ids]}}
    h = get_media_headers(headers, media)
    res = session.post(endpoint, headers=h, json=payload, timeout=30)
    if res.status_code in [200, 207]:
        return True, res.json()
    return False, f"{res.status_code} {res.text}"


def list_sp_negative_targets(session, headers, campaign_id=None, ad_group_id=None, state=None, count=1000):
    h = get_media_headers(headers, NEGATIVE_TARGET_MEDIA)
    params = {"count": count}
    if campaign_id:
        params["campaignIdFilter"] = str(campaign_id)
    if ad_group_id:
        params["adGroupIdFilter"] = str(ad_group_id)
    if state:
        params["stateFilter"] = str(state)
    res = session.get("https://advertising-api.amazon.com/v2/sp/negativeTargets", headers=h, params=params, timeout=30)
    if res.status_code == 200:
        return res.json() or []
    return []


def create_sp_negative_targets(session, headers, targets):
    if not targets:
        return True, None
    h = get_media_headers(headers, NEGATIVE_TARGET_MEDIA)
    res = session.post(
        "https://advertising-api.amazon.com/v2/sp/negativeTargets", headers=h, json=targets, timeout=30
    )
    if res.status_code in [200, 201, 207]:
        return True, res.json()
    return False, f"{res.status_code} {res.text}"


def update_sp_negative_targets(session, headers, targets):
    if not targets:
        return True, None
    h = get_media_headers(headers, NEGATIVE_TARGET_MEDIA)
    res = session.put(
        "https://advertising-api.amazon.com/v2/sp/negativeTargets", headers=h, json=targets, timeout=30
    )
    if res.status_code in [200, 207]:
        return True, res.json()
    return False, f"{res.status_code} {res.text}"


def archive_sp_negative_targets(session, headers, target_ids):
    if not target_ids:
        return True, None
    updates = [{"targetId": str(tid), "state": "ARCHIVED"} for tid in target_ids]
    return update_sp_negative_targets(session, headers, updates)
