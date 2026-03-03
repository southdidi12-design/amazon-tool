import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

from .config import BASE_DIR
from .db import get_db_connection

MODEL_DIR = BASE_DIR / "reports" / "models"
MODEL_PATH = MODEL_DIR / "autopilot_model_v1.json"


FEATURE_COLUMNS = [
    "clicks",
    "impressions",
    "cost",
    "sales",
    "orders",
    "ctr",
    "cpc",
    "cvr",
    "acos",
    "cost_ma3",
    "sales_ma3",
    "orders_ma3",
    "cost_ma7",
    "sales_ma7",
    "orders_ma7",
    "dow",
    "is_weekend",
    "ad_type_sp",
    "ad_type_sb",
    "ad_type_sd",
]


@dataclass
class TrainResult:
    model_path: str
    rows: int
    campaigns: int
    mae: float


def _safe_div(a, b):
    return np.where(b > 0, a / b, 0.0)


def _load_campaign_daily_df(days: int = 180) -> pd.DataFrame:
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=max(30, int(days)))

    conn = get_db_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT date, campaign_id, campaign_name, COALESCE(ad_type, 'SP') as ad_type,
                   COALESCE(cost,0) as cost,
                   COALESCE(sales,0) as sales,
                   COALESCE(clicks,0) as clicks,
                   COALESCE(impressions,0) as impressions,
                   COALESCE(orders,0) as orders
            FROM campaign_reports
            WHERE date >= ? AND date <= ?
            ORDER BY campaign_id, date
            """,
            conn,
            params=(start_date.isoformat(), end_date.isoformat()),
        )
    finally:
        conn.close()

    if df.empty:
        return df

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "campaign_id"]).copy()
    df["campaign_id"] = df["campaign_id"].astype(str)
    df["ad_type"] = df["ad_type"].fillna("SP").astype(str).str.upper()

    for col in ["cost", "sales", "clicks", "impressions", "orders"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    return df


def _build_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    out = out.sort_values(["campaign_id", "date"]).reset_index(drop=True)

    out["ctr"] = _safe_div(out["clicks"], out["impressions"])
    out["cpc"] = _safe_div(out["cost"], out["clicks"])
    out["cvr"] = _safe_div(out["orders"], out["clicks"])
    out["acos"] = _safe_div(out["cost"], out["sales"])

    grp = out.groupby("campaign_id", group_keys=False)

    for base in ["cost", "sales", "orders"]:
        out[f"{base}_ma3"] = grp[base].transform(lambda s: s.rolling(3, min_periods=1).mean())
        out[f"{base}_ma7"] = grp[base].transform(lambda s: s.rolling(7, min_periods=1).mean())

    out["dow"] = out["date"].dt.dayofweek.astype(float)
    out["is_weekend"] = out["dow"].isin([5, 6]).astype(float)

    ad_type = out["ad_type"].str.upper()
    out["ad_type_sp"] = (ad_type == "SP").astype(float)
    out["ad_type_sb"] = (ad_type == "SB").astype(float)
    out["ad_type_sd"] = (ad_type == "SD").astype(float)

    return out


def _build_targets(feat_df: pd.DataFrame) -> pd.DataFrame:
    if feat_df.empty:
        return feat_df

    out = feat_df.copy()
    grp = out.groupby("campaign_id", group_keys=False)

    out["next_cost"] = grp["cost"].shift(-1)
    out["next_sales"] = grp["sales"].shift(-1)
    out["next_orders"] = grp["orders"].shift(-1)

    next_acos = _safe_div(out["next_cost"], out["next_sales"])

    conditions = [
        (out["next_sales"] <= 0) & (out["next_cost"] >= 5),
        (next_acos > 0.40) & (out["next_cost"] >= 3),
        (next_acos > 0.30) & (out["next_orders"] < 1),
        (next_acos < 0.20) & (out["next_orders"] >= 1),
        (next_acos < 0.15) & (out["next_orders"] >= 2),
    ]
    choices = [0.88, 0.92, 0.96, 1.06, 1.10]
    out["target_multiplier"] = np.select(conditions, choices, default=1.0)

    out = out.dropna(subset=["next_cost", "next_sales", "next_orders"]).copy()
    return out


def _fit_linear_regression(X: np.ndarray, y: np.ndarray) -> Tuple[float, np.ndarray]:
    X_aug = np.hstack([np.ones((X.shape[0], 1)), X])
    beta, *_ = np.linalg.lstsq(X_aug, y, rcond=None)
    intercept = float(beta[0])
    coef = beta[1:].astype(float)
    return intercept, coef


def _prepare_training_matrix(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, Dict[str, Dict[str, float]]]:
    matrix_df = df[FEATURE_COLUMNS].copy()

    stats = {}
    for col in FEATURE_COLUMNS:
        mean = float(matrix_df[col].mean())
        std = float(matrix_df[col].std())
        if std <= 1e-9:
            std = 1.0
        stats[col] = {"mean": mean, "std": std}
        matrix_df[col] = (matrix_df[col] - mean) / std

    X = matrix_df.to_numpy(dtype=float)
    y = df["target_multiplier"].to_numpy(dtype=float)
    return X, y, stats


def _predict_with_model(features: pd.DataFrame, model: dict) -> np.ndarray:
    X = features[FEATURE_COLUMNS].copy()
    for col in FEATURE_COLUMNS:
        m = float(model["feature_stats"][col]["mean"])
        s = float(model["feature_stats"][col]["std"])
        X[col] = (X[col] - m) / (s if abs(s) > 1e-9 else 1.0)
    raw = model["intercept"] + np.dot(X.to_numpy(dtype=float), np.array(model["coef"], dtype=float))
    return np.clip(raw, 0.80, 1.20)


def train_and_save_model(days: int = 180, model_path: Path = MODEL_PATH) -> TrainResult:
    raw_df = _load_campaign_daily_df(days=days)
    feat_df = _build_features(raw_df)
    train_df = _build_targets(feat_df)

    if train_df.empty or len(train_df) < 80:
        raise RuntimeError("训练样本不足，请先同步更多历史数据（建议至少80条样本）")

    X, y, stats = _prepare_training_matrix(train_df)
    intercept, coef = _fit_linear_regression(X, y)

    model = {
        "version": "v1",
        "trained_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "feature_columns": FEATURE_COLUMNS,
        "intercept": intercept,
        "coef": [float(x) for x in coef.tolist()],
        "feature_stats": stats,
        "target": "bid_multiplier",
        "clip": {"min": 0.80, "max": 1.20},
    }

    pred = np.clip(intercept + np.dot(X, coef), 0.80, 1.20)
    mae = float(np.mean(np.abs(pred - y)))

    model_dir = Path(model_path).parent
    model_dir.mkdir(parents=True, exist_ok=True)
    Path(model_path).write_text(json.dumps(model, ensure_ascii=False, indent=2), encoding="utf-8")

    campaigns = train_df["campaign_id"].nunique()
    return TrainResult(str(model_path), int(len(train_df)), int(campaigns), mae)


def load_model(model_path: Path = MODEL_PATH) -> dict:
    path = Path(model_path)
    if not path.exists():
        raise FileNotFoundError(f"模型不存在: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def build_latest_recommendations(model_path: Path = MODEL_PATH) -> pd.DataFrame:
    model = load_model(model_path)

    raw_df = _load_campaign_daily_df(days=120)
    feat_df = _build_features(raw_df)
    if feat_df.empty:
        return pd.DataFrame()

    latest = feat_df.sort_values(["campaign_id", "date"]).groupby("campaign_id", as_index=False).tail(1).copy()
    if latest.empty:
        return pd.DataFrame()

    pred_mult = _predict_with_model(latest, model)
    latest["pred_multiplier"] = pred_mult

    def _action(mult):
        if mult >= 1.05:
            return "提高出价"
        if mult <= 0.95:
            return "降低出价"
        return "保持"

    latest["action"] = latest["pred_multiplier"].apply(_action)
    latest["suggested_bid_factor"] = latest["pred_multiplier"].round(3)
    latest["reason"] = latest.apply(
        lambda r: f"ACOS={r['acos']:.2f}, CVR={r['cvr']:.3f}, 7日销售均值={r['sales_ma7']:.2f}", axis=1
    )

    cols = [
        "date",
        "campaign_id",
        "campaign_name",
        "ad_type",
        "cost",
        "sales",
        "orders",
        "acos",
        "cvr",
        "suggested_bid_factor",
        "action",
        "reason",
    ]
    out = latest[cols].copy()
    out = out.sort_values(["action", "acos"], ascending=[True, False]).reset_index(drop=True)
    return out


def save_recommendations_csv(output_path: Path = None, model_path: Path = MODEL_PATH) -> str:
    if output_path is None:
        output_path = BASE_DIR / "reports" / "model_recommendations.csv"
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df = build_latest_recommendations(model_path=model_path)
    if df.empty:
        raise RuntimeError("暂无可生成推荐的数据，请先同步并训练模型")

    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    return str(output_path)
