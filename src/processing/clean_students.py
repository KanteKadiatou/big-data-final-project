#!/usr/bin/env python3
"""
clean_students.py

Nettoyage + feature engineering pour `data/processed/unified_students.csv`.

Sorties:
- data/processed/clean_students.parquet
- data/processed/clean_students.csv
- data/processed/kpi_summary.json
"""
import re
from pathlib import Path
import json
import logging
from typing import Optional

import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

ROOT = Path(".")
PROCESSED = ROOT / "data" / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)


# ----------------------
# Utilities
# ----------------------
ISO8601_DURATION_RE = re.compile(
    r"^P(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)$",
    flags=re.IGNORECASE,
)


def iso8601_duration_to_seconds(s: str) -> Optional[int]:
    """Convertit un format ISO8601 'PT1H2M3S' en secondes. Retourne None si non convertible."""
    if not isinstance(s, str):
        return None
    s = s.strip().upper()
    m = ISO8601_DURATION_RE.match(s)
    if not m:
        # try simpler patterns: "1:02:03" or "02:03"
        try:
            parts = [int(x) for x in s.split(":")]
            if len(parts) == 3:
                return parts[0] * 3600 + parts[1] * 60 + parts[2]
            if len(parts) == 2:
                return parts[0] * 60 + parts[1]
            if len(parts) == 1:
                return int(parts[0])
        except Exception:
            return None
    hours = int(m.group("hours") or 0)
    minutes = int(m.group("minutes") or 0)
    seconds = int(m.group("seconds") or 0)
    return hours * 3600 + minutes * 60 + seconds


def coerce_numeric_series(s: pd.Series) -> pd.Series:
    """Essaye de convertir une série en numérique, en gérant les strings et les virgules."""
    if s.dtype == object:
        s = s.str.replace(",", ".", regex=False)
        # remove stray non-numeric chars except . and -
        s = s.str.replace(r"[^\d\.\-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")


def cap_outliers_iqr(s: pd.Series, lower_q=0.01, upper_q=0.99) -> pd.Series:
    """Cap les outliers en se basant sur quantiles robustes (simple)"""
    if s.dropna().empty:
        return s
    low = s.quantile(lower_q)
    high = s.quantile(upper_q)
    return s.clip(lower=low, upper=high)


# ----------------------
# Cleaning + feature engineering
# ----------------------
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting cleaning process...")

    df = df.copy()

    # 1) Drop obvious metadata columns
    cols_to_drop = [c for c in df.columns if "_source_file" in c or "metadata" in c.lower()]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop, errors="ignore")
        logging.info("Dropped metadata columns: %s", cols_to_drop)

    # 2) Strip column names
    df.columns = [c.strip() for c in df.columns]

    # 3) Normalize gender with many variants
    if "student_gender" in df.columns:
        mapping = {
            "f": "female",
            "female": "female",
            "woman": "female",
            "femme": "female",
            "m": "male",
            "male": "male",
            "man": "male",
            "homme": "male",
            "0": "unknown",
            "1": "unknown",
            "nan": "unknown",
        }
        df["student_gender"] = (
            df["student_gender"].astype(str).str.lower().str.strip().map(mapping).fillna("unknown")
        )
    else:
        df["student_gender"] = "unknown"

    # 4) Coerce numeric columns sensibly
    numeric_candidates = [
        "watch_time",
        "duration_sec",
        "duration",
        "views",
        "engagement",
        "student_age",
        "hours_studied",
        "assignments_completed",
        "participation",
    ]
    for col in numeric_candidates:
        if col in df.columns:
            df[col] = coerce_numeric_series(df[col])

    # 5) Handle duration strings (YouTube etc.)
    # if duration present as string (e.g., 'PT1H2M3S' or '1:02:03'), convert to seconds
    if "duration" in df.columns and df["duration"].notna().any():
        df["duration_sec"] = df.apply(
            lambda r: r["duration_sec"]
            if pd.notna(r.get("duration_sec"))
            else iso8601_duration_to_seconds(r.get("duration")),
            axis=1,
        )

    # ensure duration_sec numeric
    if "duration_sec" in df.columns:
        df["duration_sec"] = pd.to_numeric(df["duration_sec"], errors="coerce")

    # 6) Normalize watch_time units: if watch_time is small (<100) assume minutes, else seconds.
    if "watch_time" in df.columns:
        # if most values < 100 and there's no "s" in raw strings, treat as minutes
        median_watch = df["watch_time"].median(skipna=True)
        logging.info("Median watch_time before normalization: %s", median_watch)
        # Heuristic: if durations exist and watch_time median < 100 and many durations are > watch_time, may be minutes
        if pd.notna(median_watch) and median_watch > 0 and median_watch < 100:
            # convert minutes to seconds if duration suggests seconds
            # safe heuristic — user check recommended
            df["watch_time_sec"] = df["watch_time"] * 60
        else:
            df["watch_time_sec"] = df["watch_time"]

    # 7) Parse timestamp to datetime (UTC)
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    elif "published_at" in df.columns:
        df["timestamp"] = pd.to_datetime(df["published_at"], errors="coerce", utc=True)
    else:
        df["timestamp"] = pd.NaT

    # 8) Create student_id if missing
    if "student_id" not in df.columns or df["student_id"].isna().all():
        logging.info("Creating synthetic student_id from index + source")
        df["student_id"] = df.apply(lambda r: f"{r.get('source_file','unknown')}_{r.name}", axis=1)

    # 9) Feature engineering
    # completion_rate = watch_time_sec / duration_sec
    df["completion_rate"] = np.nan
    if "watch_time_sec" in df.columns and "duration_sec" in df.columns:
        df["completion_rate"] = (df["watch_time_sec"] / df["duration_sec"]).replace([np.inf, -np.inf], np.nan)

    # engagement_rate fallback to engagement/duration if possible
    if "engagement" in df.columns and "duration_sec" in df.columns:
        df["engagement_rate"] = (df["engagement"] / df["duration_sec"]).replace([np.inf, -np.inf], np.nan)
    elif "engagement" in df.columns:
        df["engagement_rate"] = df["engagement"]

    # is_engaged binary
    if "participation" in df.columns:
        df["is_engaged"] = df["participation"].fillna(0).apply(lambda x: 1 if x > 0 else 0)
    else:
        # fallback: completion_rate > 0.5 as engaged
        df["is_engaged"] = df["completion_rate"].fillna(0).apply(lambda x: 1 if x >= 0.5 else 0)

    # timestamp derived features
    df["hour"] = df["timestamp"].dt.hour
    df["weekday"] = df["timestamp"].dt.weekday

    # 10) Remove duplicates (heuristic): same student_id + course_name + timestamp
    pre_dup = len(df)
    key_cols = [c for c in ["student_id", "course_name", "timestamp"] if c in df.columns]
    if key_cols:
        df = df.drop_duplicates(subset=key_cols, keep="first")
        logging.info("Dropped %d duplicate rows based on %s", pre_dup - len(df), key_cols)

    # 11) Cap outliers on numeric columns
    for col in ["watch_time_sec", "duration_sec", "views"]:
        if col in df.columns:
            df[col] = cap_outliers_iqr(df[col])

    # 12) Missing value strategy (conservative)
    # median for age, 0 for numeric that make sense, 'unknown' for categorical
    if "student_age" in df.columns:
        median_age = int(df["student_age"].median(skipna=True)) if df["student_age"].notna().any() else None
        if median_age is not None:
            df["student_age"] = df["student_age"].fillna(median_age)
    df["student_gender"] = df["student_gender"].fillna("unknown")

    # For numeric columns that remain NaN, let them be NaN (to avoid hiding issues), but for compatibility we can fill zeros for some
    numeric_fill_zero = ["watch_time_sec", "duration_sec", "views", "engagement"]
    for c in numeric_fill_zero:
        if c in df.columns:
            df[c] = df[c].fillna(0)

    logging.info("Cleaning done. Final shape: %s", df.shape)
    return df


def save_outputs(df: pd.DataFrame, processed_dir: Path = PROCESSED):
    processed_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = processed_dir / "clean_students.parquet"
    csv_path = processed_dir / "clean_students.csv"
    kpi_path = processed_dir / "kpi_summary.json"

    # save parquet
    df.to_parquet(parquet_path, index=False)
    logging.info("Saved parquet to %s", parquet_path)

    # save csv
    df.to_csv(csv_path, index=False)
    logging.info("Saved csv to %s", csv_path)

    # simple KPIs
    kpi = {
        "rows": int(len(df)),
        "unique_students": int(df["student_id"].nunique()) if "student_id" in df.columns else None,
        "avg_completion_rate": float(df["completion_rate"].mean(skipna=True)) if "completion_rate" in df.columns else None,
        "median_age": int(df["student_age"].median(skipna=True)) if "student_age" in df.columns else None,
        "top_courses_by_views": df.groupby("course_name")["views"].sum().sort_values(ascending=False).head(10).to_dict()
        if "course_name" in df.columns and "views" in df.columns else {},
    }

    with open(kpi_path, "w", encoding="utf-8") as f:
        json.dump(kpi, f, ensure_ascii=False, indent=2)
    logging.info("Saved KPI summary to %s", kpi_path)


def main():
    input_path = PROCESSED / "unified_students.csv"
    if not input_path.exists():
        logging.error("Input file not found: %s", input_path)
        raise SystemExit(1)

    logging.info("Loading %s", input_path)
    df = pd.read_csv(input_path)

    cleaned = clean_dataframe(df)
    save_outputs(cleaned)
    logging.info("All done.")


if __name__ == "__main__":
    main()
