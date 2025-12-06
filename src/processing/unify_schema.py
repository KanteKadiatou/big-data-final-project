#!/usr/bin/env python3
"""
Unify schemas from:
- data/raw/kaggle/*.csv
- data/raw/simulated/students_synthetic.csv
- data/raw/youtube/videos.json

Output:
- data/processed/unified_students.csv
Prints basic stats (counts per source, sample rows).
"""
from pathlib import Path
import pandas as pd
import json
import sys
import logging

# === CONFIG ===
ROOT = Path(".")
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
PROCESSED.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)


def load_kaggle(kaggle_dir: Path):
    dfs = []
    if not kaggle_dir.exists():
        logging.warning(f"Kaggle folder not found: {kaggle_dir}")
        return dfs

    for f in kaggle_dir.glob("*.csv"):
        try:
            df = pd.read_csv(f)
            df["_source_file"] = f.name
            dfs.append(df)
            logging.info(f"Loaded kaggle file {f.name} with shape {df.shape}")
        except Exception as e:
            logging.error(f"Failed to read {f}: {e}")
    return dfs


def load_simulated(sim_path: Path):
    if not sim_path.exists():
        logging.warning(f"Simulated file not found: {sim_path}")
        return None
    try:
        df = pd.read_csv(sim_path)
        df["_source_file"] = sim_path.name
        logging.info(f"Loaded simulated file {sim_path.name} with shape {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Failed to read simulated file {sim_path}: {e}")
        return None


def load_youtube(youtube_file: Path):
    if not youtube_file.exists():
        logging.warning(f"YouTube file not found: {youtube_file}")
        return None
    try:
        # assume videos.json is either a list of objects or newline-delimited json
        try:
            df = pd.read_json(youtube_file, lines=False)
        except ValueError:
            # try line-delimited
            df = pd.read_json(youtube_file, lines=True)
        df["_source_file"] = youtube_file.name
        logging.info(f"Loaded youtube file {youtube_file.name} with shape {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Failed to read youtube file {youtube_file}: {e}")
        return None


def unify_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize column names and ensure required columns exist.
    We create/rename the following canonical columns:
    - student_id
    - student_gender
    - student_age
    - course_id
    - course_name
    - watch_time  (in seconds or minutes depending on source)
    - duration_sec
    - engagement  (float 0-1 if available)
    - timestamp   (datetime or string)
    - source_file
    """
    # copy to avoid side effects
    df = df.copy()

    # lowercase column names for easier matching
    df.columns = [c.strip() for c in df.columns]
    cols_low = {c.lower(): c for c in df.columns}

    def find_column(variants):
        for v in variants:
            if v.lower() in cols_low:
                return cols_low[v.lower()]
        return None

    # mapping attempts (common names)
    mapping = {}
    # student id
    col = find_column(["student_id", "user_id", "id", "student"])
    if col:
        mapping[col] = "student_id"

    # gender
    col = find_column(["gender", "sex", "student_gender"])
    if col:
        mapping[col] = "student_gender"

    # age
    col = find_column(["age", "student_age", "years"])
    if col:
        mapping[col] = "student_age"

    # course id / name
    col = find_column(["course_id", "course", "course_name", "title"])
    if col:
        # choose course_name if it's textual
        mapping[col] = "course_name"

    # watch_time / views / engagement / duration
    col = find_column(["watch_time", "watch_seconds", "watch_minutes", "watch"])
    if col:
        mapping[col] = "watch_time"
    col = find_column(["views", "view_count"])
    if col:
        mapping[col] = "views"
    col = find_column(["duration", "duration_sec", "duration_seconds"])
    if col:
        mapping[col] = "duration_sec"
    col = find_column(["engagement", "engagement_score", "engagement_rate"])
    if col:
        mapping[col] = "engagement"

    # timestamp / published_at / datetime
    col = find_column(["timestamp", "time", "published_at", "date", "datetime"])
    if col:
        mapping[col] = "timestamp"

    # apply rename if mapping present
    if mapping:
        df = df.rename(columns=mapping)

    # create canonical columns if missing
    required = [
        "student_id",
        "student_gender",
        "student_age",
        "course_name",
        "watch_time",
        "duration_sec",
        "engagement",
        "timestamp",
    ]
    for c in required:
        if c not in df.columns:
            df[c] = pd.NA

    # normalize basic types where possible
    # try to coerce numeric columns
    for numcol in ["watch_time", "duration_sec", "engagement", "student_age", "views"]:
        if numcol in df.columns:
            try:
                df[numcol] = pd.to_numeric(df[numcol], errors="coerce")
            except Exception:
                pass

    # ensure timestamp as string (datetime parsing left to next phase)
    if "timestamp" in df.columns:
        try:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        except Exception:
            pass

    # add source_file column if exists under different name
    if "_source_file" in df.columns:
        df["source_file"] = df["_source_file"]
    else:
        df["source_file"] = df.get("source", pd.NA)

    # ensure student_id exists: if missing, create synthetic id from index + source
    if df["student_id"].isna().all():
        df["student_id"] = df.apply(
            lambda row: f"{row.get('source_file','unknown')}_{row.name}", axis=1
        )

    # keep only canonical + source + any extra useful cols
    keep = required + ["student_id", "source_file", "views"]
    keep = list(dict.fromkeys(keep))  # unique preserve order
    existing_keep = [c for c in keep if c in df.columns]
    out = df[existing_keep + [c for c in df.columns if c not in existing_keep]]

    return out


def main():
    # load
    kaggle_dfs = load_kaggle(RAW / "kaggle")
    simulated_df = load_simulated(RAW / "simulated" / "students_synthetic.csv")
    youtube_df = load_youtube(RAW / "youtube" / "videos.json")

    all_frames = []
    stats = {}

    # process kaggle
    kaggle_rows = 0
    for kdf in kaggle_dfs:
        try:
            u = unify_columns(kdf)
            all_frames.append(u)
            kaggle_rows += len(u)
        except Exception as e:
            logging.error(f"Error processing kaggle df: {e}")
    stats["kaggle_rows"] = kaggle_rows

    # simulated
    if simulated_df is not None:
        try:
            u = unify_columns(simulated_df)
            all_frames.append(u)
            stats["simulated_rows"] = len(u)
        except Exception as e:
            logging.error(f"Error processing simulated df: {e}")
            stats["simulated_rows"] = 0
    else:
        stats["simulated_rows"] = 0

    # youtube
    if youtube_df is not None:
        try:
            # youtube may have nested structures; flatten basic fields if needed
            # if 'items' key exists (YouTube API format), normalize
            if "items" in youtube_df.columns and isinstance(youtube_df.loc[0, "items"], (list, dict)):
                # expand items if top-level
                try:
                    items = pd.json_normalize(youtube_df["items"].explode())
                    youtube_df = items
                except Exception:
                    pass
            u = unify_columns(youtube_df)
            all_frames.append(u)
            stats["youtube_rows"] = len(u)
        except Exception as e:
            logging.error(f"Error processing youtube df: {e}")
            stats["youtube_rows"] = 0
    else:
        stats["youtube_rows"] = 0

    # concat
    if len(all_frames) == 0:
        logging.error("No data frames loaded. Exiting.")
        sys.exit(1)

    final = pd.concat(all_frames, ignore_index=True, sort=False)
    logging.info(f"Final unified dataframe shape: {final.shape}")

    out_path = PROCESSED / "unified_students.csv"
    final.to_csv(out_path, index=False)
    logging.info(f"Saved unified csv to {out_path}")

    # print simple stats to stdout for review
    print("\n=== QUICK STATS ===")
    print(f"Kaggle rows: {stats.get('kaggle_rows',0)}")
    print(f"Simulated rows: {stats.get('simulated_rows',0)}")
    print(f"YouTube rows: {stats.get('youtube_rows',0)}")
    print(f"TOTAL rows (final): {len(final)}")
    print("\nSample rows:")
    print(final.head(5).to_string(index=False))

    # exit normally
    return 0


if __name__ == "__main__":
    sys.exit(main())
