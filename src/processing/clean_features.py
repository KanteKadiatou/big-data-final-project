import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Starting cleaning process...")

    # ======= 1. Nettoyage des colonnes inutiles =======
    cols_to_drop = [c for c in df.columns if "_source_file" in c or "metadata" in c]
    df = df.drop(columns=cols_to_drop, errors="ignore")
    logging.info(f"Dropped {len(cols_to_drop)} metadata/source columns.")

    # ======= 2. Uniformisation des genres =======
    if "student_gender" in df.columns:
        df["student_gender"] = (
            df["student_gender"]
            .astype(str)
            .str.lower()
            .str.strip()
            .replace({"f": "female", "m": "male"})
        )

    # ======= 3. Conversion des types =======
    numeric_cols = [
        "math score", "reading score", "writing score",
        "hours_studied", "score", "assignments_completed",
        "watch_time", "duration_sec", "participation"
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ======= 4. Feature engineering =======
    if all(col in df.columns for col in ["math score", "reading score", "writing score"]):
        df["total_score"] = df[["math score", "reading score", "writing score"]].mean(axis=1)

    # engagement binaire
    if "participation" in df.columns:
        df["is_engaged"] = df["participation"].fillna(0).apply(lambda x: 1 if x > 0 else 0)

    # ======= 5. Gestion des NaN =======
    df = df.fillna({
        "student_gender": "unknown",
        "student_age": df["student_age"].median() if "student_age" in df.columns else None
    })

    df = df.fillna(0)

    logging.info("Cleaning done. Final shape: %s", df.shape)
    return df


def main():
    input_path = Path("data/processed/unified_students.csv")
    output_path = Path("data/processed/clean_students.csv")

    logging.info(f"Loading {input_path}")
    df = pd.read_csv(input_path)

    cleaned = clean_dataframe(df)

    cleaned.to_csv(output_path, index=False)
    logging.info(f"Saved cleaned dataset to {output_path}")


if __name__ == "__main__":
    main()
