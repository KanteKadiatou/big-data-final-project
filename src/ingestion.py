import os
from minio import Minio

# --- Configuration MinIO ---
client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="admin123",
    secure=False
)

# --- Buckets ---
BUCKET_RAW = "datalake-raw"

# --- Fonction pour upload ---
def upload_folder(local_folder, bucket, prefix=""):
    for root, _, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            relative = os.path.relpath(local_path, local_folder)
            s3_path = f"{prefix}/{relative}".replace("\\", "/")

            print(f"⬆️ Upload : {local_path}  →  {bucket}/{s3_path}")

            client.fput_object(
                bucket_name=bucket,
                object_name=s3_path,
                file_path=local_path
            )


# --- Upload des données RAW ---
print("\n=== UPLOAD RAW DATA ===")

upload_folder(
    local_folder="../data/Raw/Kaggle",
    bucket=BUCKET_RAW,
    prefix="kaggle"
)

upload_folder(
    local_folder="../data/Raw/Simulated",
    bucket=BUCKET_RAW,
    prefix="simulated"
)

upload_folder(
    local_folder="../data/Raw/YouTube",
    bucket=BUCKET_RAW,
    prefix="youtube"
)

print("\n✔️ Upload terminé !")
