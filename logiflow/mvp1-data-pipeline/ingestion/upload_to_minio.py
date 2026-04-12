"""

PURPOSE: Upload raw CSV files from local disk to MinIO data lake
WHY: In real data platforms, raw data lives in cloud storage (S3/MinIO), not local files
FLOW: Local CSVs → MinIO bucket → (later: ETL pulls from MinIO)
"""

import os
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

# Load environment variables from .env file
load_dotenv("/mnt/c/Users/HP PRO/Documents/global logistic project/logiflow/.env")

# 1 CONFIGURATION — Load from environment (SECURE)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME", "logiflow-raw")

# Validate that required secrets are loaded
if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise ValueError(" Missing MinIO credentials! Check your .env file")


# Define data directory and files to upload
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
FILES_TO_UPLOAD = {
    "raw/customers.csv": "customers.csv",
    "raw/drivers.csv": "drivers.csv",
    "raw/shipments.csv": "shipments.csv",
    "raw/vehicles.csv": "vehicles.csv",
}


# 2 CONNECT TO MINIO


print("Connecting to MinIO...")

try:
    # Create MinIO client
    # secure=False because we're running locally without HTTPS
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False  # No SSL for local development
    )
    print(" Connected to MinIO successfully\n")
    
except Exception as e:
    print(f" Failed to connect to MinIO: {e}")
    print(" Make sure Docker containers are running: docker compose ps")
    exit(1)


# 3 CREATE BUCKET (if it doesn't exist)


print(f" Checking if bucket '{BUCKET_NAME}' exists...")

try:
    # Check if bucket already exists
    if not client.bucket_exists(BUCKET_NAME):
        # Create the bucket
        client.make_bucket(BUCKET_NAME)
        print(f"Created bucket: {BUCKET_NAME}\n")
    else:
        print(f"Bucket already exists: {BUCKET_NAME}\n")
        
except S3Error as e:
    print(f"Error with bucket: {e}")
    exit(1)


# 4 UPLOAD FILES

print(" Starting file uploads...\n")

uploaded_count = 0

for minio_path, local_filename in FILES_TO_UPLOAD.items():
    # Build full local file path
    local_path = os.path.join(DATA_DIR, local_filename)
    
    # Check if file exists locally
    if not os.path.exists(local_path):
        print(f"  File not found: {local_path}")
        continue
    
    try:
        # Get file size for progress info
        file_size = os.path.getsize(local_path)
        file_size_mb = file_size / (1024 * 1024)  # Convert to MB
        
        print(f"Uploading: {local_filename}")
        print(f"  → Destination: {BUCKET_NAME}/{minio_path}")
        print(f"  → Size: {file_size_mb:.2f} MB")
        
        # UPLOAD THE FILE
        client.fput_object(
            bucket_name=BUCKET_NAME,
            object_name=minio_path,      # Path inside bucket
            file_path=local_path          # Path on your computer
        )
        
        print(f"  Uploaded successfully\n")
        uploaded_count += 1
        
    except S3Error as e:
        print(f"   Upload failed: {e}\n")
    except Exception as e:
        print(f"  Unexpected error: {e}\n")

# 5 VERIFY UPLOADS

print(f" UPLOAD SUMMARY")
print(f" Successfully uploaded: {uploaded_count}/{len(FILES_TO_UPLOAD)} files\n")

print(" Verifying files in MinIO bucket...")

try:
    # List all objects in the bucket
    objects = client.list_objects(BUCKET_NAME, recursive=True)
    
    print(f"\nContents of bucket '{BUCKET_NAME}':")

    
    for obj in objects:
        size_mb = obj.size / (1024 * 1024)
        print(f"  • {obj.object_name} ({size_mb:.2f} MB)")
    

    print("\n All files are in MinIO!")
    print(f" View in browser: http://localhost:9001")
    print(f"   Login: Check your .env file for credentials")
    
except Exception as e:
    print(f" Error listing bucket contents: {e}")