"""
common/storage.py

Single object-storage interface for LogiFlow, with two interchangeable
backends:

  STORAGE_BACKEND=minio  -> local MinIO container (default, dev)
  STORAGE_BACKEND=s3     -> real AWS S3 (cloud)

Every caller uses the same four methods and never touches either SDK
directly. This is the file -- and the only file -- that changed when
LogiFlow migrated from MinIO to S3.
"""

import io
import logging

from common.config import settings

log = logging.getLogger(__name__)


class MinIOStorage:
    """Local development backend -- MinIO container."""

    def __init__(self) -> None:
        from minio import Minio

        self._client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=False,  # local container, no TLS
        )
        self._bucket = settings.bucket_name
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self) -> None:
        try:
            if not self._client.bucket_exists(self._bucket):
                self._client.make_bucket(self._bucket)
                log.info("Created bucket: %s", self._bucket)
        except Exception as e:
            raise ConnectionError(
                f"Could not reach MinIO at {settings.minio_endpoint}. "
                f"Is the container running? Try: docker compose up -d minio"
            ) from e

    def upload_bytes(self, object_name: str, data: bytes, content_type: str = "text/csv") -> None:
        self._client.put_object(
            self._bucket, object_name, io.BytesIO(data), length=len(data), content_type=content_type
        )
        log.info("Uploaded %d bytes -> minio://%s/%s", len(data), self._bucket, object_name)

    def upload_file(self, object_name: str, local_path: str) -> None:
        self._client.fput_object(self._bucket, object_name, local_path)
        log.info("Uploaded file %s -> minio://%s/%s", local_path, self._bucket, object_name)

    def download_bytes(self, object_name: str) -> bytes:
        response = self._client.get_object(self._bucket, object_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def list_objects(self, prefix: str = "") -> list[str]:
        objects = self._client.list_objects(self._bucket, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]


class S3Storage:
    """AWS backend -- real S3, credentials from the standard AWS chain
    (~/.aws/credentials, env vars, or an instance role)."""

    def __init__(self) -> None:
        import boto3
        from botocore.exceptions import ClientError

        self._ClientError = ClientError
        self._client = boto3.client("s3", region_name=settings.aws_region)
        self._bucket = settings.bucket_name
        self._verify_bucket_access()

    def _verify_bucket_access(self) -> None:
        # The bucket is created by Terraform, not by this code -- infrastructure
        # is provisioned declaratively, the application only verifies it can reach it.
        try:
            self._client.head_bucket(Bucket=self._bucket)
        except self._ClientError as e:
            raise ConnectionError(
                f"Cannot access S3 bucket '{self._bucket}' in {settings.aws_region}. "
                f"Check BUCKET_NAME, your AWS credentials, and that terraform apply succeeded."
            ) from e

    def upload_bytes(self, object_name: str, data: bytes, content_type: str = "text/csv") -> None:
        self._client.put_object(
            Bucket=self._bucket, Key=object_name, Body=data, ContentType=content_type
        )
        log.info("Uploaded %d bytes -> s3://%s/%s", len(data), self._bucket, object_name)

    def upload_file(self, object_name: str, local_path: str) -> None:
        self._client.upload_file(local_path, self._bucket, object_name)
        log.info("Uploaded file %s -> s3://%s/%s", local_path, self._bucket, object_name)

    def download_bytes(self, object_name: str) -> bytes:
        response = self._client.get_object(Bucket=self._bucket, Key=object_name)
        return response["Body"].read()

    def list_objects(self, prefix: str = "") -> list[str]:
        keys: list[str] = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys


def _build_storage():
    if settings.storage_backend == "s3":
        return S3Storage()
    if settings.storage_backend == "minio":
        return MinIOStorage()
    raise ValueError(
        f"Unknown STORAGE_BACKEND '{settings.storage_backend}'. Use 'minio' or 's3'."
    )


storage = _build_storage()


if __name__ == "__main__":
    print(f"Backend: {settings.storage_backend.upper()} | bucket: {settings.bucket_name}")
    found = storage.list_objects()
    if not found:
        print("  (empty -- nothing uploaded yet)")
    for name in found:
        print("  -", name)