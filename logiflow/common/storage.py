"""
common/storage.py

Single object-storage client for LogiFlow. Every script that reads or
writes to MinIO/S3 imports `storage` from here instead of constructing
its own Minio() client.

This is the ONE file that changes when LogiFlow migrates from local
MinIO to real AWS S3 -- nothing else in the codebase touches the
storage SDK directly.
"""

import io
import logging

from minio import Minio

from common.config import settings

log = logging.getLogger(__name__)


class StorageClient:
    def __init__(self) -> None:
        self._client = Minio(
            settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=False,  # local MinIO only -- flip to True when pointed at real S3
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
        """Write raw bytes to <bucket>/<object_name>."""
        self._client.put_object(
            self._bucket,
            object_name,
            io.BytesIO(data),
            length=len(data),
            content_type=content_type,
        )
        log.info("Uploaded %d bytes -> %s/%s", len(data), self._bucket, object_name)

    def upload_file(self, object_name: str, local_path: str) -> None:
        """Upload a file already sitting on disk to <bucket>/<object_name>."""
        self._client.fput_object(self._bucket, object_name, local_path)
        log.info("Uploaded file %s -> %s/%s", local_path, self._bucket, object_name)

    def download_bytes(self, object_name: str) -> bytes:
        """Read <bucket>/<object_name> back as raw bytes."""
        response = self._client.get_object(self._bucket, object_name)
        try:
            return response.read()
        finally:
            response.close()
            response.release_conn()

    def list_objects(self, prefix: str = "") -> list[str]:
        """List object names under a prefix, e.g. 'raw/'."""
        objects = self._client.list_objects(self._bucket, prefix=prefix, recursive=True)
        return [obj.object_name for obj in objects]


storage = StorageClient()


if __name__ == "__main__":
    # Manual sanity check: confirm we can reach MinIO and see the bucket.
    print(f"Connected. Bucket '{settings.bucket_name}' objects:")
    found = storage.list_objects()
    if not found:
        print("  (empty -- nothing uploaded yet, that's expected right now)")
    for name in found:
        print("  -", name)