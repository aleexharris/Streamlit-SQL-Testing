import os
from dotenv import load_dotenv

load_dotenv()

LIVE = os.getenv("LIVE", "false").lower() == "true"
QUERY_CACHE_LEN = 15
QUERY_PREVIEW_LEN = 100
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_HOST = "minio"
MINIO_PORT = 9000
