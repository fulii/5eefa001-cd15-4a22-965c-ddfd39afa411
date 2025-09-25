from dotenv import load_dotenv

# ruff: noqa: E402
load_dotenv()

from sensor_api.config.timescaledb import DATABASE_URL, DATABASE_URL_SYNC

__all__ = [
    "DATABASE_URL",
    "DATABASE_URL_SYNC",
]
