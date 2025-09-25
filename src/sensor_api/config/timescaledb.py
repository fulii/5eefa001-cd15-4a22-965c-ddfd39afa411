import os

DATABASE_CREDENTIALS = os.getenv("DATABASE_CREDENTIALS")
if not DATABASE_CREDENTIALS:
    raise ValueError("DATABASE_CREDENTIALS environment variable is required")

DATABASE_URL = f"postgresql+asyncpg://{DATABASE_CREDENTIALS}"
DATABASE_URL_SYNC = f"postgresql+psycopg2://{DATABASE_CREDENTIALS}"
