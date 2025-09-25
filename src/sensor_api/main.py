from contextlib import asynccontextmanager

from litestar import Litestar
from litestar.openapi import OpenAPIConfig

from sensor_api.api.sensors import sensor_router
from sensor_api.storage.timescaledb import TimescaleDBHandler

storage: TimescaleDBHandler | None = None


@asynccontextmanager
async def lifespan(app: Litestar):
    """Application lifespan"""
    global storage

    storage = TimescaleDBHandler()
    app.state.storage = storage

    yield

    if storage:
        await storage.close()


def create_app() -> Litestar:
    return Litestar(
        route_handlers=[sensor_router],
        lifespan=[lifespan],
        openapi_config=OpenAPIConfig(
            title="Sensor Data API",
            description="A REST API for sensor data ingestion and querying with statistics",
            version="0.1.0",
        ),
    )


app = create_app()
