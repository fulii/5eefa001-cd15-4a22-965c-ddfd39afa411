from typing import Annotated

import msgspec
from litestar import Router, get, post
from litestar.connection import Request
from litestar.exceptions import HTTPException, ValidationException
from litestar.params import Parameter
from litestar.status_codes import HTTP_201_CREATED

from sensor_api.api.utils import (
    compute_date_range_from_days,
    parse_metrics_param,
    parse_sensors_param,
    parse_stat,
)
from sensor_api.api.validators import validate_ingest_payload
from sensor_api.data.models import (
    MetricType,
    SensorData,
    SensorIngestPayload,
    SensorQuery,
    SensorQueryResponse,
)


@post("/sensors/{sensor_id:str}/data", status_code=HTTP_201_CREATED)
async def ingest_sensor_data(sensor_id: str, request: Request) -> dict[str, str]:
    """Ingest sensor data for specific sensor"""
    raw = await request.body()
    content_type = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    if "json" not in content_type:
        raise ValidationException("Content-Type must be application/json")
    try:
        payload = msgspec.json.decode(raw, type=SensorIngestPayload)
    except Exception as e:
        raise ValidationException(f"Invalid JSON body: {e}") from e

    sensor_data = SensorData(
        sensor_id=sensor_id,
        metrics=payload.metrics,
        timestamp=payload.timestamp,
        location=payload.location,
        sensor_type=payload.sensor_type,
    )
    try:
        validate_ingest_payload(sensor_id, payload)

        storage = request.app.state.storage
        await storage.store_sensor_data(sensor_data)
        return {"message": "Sensor data stored successfully", "sensor_id": sensor_id}
    except Exception as e:
        if isinstance(e, ValidationException):
            raise
        raise HTTPException(status_code=500, detail=f"Failed to store sensor data: {str(e)}") from e


@get("/sensors/{sensor_id:str}/data")
async def get_single_sensor_data(
    sensor_id: str,
    request: Request,
    metrics: Annotated[str | None, Parameter(description="Comma-separated metrics")] = None,
    stat: Annotated[str, Parameter(description="Statistic: average, min, max, sum")] = "average",
    days: Annotated[int | None, Parameter(description="Days back from now")] = None,
) -> SensorQueryResponse:
    """Query data for single sensor"""
    metric_list = parse_metrics_param(metrics)
    statistic = parse_stat(stat)
    start_date, end_date = compute_date_range_from_days(days)

    storage = request.app.state.storage

    query = SensorQuery(
        sensor_ids=[sensor_id],
        metrics=metric_list,
        statistic=statistic,
        start_date=start_date,
        end_date=end_date,
    )

    results = await storage.query_sensor_data(query)

    return SensorQueryResponse(
        results=results,
        query_info={
            "sensors": sensor_id,
            "metrics": ",".join(metric_list) if metric_list else "all",
            "statistic": statistic.value,
            "date_range": f"{start_date.isoformat()} to {end_date.isoformat()}",
        },
        message=f"Retrieved data for sensor {sensor_id}",
    )


@get("/sensors/data")
async def get_multi_sensor_data(
    request: Request,
    sensors: Annotated[str | None, Parameter(description="Comma-separated sensor IDs")] = None,
    metrics: Annotated[str | None, Parameter(description="Comma-separated metrics")] = None,
    stat: Annotated[str, Parameter(description="Statistic: average, min, max, sum")] = "average",
    days: Annotated[int | None, Parameter(description="Days back from now")] = None,
) -> SensorQueryResponse:
    """Query data for multiple sensors"""

    sensor_list = parse_sensors_param(sensors)
    metric_list = parse_metrics_param(metrics)
    statistic = parse_stat(stat)
    start_date, end_date = compute_date_range_from_days(days)

    query = SensorQuery(
        sensor_ids=sensor_list,
        metrics=metric_list,
        statistic=statistic,
        start_date=start_date,
        end_date=end_date,
    )

    storage = request.app.state.storage
    results = await storage.query_sensor_data(query)

    return SensorQueryResponse(
        results=results,
        query_info={
            "sensors": ",".join(sensor_list) if sensor_list else "all",
            "metrics": ",".join(metric_list) if metric_list else "all",
            "statistic": statistic.value,
            "date_range": f"{start_date.isoformat()} to {end_date.isoformat()}",
        },
        message=f"Retrieved data for {len(results)} sensors",
    )


@get("/sensors")
async def list_sensors(request: Request) -> dict[str, list[str]]:
    """List all available sensors"""
    storage = request.app.state.storage

    sensor_ids = await storage.list_sensor_ids()
    return {"sensors": sensor_ids}


@get("/metrics")
async def list_metrics() -> dict[str, list[str]]:
    """List all available metrics"""
    return {"metrics": [m.value for m in MetricType.__members__.values()]}


@get("/health")
async def health_check() -> dict[str, str]:
    return {"status": "healthy", "service": "sensor-api"}


sensor_router = Router(
    path="/api/v1",
    route_handlers=[
        ingest_sensor_data,
        get_single_sensor_data,
        get_multi_sensor_data,
        list_sensors,
        list_metrics,
        health_check,
    ],
)
