import math

from litestar.exceptions import ValidationException

from sensor_api.data.models import MetricType, SensorIngestPayload


def validate_ingest_payload(sensor_id: str, payload: SensorIngestPayload) -> None:
    """Validate ingest request payload and path params.

    Raises ValidationException on any validation error.
    """
    if not sensor_id or not sensor_id.strip():
        raise ValidationException("sensor_id cannot be empty")

    if not isinstance(payload.metrics, dict) or not payload.metrics:
        raise ValidationException("At least one metric is required")

    if not isinstance(payload.sensor_type, str) or not payload.sensor_type.strip():
        raise ValidationException("'sensor_type' must be a non-empty string")

    if not isinstance(payload.location, str) or not payload.location.strip():
        raise ValidationException("'location' must be a non-empty string")

    allowed_metrics = {m.value for m in MetricType.__members__.values()}
    for name, value in payload.metrics.items():
        if not isinstance(name, str) or not name.strip():
            raise ValidationException("Metric names must be non-empty strings")
        if name not in allowed_metrics:
            raise ValidationException(f"Unknown metric '{name}'. See /api/v1/metrics for allowed values")
        if not isinstance(value, (int, float)) or not math.isfinite(float(value)):
            raise ValidationException(f"Metric '{name}' must be a finite number")
