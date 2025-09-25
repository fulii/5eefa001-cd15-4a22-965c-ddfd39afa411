from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Any

import msgspec
from sqlalchemy import Column, Float, Index, Text, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Statistic(str, Enum):
    MIN = "min"
    MAX = "max"
    AVG = "average"
    SUM = "sum"

    def to_sqlalchemy_func(self):
        mapping = {
            Statistic.MIN: func.min,
            Statistic.MAX: func.max,
            Statistic.SUM: func.sum,
            Statistic.AVG: func.avg,
        }
        return mapping.get(self, func.avg)


class MetricType(str, Enum):
    TEMPERATURE = "temperature"
    HUMIDITY = "humidity"
    PRESSURE = "pressure"


class SensorMetric(Base):
    """TimescaleDB hypertable for sensor metrics"""

    __tablename__ = "sensor_metrics"

    timestamp = Column(TIMESTAMP(timezone=True), primary_key=True, nullable=False)
    sensor_id = Column(Text, primary_key=True, nullable=False)
    location = Column("location", Text, nullable=False)
    sensor_type = Column(Text, nullable=False)
    temperature = Column("temperature", Float(precision=53), nullable=True)
    humidity = Column("humidity", Float(precision=53), nullable=True)
    pressure = Column("pressure", Float(precision=53), nullable=True)

    __table_args__ = (Index("idx_sensor_metrics_sensor_time", "sensor_id", "timestamp"),)

    @classmethod
    def from_sensor_data(cls, data: "SensorData") -> "SensorMetric":
        """Create TimescaleDB record from SensorData"""
        metric_values = {
            "temperature": data.metrics.get("temperature"),
            "humidity": data.metrics.get("humidity"),
            "pressure": data.metrics.get("pressure"),
        }

        return cls(
            timestamp=data.timestamp,
            sensor_id=data.sensor_id,
            location=data.location,
            sensor_type=data.sensor_type,
            **metric_values,
        )


class SensorData(msgspec.Struct):
    """API input model for receiving sensor data with multiple metrics"""

    sensor_id: str
    metrics: dict[str, float]
    timestamp: datetime | None = None
    location: str = ""
    sensor_type: str = ""

    def __post_init__(self):
        if self.timestamp is None:
            object.__setattr__(self, "timestamp", datetime.now(UTC))

    def to_db_record(self) -> SensorMetric:
        """Convert to single TimescaleDB record"""
        return SensorMetric.from_sensor_data(self)


class SensorIngestPayload(msgspec.Struct):
    """Body payload for ingestion without sensor_id (taken from URL)."""

    location: str
    sensor_type: str
    metrics: dict[str, float]
    timestamp: datetime | None = None


class SensorQuery(msgspec.Struct):
    """Query parameters for sensor data"""

    sensor_ids: list[str] | None = None
    metrics: list[str] = msgspec.field(default_factory=list)
    statistic: Statistic = Statistic.AVG
    start_date: datetime | None = None
    end_date: datetime | None = None

    def get_date_filter(self) -> tuple[datetime, datetime]:
        end = self.end_date or datetime.now(UTC)
        start = self.start_date or (end - timedelta(days=1))
        return start, end


class MetricResult(msgspec.Struct):
    """Result for a single metric with calculated statistic"""

    metric: str
    value: float
    statistic: str


class SensorQueryResult(msgspec.Struct):
    """Results for a single sensor"""

    sensor_id: str
    metrics: list[MetricResult]
    timestamp: datetime


class SensorQueryResponse(msgspec.Struct):
    """Complete query response"""

    results: list[SensorQueryResult]
    query_info: dict[str, Any]
    message: str = "Success"
