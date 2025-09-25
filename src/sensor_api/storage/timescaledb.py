from contextlib import asynccontextmanager

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from sensor_api.config import DATABASE_URL
from sensor_api.data.models import (
    MetricResult,
    MetricType,
    SensorData,
    SensorMetric,
    SensorQuery,
    SensorQueryResult,
)


class TimescaleDBHandler:
    def __init__(self):
        self.async_engine = create_async_engine(DATABASE_URL)
        self.async_session = async_sessionmaker(
            self.async_engine, class_=AsyncSession, expire_on_commit=False
        )

    @asynccontextmanager
    async def get_session(self):
        """Get async database session"""
        session = self.async_session()
        try:
            yield session
        finally:
            await session.close()

    async def store_sensor_data(self, data: SensorData) -> None:
        """Store sensor data in TimescaleDB"""
        async with self.get_session() as session:
            db_record = data.to_db_record()
            session.add(db_record)
            await session.commit()

    async def query_sensor_data(self, query: SensorQuery) -> list[SensorQueryResult]:
        """Query sensor data from TimescaleDB with statistics"""
        async with self.get_session() as session:
            metric_columns = []
            requested_metrics = query.metrics or [
                m.value for m in MetricType.__members__.values()
            ]

            agg_func = query.statistic.to_sqlalchemy_func()

            for metric_name in requested_metrics:
                if hasattr(SensorMetric, metric_name):
                    column = getattr(SensorMetric, metric_name)
                    metric_columns.append(
                        agg_func(column).label(f"{metric_name}_value")
                    )

            stmt = select(SensorMetric.sensor_id, *metric_columns).group_by(
                SensorMetric.sensor_id
            )

            if query.sensor_ids:
                stmt = stmt.where(SensorMetric.sensor_id.in_(query.sensor_ids))

            start_date, end_date = query.get_date_filter()
            stmt = stmt.where(
                SensorMetric.timestamp >= start_date, SensorMetric.timestamp <= end_date
            )

            result = await session.execute(stmt)
            rows = result.fetchall()

            sensor_results = []
            for row in rows:
                metrics = []
                for metric_name in requested_metrics:
                    if hasattr(SensorMetric, metric_name):
                        value = getattr(row, f"{metric_name}_value", None)
                        if value is not None:
                            metrics.append(
                                MetricResult(
                                    metric=metric_name,
                                    value=float(value),
                                    statistic=query.statistic.value,
                                )
                            )

                if metrics:
                    sensor_results.append(
                        SensorQueryResult(
                            sensor_id=row.sensor_id, metrics=metrics, timestamp=end_date
                        )
                    )

            return sensor_results

    async def close(self):
        """Close database connections"""
        await self.async_engine.dispose()

    async def list_sensor_ids(self) -> list[str]:
        """Return distinct sensor IDs seen"""
        async with self.get_session() as session:
            stmt = select(SensorMetric.sensor_id).distinct()
            result = await session.execute(stmt)
            rows = result.scalars().all()
            return sorted(set(rows))
