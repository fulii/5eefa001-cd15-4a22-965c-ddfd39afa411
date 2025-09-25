import asyncio
import contextlib
import random
from datetime import UTC, datetime, timedelta

import aiohttp
from invoke.collection import Collection
from invoke.tasks import task

from sensor_api.data.models import MetricType


def generate_weather_data() -> dict:
    """Generate simple numeric metrics."""
    keys = [m.value for m in MetricType.__members__.values()]
    return {key: round(random.uniform(0, 100), 2) for key in keys}


async def send_sensor_data(
    session: aiohttp.ClientSession,
    sensor_id: str,
    timestamp: datetime,
    api_url: str,
    location: str,
    sensor_type: str,
) -> bool:
    """Send sensor data to the API."""
    metrics = generate_weather_data()

    payload = {
        "location": location,
        "sensor_type": sensor_type,
        "metrics": metrics,
        "timestamp": timestamp.isoformat(),
    }

    async with session.post(
        f"{api_url}/api/v1/sensors/{sensor_id}/data", json=payload
    ) as response:
        if response.status >= 400:
            with contextlib.suppress(Exception):
                await response.text()
            return False
        return True


async def send_all_data_concurrently(
    tasks_batch: list, api_url: str, semaphore: asyncio.Semaphore
) -> list[bool]:
    """Send sensor data concurrently with a single session and semaphore control."""
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:

        async def send_with_semaphore(sensor_id, timestamp, location, sensor_type):
            async with semaphore:
                return await send_sensor_data(
                    session,
                    sensor_id,
                    timestamp,
                    api_url,
                    location,
                    sensor_type,
                )

        results = await asyncio.gather(
            *[
                send_with_semaphore(sensor_id, timestamp, location, sensor_type)
                for sensor_id, timestamp, location, sensor_type in tasks_batch
            ],
            return_exceptions=True,
        )

        return [result if isinstance(result, bool) else False for result in results]


async def _run_generation(sensors: int, years: int, api_url: str, max_workers: int):
    sensors_count = sensors
    years_count = years
    max_workers_count = max_workers

    sensor_ids = [f"sensor_{i:03d}" for i in range(1, sensors_count + 1)]
    profiles: dict[str, tuple[str, str]] = {}
    for idx, sid in enumerate(sensor_ids, start=1):
        location = f"location_{(idx % 5) + 1}"
        sensor_type = f"sensor_type_{(idx % 5) + 1}"
        profiles[sid] = (location, sensor_type)

    end_time = datetime.now(UTC)
    start_time = end_time - timedelta(days=365 * years_count)

    tasks = []
    current_time = start_time
    while current_time <= end_time:
        for sensor_id in sensor_ids:
            location, stype = profiles[sensor_id]
            tasks.append((sensor_id, current_time, location, stype))
        current_time += timedelta(hours=1)

    semaphore = asyncio.Semaphore(max_workers_count)
    results = await send_all_data_concurrently(tasks, api_url, semaphore)

    sum(results)


@task
def generate(ctx, sensors=5, years=1, api_url="http://localhost:8000", max_workers=200):
    """Generate and send synthetic sensor data to the API."""
    asyncio.run(_run_generation(int(sensors), int(years), api_url, int(max_workers)))


data_ns = Collection("data", generate=generate)
