from datetime import UTC, datetime, timedelta

import aiohttp
import pytest
import pytest_asyncio
from dotenv import load_dotenv


@pytest.fixture(scope="session", autouse=True)
def load_env():
    load_dotenv()


@pytest.fixture(scope="session")
def api_url() -> str:
    return "http://localhost:8000"


@pytest_asyncio.fixture
async def http_session():
    """HTTP session for making requests."""
    timeout = aiohttp.ClientTimeout(total=30)
    session = aiohttp.ClientSession(timeout=timeout)
    yield session
    await session.close()


@pytest_asyncio.fixture(scope="session")
async def test_data_setup(api_url):
    timeout = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        now = datetime.now(UTC)
        base_time = now - timedelta(days=10)

        test_data = [
            {
                "sensor_id": "sensor_001",
                "timestamp": base_time + timedelta(days=1, hours=0),
                "metrics": {"temperature": 100.0, "humidity": 10.0},
                "location": "location_1",
                "sensor_type": "sensor_type_1",
            },
            {
                "sensor_id": "sensor_001",
                "timestamp": base_time + timedelta(days=1, hours=12),
                "metrics": {"temperature": 200.0, "humidity": 20.0},
                "location": "location_1",
                "sensor_type": "sensor_type_1",
            },
            {
                "sensor_id": "sensor_001",
                "timestamp": base_time + timedelta(days=2, hours=0),
                "metrics": {"temperature": 300.0, "humidity": 30.0},
                "location": "location_1",
                "sensor_type": "sensor_type_1",
            },
            {
                "sensor_id": "sensor_001",
                "timestamp": base_time + timedelta(days=2, hours=12),
                "metrics": {"temperature": 400.0, "humidity": 40.0},
                "location": "location_1",
                "sensor_type": "sensor_type_1",
            },
            {
                "sensor_id": "sensor_002",
                "timestamp": base_time + timedelta(days=1, hours=0),
                "metrics": {"temperature": 500.0, "humidity": 50.0, "pressure": 1000.0},
                "location": "location_2",
                "sensor_type": "sensor_type_2",
            },
            {
                "sensor_id": "sensor_002",
                "timestamp": base_time + timedelta(days=1, hours=12),
                "metrics": {"temperature": 600.0, "humidity": 60.0, "pressure": 2000.0},
                "location": "location_2",
                "sensor_type": "sensor_type_2",
            },
            {
                "sensor_id": "sensor_001",
                "timestamp": now - timedelta(hours=2),
                "metrics": {"temperature": 1000.0, "humidity": 100.0},
                "location": "location_1",
                "sensor_type": "sensor_type_1",
            },
            {
                "sensor_id": "sensor_002",
                "timestamp": now - timedelta(hours=4),
                "metrics": {
                    "temperature": 2000.0,
                    "humidity": 200.0,
                    "pressure": 3000.0,
                },
                "location": "location_2",
                "sensor_type": "sensor_type_2",
            },
        ]

        for data in test_data:
            sensor_id = data.pop("sensor_id")
            if isinstance(data["timestamp"], datetime):
                data["timestamp"] = data["timestamp"].isoformat()
            async with session.post(f"{api_url}/api/v1/sensors/{sensor_id}/data", json=data) as response:
                assert response.status == 201, f"Failed to load test data: {await response.text()}"


@pytest_asyncio.fixture(scope="session", autouse=True)
async def cleanup_test_data():
    yield

    from sqlalchemy import delete

    from sensor_api.data.models import SensorMetric
    from sensor_api.storage.timescaledb import TimescaleDBHandler

    storage = TimescaleDBHandler()
    async with storage.get_session() as session:
        test_sensors = ["sensor_001", "sensor_002", "test_ingest"]
        stmt = delete(SensorMetric).where(SensorMetric.sensor_id.in_(test_sensors))
        await session.execute(stmt)
        await session.commit()

    await storage.close()


class TestSensorAPICore:
    """Test core sensor API requirements with exact data and assertions."""

    @pytest.mark.asyncio
    async def test_api_receives_new_metric_values(self, http_session: aiohttp.ClientSession, api_url: str):
        """Test: The application can receive new metric values via API call."""
        payload = {
            "location": "location_3",
            "sensor_type": "test_sensor",
            "metrics": {"temperature": 100.0, "humidity": 200.0},
            "timestamp": datetime.now(UTC).isoformat(),
        }

        async with http_session.post(f"{api_url}/api/v1/sensors/test_ingest/data", json=payload) as response:
            assert response.status == 201
            data = await response.json()
            assert data["message"] == "Sensor data stored successfully"
            assert data["sensor_id"] == "test_ingest"

    @pytest.mark.asyncio
    async def test_single_sensor_query(self, http_session: aiohttp.ClientSession, api_url: str, test_data_setup):
        """Test: Query single sensor with known data."""
        url = f"{api_url}/api/v1/sensors/sensor_001/data?metrics=temperature,humidity&stat=average&days=10"
        async with http_session.get(url) as response:
            assert response.status == 200
            data = await response.json()

            assert len(data["results"]) == 1
            result = data["results"][0]
            assert result["sensor_id"] == "sensor_001"

            metrics_by_name = {m["metric"]: m for m in result["metrics"]}
            assert metrics_by_name["temperature"]["value"] == 400.0  # (1000+1000)/5
            assert metrics_by_name["temperature"]["statistic"] == "average"
            assert metrics_by_name["humidity"]["value"] == 40.0  # (10+20+30+40+100)/5
            assert metrics_by_name["humidity"]["statistic"] == "average"

    @pytest.mark.asyncio
    async def test_multiple_sensors_query(self, http_session: aiohttp.ClientSession, api_url: str, test_data_setup):
        """Test: Query multiple sensors."""
        url = f"{api_url}/api/v1/sensors/data?sensors=sensor_001,sensor_002&metrics=temperature&stat=average&days=10"
        async with http_session.get(url) as response:
            assert response.status == 200
            data = await response.json()

            results_by_sensor = {r["sensor_id"]: r for r in data["results"]}

            assert "sensor_001" in results_by_sensor
            assert "sensor_002" in results_by_sensor

            for sensor_id in ["sensor_001", "sensor_002"]:
                result = results_by_sensor[sensor_id]
                assert len(result["metrics"]) == 1
                temp_metric = result["metrics"][0]
                assert temp_metric["metric"] == "temperature"
                assert isinstance(temp_metric["value"], (int, float))

    @pytest.mark.asyncio
    async def test_statistics_min(self, http_session: aiohttp.ClientSession, api_url: str, test_data_setup):
        """Test: MIN statistic with exact assertions."""
        url = f"{api_url}/api/v1/sensors/sensor_001/data?metrics=temperature,humidity&stat=min&days=10"
        async with http_session.get(url) as response:
            assert response.status == 200
            data = await response.json()

            result = data["results"][0]
            metrics_by_name = {m["metric"]: m for m in result["metrics"]}

            assert metrics_by_name["temperature"]["value"] == 100.0  # min(100,200,300,400,1000)
            assert metrics_by_name["temperature"]["statistic"] == "min"
            assert metrics_by_name["humidity"]["value"] == 10.0  # min(10,20,30,40,100)
            assert metrics_by_name["humidity"]["statistic"] == "min"

    @pytest.mark.asyncio
    async def test_statistics_max(self, http_session: aiohttp.ClientSession, api_url: str, test_data_setup):
        """Test: MAX statistic with exact assertions."""
        url = f"{api_url}/api/v1/sensors/sensor_001/data?metrics=temperature,humidity&stat=max&days=10"
        async with http_session.get(url) as response:
            assert response.status == 200
            data = await response.json()

            result = data["results"][0]
            metrics_by_name = {m["metric"]: m for m in result["metrics"]}

            assert metrics_by_name["temperature"]["value"] == 1000.0  # max(100,200,300,400,1000)
            assert metrics_by_name["temperature"]["statistic"] == "max"
            assert metrics_by_name["humidity"]["value"] == 100.0  # max(10,20,30,40,100)
            assert metrics_by_name["humidity"]["statistic"] == "max"

    @pytest.mark.asyncio
    async def test_statistics_sum(self, http_session: aiohttp.ClientSession, api_url: str, test_data_setup):
        """Test: SUM statistic with exact assertions."""
        url = f"{api_url}/api/v1/sensors/sensor_001/data?metrics=temperature,humidity&stat=sum&days=10"
        async with http_session.get(url) as response:
            assert response.status == 200
            data = await response.json()

            result = data["results"][0]
            metrics_by_name = {m["metric"]: m for m in result["metrics"]}

            assert metrics_by_name["temperature"]["value"] == 2000.0  # sum(100+200+300+400+1000)
            assert metrics_by_name["temperature"]["statistic"] == "sum"
            assert metrics_by_name["humidity"]["value"] == 200.0  # sum(10+20+30+40+100)
            assert metrics_by_name["humidity"]["statistic"] == "sum"

    @pytest.mark.asyncio
    async def test_statistics_average(self, http_session: aiohttp.ClientSession, api_url: str, test_data_setup):
        """Test: AVERAGE statistic with exact assertions."""
        url = f"{api_url}/api/v1/sensors/sensor_001/data?metrics=temperature,humidity&stat=average&days=10"
        async with http_session.get(url) as response:
            assert response.status == 200
            data = await response.json()

            result = data["results"][0]
            metrics_by_name = {m["metric"]: m for m in result["metrics"]}

            assert metrics_by_name["temperature"]["value"] == 400.0  # average(100+200+300+400+1000)/5
            assert metrics_by_name["temperature"]["statistic"] == "average"
            assert metrics_by_name["humidity"]["value"] == 40.0  # average(10+20+30+40+100)/5
            assert metrics_by_name["humidity"]["statistic"] == "average"

    @pytest.mark.asyncio
    async def test_date_range_days_parameter(self, http_session: aiohttp.ClientSession, api_url: str, test_data_setup):
        """Test: Date range specified by days parameter."""
        url = f"{api_url}/api/v1/sensors/sensor_001/data?metrics=temperature&stat=average&days=2"
        async with http_session.get(url) as response:
            assert response.status == 200
            data = await response.json()

            assert len(data["results"]) == 1
            result = data["results"][0]
            assert result["sensor_id"] == "sensor_001"

            assert len(result["metrics"]) == 1
            temp_metric = result["metrics"][0]
            assert temp_metric["metric"] == "temperature"
            assert temp_metric["value"] == 1000.0

    @pytest.mark.asyncio
    async def test_default_latest_data_query(self, http_session: aiohttp.ClientSession, api_url: str, test_data_setup):
        """Test: Default behavior queries latest data (1 day) when no date range specified."""
        url = f"{api_url}/api/v1/sensors/sensor_001/data?metrics=temperature"
        async with http_session.get(url) as response:
            assert response.status == 200
            data = await response.json()

            assert len(data["results"]) == 1
            result = data["results"][0]
            temp_metric = result["metrics"][0]
            assert temp_metric["value"] == 1000.0

    @pytest.mark.asyncio
    async def test_example_query_average_temp_humidity_last_week(
        self, http_session: aiohttp.ClientSession, api_url: str, test_data_setup
    ):
        """Test: Example query - average temperature and humidity for sensor 1 in last week."""
        url = f"{api_url}/api/v1/sensors/sensor_001/data?metrics=temperature,humidity&stat=average&days=7"
        async with http_session.get(url) as response:
            assert response.status == 200
            data = await response.json()

            assert len(data["results"]) == 1
            result = data["results"][0]
            assert result["sensor_id"] == "sensor_001"

            metrics_by_name = {m["metric"]: m for m in result["metrics"]}

            assert "temperature" in metrics_by_name
            assert "humidity" in metrics_by_name

            assert metrics_by_name["temperature"]["statistic"] == "average"
            assert metrics_by_name["humidity"]["statistic"] == "average"

            assert metrics_by_name["temperature"]["value"] == 1000.0
            assert metrics_by_name["humidity"]["value"] == 100.0

    @pytest.mark.asyncio
    async def test_all_sensors_query(self, http_session: aiohttp.ClientSession, api_url: str, test_data_setup):
        """Test: Query all sensors (no sensors parameter specified)."""
        url = f"{api_url}/api/v1/sensors/data?metrics=temperature&stat=average&days=10"
        async with http_session.get(url) as response:
            assert response.status == 200
            data = await response.json()

            sensor_ids = {r["sensor_id"] for r in data["results"]}
            assert "sensor_001" in sensor_ids
            assert "sensor_002" in sensor_ids

    @pytest.mark.asyncio
    async def test_all_metrics_query(self, http_session: aiohttp.ClientSession, api_url: str, test_data_setup):
        """Test: Query all metrics (no metrics parameter specified)."""
        url = f"{api_url}/api/v1/sensors/sensor_001/data?stat=average&days=10"
        async with http_session.get(url) as response:
            assert response.status == 200
            data = await response.json()

            result = data["results"][0]
            metrics_by_name = {m["metric"]: m for m in result["metrics"]}

            assert "temperature" in metrics_by_name
            assert "humidity" in metrics_by_name

    @pytest.mark.asyncio
    async def test_default_average_statistic(self, http_session: aiohttp.ClientSession, api_url: str, test_data_setup):
        """Test: Default statistic is average when not specified."""
        url = f"{api_url}/api/v1/sensors/sensor_001/data?metrics=temperature&days=10"
        async with http_session.get(url) as response:
            assert response.status == 200
            data = await response.json()

            result = data["results"][0]
            temp_metric = result["metrics"][0]
            assert temp_metric["statistic"] == "average"
            assert temp_metric["value"] == 400.0

    @pytest.mark.asyncio
    async def test_complete_defaults_query(self, http_session: aiohttp.ClientSession, api_url: str, test_data_setup):
        """Test: All defaults - average for all sensors, all metrics, last 1 day."""
        url = f"{api_url}/api/v1/sensors/data"
        async with http_session.get(url) as response:
            assert response.status == 200
            data = await response.json()

            sensor_ids = {r["sensor_id"] for r in data["results"]}
            assert "sensor_001" in sensor_ids
            assert "sensor_002" in sensor_ids

            sensor_001_result = next(r for r in data["results"] if r["sensor_id"] == "sensor_001")
            metrics_by_name = {m["metric"]: m for m in sensor_001_result["metrics"]}

            for _metric_name, metric_data in metrics_by_name.items():
                assert metric_data["statistic"] == "average"
