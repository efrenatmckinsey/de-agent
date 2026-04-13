"""
Sample public API endpoint clients for hydra-ingest demos and integration tests.

These modules are intentionally self-contained (httpx only) and may be run as scripts.
"""

from src.endpoints.census_batch import CensusBatchClient
from src.endpoints.food_api import USDAFoodClient
from src.endpoints.weather_stream import NOAAWeatherClient

__all__ = [
    "CensusBatchClient",
    "NOAAWeatherClient",
    "USDAFoodClient",
]
