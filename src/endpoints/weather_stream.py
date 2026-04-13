"""
NOAA api.weather.gov client and polling-based stream simulator (no API key).

NOAA requires a descriptive User-Agent identifying your application.
Docs: https://www.weather.gov/documentation/services-web-api
"""

from __future__ import annotations

import asyncio
import sys
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

import os

import httpx

BASE_URL = "https://api.weather.gov"

_contact = os.environ.get("HYDRA_CONTACT_EMAIL", "set-HYDRA_CONTACT_EMAIL@example.com")
DEFAULT_USER_AGENT = f"(hydra-ingest, {_contact})"


class NOAAWeatherClient:
    """Async NOAA Weather API client with optional long-poll style observation streaming."""

    BASE_URL = BASE_URL
    DEMO_STATIONS = ["KNYC", "KLAX", "KORD", "KATL", "KDEN"]

    def __init__(self, user_agent: str = DEFAULT_USER_AGENT) -> None:
        self._headers = {
            "User-Agent": user_agent,
            "Accept": "application/geo+json",
        }
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> NOAAWeatherClient:
        self._client = httpx.AsyncClient(
            base_url=self.BASE_URL,
            headers=self._headers,
            timeout=60.0,
        )
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.BASE_URL,
                headers=self._headers,
                timeout=60.0,
            )
        return self._client

    async def get_station_observation(self, station_id: str) -> dict[str, Any]:
        """GET /stations/{station_id}/observations/latest — latest observation for a station."""
        client = self._get_client()
        r = await client.get(f"/stations/{station_id}/observations/latest")
        r.raise_for_status()
        return r.json()

    async def get_active_alerts(self, area: str = "NY") -> list[dict[str, Any]]:
        """GET /alerts/active?area={area} — active alerts for a two-letter area code."""
        client = self._get_client()
        r = await client.get("/alerts/active", params={"area": area})
        r.raise_for_status()
        body = r.json()
        features = body.get("features")
        if isinstance(features, list):
            return [x for x in features if isinstance(x, dict)]
        return []

    @staticmethod
    def _normalize_observation(station_id: str, raw: dict[str, Any]) -> dict[str, Any]:
        """Map NOAA GeoJSON observation payload to a flat hydra-ingest friendly record."""
        props = raw.get("properties") if isinstance(raw.get("properties"), dict) else {}

        def _num(obj: Any) -> float | None:
            if isinstance(obj, dict) and obj.get("value") is not None:
                try:
                    return float(obj["value"])
                except (TypeError, ValueError):
                    return None
            return None

        temp_c = _num(props.get("temperature"))
        humidity = _num(props.get("relativeHumidity"))
        # NOAA typically exposes wind speed in m/s (SI); convert to km/h for downstream consistency.
        wind_m_s = _num(props.get("windSpeed"))
        wind_kmh = round(wind_m_s * 3.6, 2) if wind_m_s is not None else None

        ts = props.get("timestamp")
        if isinstance(ts, str):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc).isoformat()

        description = props.get("textDescription") or props.get("shortForecast") or ""

        return {
            "station_id": station_id,
            "timestamp": timestamp,
            "temperature_c": temp_c,
            "humidity": humidity,
            "wind_speed_kmh": wind_kmh,
            "description": str(description) if description is not None else "",
            "raw_observation": raw,
        }

    async def stream_observations(
        self,
        stations: list[str] | None = None,
        interval_seconds: int = 30,
        max_events: int = 100,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Poll stations in rotation and yield normalized observation records.

        This simulates a streaming source using HTTP polling (suitable for demos and tests).
        """
        ids = list(stations) if stations else list(self.DEMO_STATIONS)
        if not ids:
            return
        idx = 0
        count = 0
        while count < max_events:
            station_id = ids[idx % len(ids)]
            idx += 1
            try:
                raw = await self.get_station_observation(station_id)
                yield self._normalize_observation(station_id, raw)
            except httpx.HTTPError:
                # Yield a minimal error-shaped record so the stream remains observable.
                yield {
                    "station_id": station_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "temperature_c": None,
                    "humidity": None,
                    "wind_speed_kmh": None,
                    "description": "fetch_error",
                    "raw_observation": {},
                }
            count += 1
            if count < max_events:
                await asyncio.sleep(max(1, interval_seconds))


async def _demo_main() -> None:
    """Fetch one observation per demo station and print alert count for NY."""
    async with NOAAWeatherClient() as client:
        for sid in NOAAWeatherClient.DEMO_STATIONS[:2]:
            obs = await client.get_station_observation(sid)
            props = obs.get("properties") or {}
            print(f"{sid}: {props.get('textDescription', 'n/a')}")
        alerts = await client.get_active_alerts("NY")
        print(f"active alerts (NY): {len(alerts)}")
        n = 0
        async for rec in client.stream_observations(
            stations=["KNYC"],
            interval_seconds=2,
            max_events=3,
        ):
            print(f"stream event {n}: {rec['station_id']} T={rec['temperature_c']}C")
            n += 1


def main() -> None:
    asyncio.run(_demo_main())


if __name__ == "__main__":
    main()
    sys.exit(0)
