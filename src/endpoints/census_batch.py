"""
US Census Bureau public API client — batch-friendly state-level extracts.

API key is optional for most endpoints; set CENSUS_API_KEY for higher limits.
Docs: https://www.census.gov/data/developers/data-sets.html
"""

from __future__ import annotations

import asyncio
import os
import sys
from typing import Any

import httpx

BASE_URL = "https://api.census.gov/data"


class CensusBatchClient:
    """Async client for decennial, SAPE poverty time series, and DHC housing extracts."""

    BASE_URL = BASE_URL

    def __init__(self, api_key: str | None = None) -> None:
        self._api_key = api_key if api_key is not None else os.environ.get("CENSUS_API_KEY")
        self._client: httpx.AsyncClient | None = None

    def _params(self, extra: dict[str, str | int] | None = None) -> dict[str, str]:
        p: dict[str, str] = {}
        if self._api_key:
            p["key"] = str(self._api_key)
        if extra:
            for k, v in extra.items():
                p[str(k)] = str(v)
        return p

    async def __aenter__(self) -> CensusBatchClient:
        self._client = httpx.AsyncClient(base_url=self.BASE_URL, timeout=120.0)
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self.BASE_URL, timeout=120.0)
        return self._client

    @staticmethod
    def _rows_to_dicts(census_json: list[list[str]]) -> list[dict[str, str]]:
        """Census returns [header_row, data_rows...]."""
        if not census_json or len(census_json) < 2:
            return []
        headers = census_json[0]
        out: list[dict[str, str]] = []
        for row in census_json[1:]:
            if len(row) != len(headers):
                continue
            out.append(dict(zip(headers, row, strict=False)))
        return out

    async def get_population_data(self, year: int = 2020, dataset: str = "dec/pl") -> list[dict[str, Any]]:
        """
        State-level population from decennial PL (P1_001N = total population).
        """
        client = self._get_client()
        path = f"/{year}/{dataset}"
        r = await client.get(path, params=self._params({"get": "NAME,P1_001N", "for": "state:*"}))
        r.raise_for_status()
        rows = r.json()
        if not isinstance(rows, list):
            return []
        raw = self._rows_to_dicts(rows)
        result: list[dict[str, Any]] = []
        for d in raw:
            result.append(
                {
                    "state_name": d.get("NAME", ""),
                    "population": int(d["P1_001N"]) if d.get("P1_001N", "").isdigit() else None,
                    "state_fips": d.get("state", ""),
                }
            )
        return result

    async def get_poverty_estimates(self, year: int = 2022) -> list[dict[str, Any]]:
        """
        SAPE poverty time series: poverty rate and median household income by state.
        """
        client = self._get_client()
        path = "/timeseries/poverty/saipe"
        r = await client.get(
            path,
            params=self._params(
                {
                    "get": "NAME,SAEPOVRTALL_PT,SAEMHI_PT",
                    "for": "state:*",
                    "YEAR": year,
                }
            ),
        )
        r.raise_for_status()
        rows = r.json()
        if not isinstance(rows, list):
            return []
        raw = self._rows_to_dicts(rows)
        out: list[dict[str, Any]] = []
        for d in raw:
            pr = d.get("SAEPOVRTALL_PT")
            mi = d.get("SAEMHI_PT")
            out.append(
                {
                    "state_name": d.get("NAME", ""),
                    "poverty_rate": float(pr) if pr not in (None, "", "-") else None,
                    "median_income": float(mi) if mi not in (None, "", "-") else None,
                    "year": year,
                }
            )
        return out

    async def get_housing_data(self, year: int = 2020) -> list[dict[str, Any]]:
        """
        Housing unit counts by state (DHC table H1_001N).
        """
        client = self._get_client()
        path = f"/{year}/dec/dhc"
        r = await client.get(path, params=self._params({"get": "NAME,H1_001N", "for": "state:*"}))
        r.raise_for_status()
        rows = r.json()
        if not isinstance(rows, list):
            return []
        raw = self._rows_to_dicts(rows)
        result: list[dict[str, Any]] = []
        for d in raw:
            result.append(
                {
                    "state_name": d.get("NAME", ""),
                    "housing_units": int(d["H1_001N"]) if d.get("H1_001N", "").isdigit() else None,
                    "state_fips": d.get("state", ""),
                }
            )
        return result

    async def demo_batch_ingest(self) -> dict[str, Any]:
        """Fetch population, poverty, and housing datasets for a single batch snapshot."""
        population = await self.get_population_data()
        poverty = await self.get_poverty_estimates()
        housing = await self.get_housing_data()
        return {"population": population, "poverty": poverty, "housing": housing}


async def _demo_main() -> None:
    async with CensusBatchClient() as client:
        bundle = await client.demo_batch_ingest()
    for key, rows in bundle.items():
        print(f"{key}: {len(rows)} states")
    pop = bundle["population"]
    if pop:
        print(f"sample population row: {pop[0]}")


def main() -> None:
    asyncio.run(_demo_main())


if __name__ == "__main__":
    main()
    sys.exit(0)
