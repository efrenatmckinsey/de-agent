"""
USDA FoodData Central (FDC) API client — sample data source for hydra-ingest.

Free public API; optional API key improves rate limits (USDA_API_KEY).
Docs: https://fdc.nal.usda.gov/api-guide.html
"""

from __future__ import annotations

import os
import sys
from typing import Any

import httpx

# Base URL for FoodData Central v1 REST API
BASE_URL = "https://api.nal.usda.gov/fdc/v1"


class USDAFoodClient:
    """Async client for USDA FoodData Central search, detail, and list endpoints."""

    BASE_URL = BASE_URL

    def __init__(self, api_key: str | None = None) -> None:
        """
        Initialize the client.

        :param api_key: USDA FDC API key, or None to read USDA_API_KEY from the environment.
            The API accepts unsigned requests with lower rate limits.
        """
        self._api_key = api_key if api_key is not None else os.environ.get("USDA_API_KEY") or None
        self._client: httpx.AsyncClient | None = None

    def _params(self) -> dict[str, str]:
        """Query params including apiKey when configured."""
        if self._api_key:
            return {"apiKey": self._api_key}
        return {}

    async def __aenter__(self) -> USDAFoodClient:
        self._client = httpx.AsyncClient(base_url=self.BASE_URL, timeout=60.0)
        return self

    async def __aexit__(self, *args: object) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self.BASE_URL, timeout=60.0)
        return self._client

    async def search_foods(
        self,
        query: str,
        page_size: int = 25,
        page_number: int = 1,
    ) -> dict[str, Any]:
        """
        POST /foods/search — keyword search with pagination metadata in the response.
        """
        client = self._get_client()
        payload: dict[str, Any] = {
            "query": query,
            "pageSize": page_size,
            "pageNumber": page_number,
        }
        r = await client.post("/foods/search", params=self._params(), json=payload)
        r.raise_for_status()
        return r.json()

    async def get_food(self, fdc_id: int) -> dict[str, Any]:
        """GET /food/{fdc_id} — full food record for a single FDC ID."""
        client = self._get_client()
        r = await client.get(f"/food/{fdc_id}", params=self._params())
        r.raise_for_status()
        return r.json()

    async def get_foods_list(
        self,
        data_type: str = "Foundation",
        page_size: int = 50,
        page_number: int = 1,
    ) -> list[dict[str, Any]]:
        """
        POST /foods/list — browse foods by data type (e.g. Foundation, SR Legacy).
        """
        client = self._get_client()
        body = {
            "dataType": [data_type],
            "pageSize": page_size,
            "pageNumber": page_number,
        }
        r = await client.post("/foods/list", params=self._params(), json=body)
        r.raise_for_status()
        data = r.json()
        foods = data.get("foods")
        if isinstance(foods, list):
            return [x for x in foods if isinstance(x, dict)]
        return []

    async def demo_ingest(
        self,
        query: str = "apple",
        max_pages: int = 3,
    ) -> list[dict[str, Any]]:
        """
        Paginate search results up to ``max_pages`` and return a flat list of food dicts.

        Each record includes ``_ingest_page`` and ``_ingest_query`` for traceability.
        """
        flat: list[dict[str, Any]] = []
        for page in range(1, max_pages + 1):
            body = await self.search_foods(query=query, page_size=25, page_number=page)
            foods = body.get("foods")
            if not isinstance(foods, list) or not foods:
                break
            for item in foods:
                if isinstance(item, dict):
                    row = dict(item)
                    row["_ingest_page"] = page
                    row["_ingest_query"] = query
                    flat.append(row)
            total_pages = body.get("totalPages")
            if isinstance(total_pages, int) and page >= total_pages:
                break
        return flat


async def _demo_main() -> None:
    """Run demo ingest and print summary statistics."""
    async with USDAFoodClient() as client:
        records = await client.demo_ingest(query="apple", max_pages=3)
    print(f"demo_ingest records: {len(records)}")
    if records:
        sample = records[0]
        fdc_id = sample.get("fdcId") or sample.get("id")
        print(f"sample fdcId: {fdc_id}, description: {sample.get('description', '')[:80]!r}")
    with_ids = sum(1 for r in records if r.get("fdcId") is not None)
    print(f"records with fdcId: {with_ids}")


def main() -> None:
    asyncio.run(_demo_main())


if __name__ == "__main__":
    main()
    sys.exit(0)
