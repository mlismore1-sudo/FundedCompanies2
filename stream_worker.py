import asyncio
import json
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import asyncpg
import httpx

STREAM_API_KEY = os.getenv("STREAM_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", "")
DEBUG_MODE = os.getenv("DEBUG_MODE", "true").lower() == "true"
STREAMS = {
    "FILING": os.getenv("FILING_SSE_URL", "https://stream.companieshouse.gov.uk/filings"),
    "PSC": os.getenv("PSC_SSE_URL", "https://stream.companieshouse.gov.uk/persons-with-significant-control"),
    "PSC_STATEMENT": os.getenv("PSC_STATEMENTS_SSE_URL", "https://stream.companieshouse.gov.uk/persons-with-significant-control-statements"),
}
COUNTS = Counter()
CATEGORIES = Counter()
INVALID_SAMPLES: list[dict[str, Any]] = []


def debug(message: str) -> None:
    if DEBUG_MODE:
        print(f"[DEBUG] {message}", flush=True)


def parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif value:
        try:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError:
            parsed = datetime.now(timezone.utc)
    else:
        parsed = datetime.now(timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def valid_company_number(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", "", str(value).strip().upper()).replace("/", "")
    if re.fullmatch(r"\d{8}", text) or re.fullmatch(r"[A-Z]{2}\d{6}", text):
        return text
    return None


def company_number(event: dict[str, Any], data: dict[str, Any]) -> tuple[str | None, str | None]:
    direct = [
        ("data.company_number", data.get("company_number")),
        ("event.company_number", event.get("company_number")),
        ("data.company_registration_number", data.get("company_registration_number")),
    ]
    for source, value in direct:
        candidate = valid_company_number(value)
        if candidate:
            return candidate, source
    links = data.get("links") or {}
    for source, value in [
        ("event.resource_uri", event.get("resource_uri", "")),
        ("data.links.self", links.get("self", "")),
        ("data.links.company", links.get("company", "")),
        ("data.links.company_profile", links.get("company_profile", "")),
    ]:
        match = re.search(r"/company/([A-Za-z0-9]+)", str(value))
        if match:
            candidate = valid_company_number(match.group(1))
            if candidate:
                return candidate, source
    return None, None


def category(stream: str, data: dict[str, Any]) -> str | None:
    if stream == "FILING":
        return "SH01" if data.get("type") == "SH01" else None
    if stream == "PSC_STATEMENT":
        return "PSC_STATEMENT"
    kind = str(data.get("kind", "")).lower()
    if "individual-person-with-significant-control" in kind:
        return "NEW_INDIVIDUAL_PSC"
    if any(marker in kind for marker in ("corporate-entity", "legal-person", "relevant-legal-entity")):
        return "NEW_RLE"
    return None


def normalise(stream: str, event: dict[str, Any], event_category: str) -> dict[str, Any] | None:
    data = event.get("data") or {}
    number, source = company_number(event, data)
    if not number or event.get("event", {}).get("type") == "deleted":
        COUNTS["invalid_or_deleted"] += 1
        if len(INVALID_SAMPLES) < 20:
            INVALID_SAMPLES.append({
                "stream": stream,
                "resource_id": event.get("resource_id"),
                "resource_kind": event.get("resource_kind"),
                "resource_uri": event.get("resource_uri"),
                "data_keys": sorted(data.keys()),
            })
        debug(f"REJECTED stream={stream} resource_id={event.get('resource_id')} resource_kind={event.get('resource_kind')}")
        return None
    debug(f"EXTRACTED company_number={number} source={source} category={event_category} resource_id={event.get('resource_id')}")
    metadata = event.get("event") or {}
    return {
        "company_number": number,
        "company_name": data.get("company_name") or data.get("name") or data.get("linked_psc_name"),
        "event_category": event_category,
        "event_type": metadata.get("type"),
        "resource_kind": event.get("resource_kind"),
        "resource_id": event.get("resource_id", "") or number,
        "resource_uri": event.get("resource_uri"),
        "filing_type": data.get("type") if stream == "FILING" else None,
        "filing_description": data.get("description") if stream == "FILING" else None,
        "psc_kind": data.get("kind") if stream == "PSC" else None,
        "psc_name": data.get("name") or data.get("linked_psc_name"),
        "statement_type": data.get("statement") if stream == "PSC_STATEMENT" else None,
        "event_date": str(data.get("date") or data.get("notified_on") or "") or None,
        "published_at": parse_timestamp(metadata.get("published_at")),
        "raw_data": json.dumps(event),
    }


async def save_event(pool: asyncpg.Pool, event: dict[str, Any]) -> bool:
    result = await pool.execute("""
        INSERT INTO company_events (
            company_number, company_name, event_category, event_type,
            resource_kind, resource_id, resource_uri, filing_type,
            filing_description, psc_kind, psc_name, statement_type,
            event_date, published_at, raw_data
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb)
        ON CONFLICT(resource_kind, resource_id, event_type) DO NOTHING
    """, event["company_number"], event.get("company_name"), event["event_category"], event.get("event_type"), event.get("resource_kind"), event["resource_id"], event.get("resource_uri"), event.get("filing_type"), event.get("filing_description"), event.get("psc_kind"), event.get("psc_name"), event.get("statement_type"), event.get("event_date"), event["published_at"], event["raw_data"])
    inserted = result.endswith("1")
    COUNTS["inserted" if inserted else "duplicates"] += 1
    return inserted


async def consume(stream: str, url: str, pool: asyncpg.Pool) -> None:
    timepoint_file = Path(f"/data/{stream.lower()}_timepoint.txt")
    timepoint_file.parent.mkdir(parents=True, exist_ok=True)
    last = timepoint_file.read_text().strip() if timepoint_file.exists() else None
    while True:
        try:
            params = {"timepoint": last} if last else None
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream("GET", url, params=params, auth=(STREAM_API_KEY, ""), headers={"Accept": "application/json"}) as response:
                    if response.status_code != 200:
                        COUNTS[f"http_{response.status_code}"] += 1
                        print(f"{stream} stream returned {response.status_code}", flush=True)
                        await asyncio.sleep(10)
                        continue
                    print(f"Connected to {stream}: {url}", flush=True)
                    async for line in response.aiter_lines():
                        if not line or line.startswith("id:") or line.startswith(":"):
                            continue
                        try:
                            event = json.loads(line)
                        except json.JSONDecodeError:
                            COUNTS["invalid_json"] += 1
                            continue
                        COUNTS["events_seen"] += 1
                        data = event.get("data") or {}
                        event_category = category(stream, data)
                        if event_category:
                            COUNTS["categorised"] += 1
                            CATEGORIES[event_category] += 1
                            item = normalise(stream, event, event_category)
                            if item and await save_event(pool, item):
                                print(f"Stored {item['event_category']}: {item['company_number']}", flush=True)
                        else:
                            COUNTS["uncategorised"] += 1
                        if COUNTS["events_seen"] % 100 == 0:
                            debug(f"SUMMARY stream={stream} counts={dict(COUNTS)} categories={dict(CATEGORIES)} invalid_samples={INVALID_SAMPLES[-3:]}")
                        point = event.get("event", {}).get("timepoint")
                        if point:
                            last = str(point)
                            timepoint_file.write_text(last)
        except Exception as exc:
            COUNTS["exceptions"] += 1
            print(f"{stream} error: {exc}", flush=True)
            await asyncio.sleep(10)


async def main() -> None:
    if not STREAM_API_KEY:
        raise RuntimeError("STREAM_API_KEY is not set")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    await asyncio.gather(*(consume(name, url, pool) for name, url in STREAMS.items()))


if __name__ == "__main__":
    asyncio.run(main())
