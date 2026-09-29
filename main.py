import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

import asyncpg
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse

DATABASE_URL = os.getenv("DATABASE_URL", "")
PORT = int(os.getenv("PORT", "8000"))
DB_POOL: asyncpg.Pool | None = None
SSE_CLIENTS: list[asyncio.Queue] = []
CLASSIFICATIONS = ["SH01 only", "SH01 + new individual PSC", "SH01 + new RLE", "SH01 + new PSC/RLE", "New PSC/RLE only", "PSC statement only"]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def classify_company_events(events: list[dict[str, Any]]) -> str | None:
    categories = {event["event_category"] for event in events}
    if {"SH01", "NEW_INDIVIDUAL_PSC", "NEW_RLE"}.issubset(categories):
        return "SH01 + new PSC/RLE"
    if {"SH01", "NEW_INDIVIDUAL_PSC"}.issubset(categories):
        return "SH01 + new individual PSC"
    if {"SH01", "NEW_RLE"}.issubset(categories):
        return "SH01 + new RLE"
    if "SH01" in categories:
        return "SH01 only"
    if categories & {"NEW_INDIVIDUAL_PSC", "NEW_RLE"}:
        return "New PSC/RLE only"
    if "PSC_STATEMENT" in categories:
        return "PSC statement only"
    return None


async def initialise_database() -> None:
    assert DB_POOL is not None
    await DB_POOL.execute("""
        CREATE TABLE IF NOT EXISTS company_events (
            id BIGSERIAL PRIMARY KEY,
            company_number TEXT NOT NULL,
            company_name TEXT,
            event_category TEXT NOT NULL,
            event_type TEXT,
            resource_kind TEXT,
            resource_id TEXT NOT NULL,
            resource_uri TEXT,
            filing_type TEXT,
            filing_description TEXT,
            psc_kind TEXT,
            psc_name TEXT,
            statement_type TEXT,
            event_date TEXT,
            published_at TIMESTAMPTZ NOT NULL,
            raw_data JSONB NOT NULL,
            UNIQUE(resource_kind, resource_id, event_type)
        );
        CREATE INDEX IF NOT EXISTS idx_company_events_company ON company_events(company_number);
        CREATE TABLE IF NOT EXISTS company_matches (
            company_number TEXT PRIMARY KEY,
            company_name TEXT,
            classification TEXT NOT NULL,
            has_sh01 BOOLEAN NOT NULL DEFAULT FALSE,
            has_new_individual_psc BOOLEAN NOT NULL DEFAULT FALSE,
            has_new_rle BOOLEAN NOT NULL DEFAULT FALSE,
            has_psc_statement BOOLEAN NOT NULL DEFAULT FALSE,
            first_event_at TIMESTAMPTZ,
            latest_event_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL
        );
    """)


async def refresh_company_match(company_number: str) -> None:
    assert DB_POOL is not None
    rows = await DB_POOL.fetch("SELECT * FROM company_events WHERE company_number = $1 ORDER BY published_at", company_number)
    if not rows:
        return
    events = [dict(row) for row in rows]
    classification = classify_company_events(events)
    if not classification:
        return
    categories = {event["event_category"] for event in events}
    await DB_POOL.execute("""
        INSERT INTO company_matches (company_number, company_name, classification, has_sh01, has_new_individual_psc, has_new_rle, has_psc_statement, first_event_at, latest_event_at, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT(company_number) DO UPDATE SET
            company_name=EXCLUDED.company_name, classification=EXCLUDED.classification,
            has_sh01=EXCLUDED.has_sh01, has_new_individual_psc=EXCLUDED.has_new_individual_psc,
            has_new_rle=EXCLUDED.has_new_rle, has_psc_statement=EXCLUDED.has_psc_statement,
            first_event_at=EXCLUDED.first_event_at, latest_event_at=EXCLUDED.latest_event_at,
            updated_at=EXCLUDED.updated_at
    """, company_number, events[-1]["company_name"], classification,
        "SH01" in categories, "NEW_INDIVIDUAL_PSC" in categories,
        "NEW_RLE" in categories, "PSC_STATEMENT" in categories,
        events[0]["published_at"], events[-1]["published_at"], datetime.now(timezone.utc))


async def insert_event(event: dict[str, Any]) -> None:
    assert DB_POOL is not None
    await DB_POOL.execute("""
        INSERT INTO company_events (company_number, company_name, event_category, event_type, resource_kind, resource_id, resource_uri, filing_type, filing_description, psc_kind, psc_name, statement_type, event_date, published_at, raw_data)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15::jsonb)
        ON CONFLICT(resource_kind, resource_id, event_type) DO NOTHING
    """, event["company_number"], event.get("company_name"), event["event_category"], event.get("event_type"), event.get("resource_kind"), event["resource_id"], event.get("resource_uri"), event.get("filing_type"), event.get("filing_description"), event.get("psc_kind"), event.get("psc_name"), event.get("statement_type"), event.get("event_date"), event["published_at"], event["raw_data"])
    await refresh_company_match(event["company_number"])


app = FastAPI(title="Companies House Change Monitor")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def startup() -> None:
    global DB_POOL
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    DB_POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    await initialise_database()


@app.on_event("shutdown")
async def shutdown() -> None:
    if DB_POOL:
        await DB_POOL.close()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "timestamp": utc_now()}


@app.get("/api/metrics")
async def metrics() -> dict[str, Any]:
    assert DB_POOL is not None
    rows = await DB_POOL.fetch("SELECT classification, COUNT(*) AS count FROM company_matches GROUP BY classification")
    counts = {classification: 0 for classification in CLASSIFICATIONS}
    for row in rows:
        counts[row["classification"]] = row["count"]
    return {"counts": counts, "total": sum(counts.values()), "updated_at": utc_now()}


@app.get("/api/companies")
async def companies(limit: int = Query(100, ge=1, le=1000), classification: str | None = None) -> dict[str, Any]:
    assert DB_POOL is not None
    if classification:
        rows = await DB_POOL.fetch("SELECT * FROM company_matches WHERE classification = $1 ORDER BY latest_event_at DESC LIMIT $2", classification, limit)
    else:
        rows = await DB_POOL.fetch("SELECT * FROM company_matches ORDER BY latest_event_at DESC LIMIT $1", limit)
    result = []
    for row in rows:
        item = dict(row)
        item["companies_house_url"] = f"https://find-and-update.company-information.service.gov.uk/company/{row['company_number']}"
        for key, value in list(item.items()):
            if hasattr(value, "isoformat"):
                item[key] = value.isoformat()
        result.append(item)
    return {"companies": result, "count": len(result)}


@app.get("/api/companies/{company_number}/events")
async def company_events(company_number: str) -> dict[str, Any]:
    assert DB_POOL is not None
    rows = await DB_POOL.fetch("SELECT * FROM company_events WHERE company_number = $1 ORDER BY published_at DESC", company_number)
    events = []
    for row in rows:
        item = dict(row)
        for key, value in list(item.items()):
            if hasattr(value, "isoformat"):
                item[key] = value.isoformat()
        events.append(item)
    return {"company_number": company_number, "events": events}


@app.get("/stream")
async def stream() -> StreamingResponse:
    queue: asyncio.Queue = asyncio.Queue()
    SSE_CLIENTS.append(queue)
    async def generate():
        try:
            while True:
                yield f"data: {json.dumps(await queue.get())}\n\n"
        finally:
            if queue in SSE_CLIENTS:
                SSE_CLIENTS.remove(queue)
    return StreamingResponse(generate(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    return """<!doctype html><html><head><meta charset='utf-8'><title>Companies House Change Monitor</title><style>body{font-family:Arial;margin:24px;color:#172033}table{border-collapse:collapse;width:100%;font-size:14px}th,td{padding:9px;border-bottom:1px solid #ddd;text-align:left}th{background:#f3f4f6}.pill{padding:4px 7px;border-radius:12px;background:#e0e7ff}.toolbar{display:flex;gap:10px;margin:15px 0}button,select{padding:8px}</style></head><body><h1>Companies House Change Monitor</h1><div id='metrics'></div><div class='toolbar'><select id='classification'><option value=''>All classifications</option></select><button onclick='load()'>Refresh</button></div><table><thead><tr><th>Company</th><th>Classification</th><th>Latest event</th><th>Companies House</th></tr></thead><tbody id='rows'></tbody></table><script>const cs=['SH01 only','SH01 + new individual PSC','SH01 + new RLE','SH01 + new PSC/RLE','New PSC/RLE only','PSC statement only'];for(const c of cs){let o=document.createElement('option');o.value=c;o.textContent=c;classification.appendChild(o)}async function load(){let c=classification.value;let d=await(await fetch('/api/companies?limit=250'+(c?'&classification='+encodeURIComponent(c):''))).json();rows.innerHTML=d.companies.map(x=>`<tr><td><a href='/api/companies/${x.company_number}/events'>${x.company_name||''}</a><br><small>${x.company_number}</small></td><td><span class='pill'>${x.classification}</span></td><td>${x.latest_event_at||''}</td><td><a target='_blank' href='${x.companies_house_url}'>Open</a></td></tr>`).join('');let m=await(await fetch('/api/metrics')).json();metrics.textContent='Total matched companies: '+m.total+' | '+Object.entries(m.counts).map(([k,v])=>k+': '+v).join(' | ')}load();setInterval(load,30000);new EventSource('/stream').onmessage=load;</script></body></html>"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
