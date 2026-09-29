import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import aiosqlite
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse

DATABASE_FILE = os.getenv("DATABASE_FILE", "/data/companies.db")
PORT = int(os.getenv("PORT", "8000"))

sse_clients: list[asyncio.Queue] = []
db_conn: aiosqlite.Connection | None = None

CLASSIFICATIONS = [
    "SH01 only",
    "SH01 + new individual PSC",
    "SH01 + new RLE",
    "SH01 + new PSC/RLE",
    "New PSC/RLE only",
    "PSC statement only",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def classify_company_events(events: list[dict[str, Any]]) -> str | None:
    has_sh01 = any(e["event_category"] == "SH01" for e in events)
    has_individual = any(e["event_category"] == "NEW_INDIVIDUAL_PSC" for e in events)
    has_rle = any(e["event_category"] == "NEW_RLE" for e in events)
    has_statement = any(e["event_category"] == "PSC_STATEMENT" for e in events)

    if has_sh01 and has_individual and has_rle:
        return "SH01 + new PSC/RLE"
    if has_sh01 and has_individual:
        return "SH01 + new individual PSC"
    if has_sh01 and has_rle:
        return "SH01 + new RLE"
    if has_sh01:
        return "SH01 only"
    if has_individual or has_rle:
        return "New PSC/RLE only"
    if has_statement:
        return "PSC statement only"
    return None


async def initialise_database() -> None:
    global db_conn
    path = Path(DATABASE_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    db_conn = await aiosqlite.connect(str(path))
    db_conn.row_factory = aiosqlite.Row

    await db_conn.executescript("""
    CREATE TABLE IF NOT EXISTS company_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        published_at TEXT NOT NULL,
        raw_data TEXT NOT NULL,
        UNIQUE(resource_kind, resource_id, event_type)
    );
    CREATE INDEX IF NOT EXISTS idx_events_company
        ON company_events(company_number);
    CREATE TABLE IF NOT EXISTS company_matches (
        company_number TEXT PRIMARY KEY,
        company_name TEXT,
        classification TEXT NOT NULL,
        has_sh01 INTEGER NOT NULL DEFAULT 0,
        has_new_individual_psc INTEGER NOT NULL DEFAULT 0,
        has_new_rle INTEGER NOT NULL DEFAULT 0,
        has_psc_statement INTEGER NOT NULL DEFAULT 0,
        first_event_at TEXT,
        latest_event_at TEXT,
        updated_at TEXT NOT NULL
    );
    """)
    await db_conn.commit()


async def refresh_company_match(company_number: str) -> None:
    assert db_conn is not None
    cursor = await db_conn.execute(
        "SELECT * FROM company_events WHERE company_number = ? ORDER BY published_at",
        (company_number,),
    )
    rows = await cursor.fetchall()
    events = [dict(row) for row in rows]
    classification = classify_company_events(events)
    if not classification:
        return

    categories = {e["event_category"] for e in events}
    values = (
        company_number,
        events[-1]["company_name"],
        classification,
        int("SH01" in categories),
        int("NEW_INDIVIDUAL_PSC" in categories),
        int("NEW_RLE" in categories),
        int("PSC_STATEMENT" in categories),
        events[0]["published_at"],
        events[-1]["published_at"],
        utc_now(),
    )
    await db_conn.execute("""
        INSERT INTO company_matches (
            company_number, company_name, classification,
            has_sh01, has_new_individual_psc, has_new_rle,
            has_psc_statement, first_event_at, latest_event_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(company_number) DO UPDATE SET
            company_name = excluded.company_name,
            classification = excluded.classification,
            has_sh01 = excluded.has_sh01,
            has_new_individual_psc = excluded.has_new_individual_psc,
            has_new_rle = excluded.has_new_rle,
            has_psc_statement = excluded.has_psc_statement,
            first_event_at = excluded.first_event_at,
            latest_event_at = excluded.latest_event_at,
            updated_at = excluded.updated_at
    """, values)
    await db_conn.commit()


async def insert_event(event: dict[str, Any]) -> None:
    assert db_conn is not None
    await db_conn.execute("""
        INSERT OR IGNORE INTO company_events (
            company_number, company_name, event_category, event_type,
            resource_kind, resource_id, resource_uri, filing_type,
            filing_description, psc_kind, psc_name, statement_type,
            event_date, published_at, raw_data
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, tuple(event.get(k) for k in (
        "company_number", "company_name", "event_category", "event_type",
        "resource_kind", "resource_id", "resource_uri", "filing_type",
        "filing_description", "psc_kind", "psc_name", "statement_type",
        "event_date", "published_at", "raw_data",
    )))
    await db_conn.commit()
    await refresh_company_match(event["company_number"])


@app = FastAPI(title="Companies House Change Monitor")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def startup() -> None:
    await initialise_database()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "timestamp": utc_now()}


@app.get("/api/metrics")
async def metrics() -> dict[str, Any]:
    assert db_conn is not None
    cursor = await db_conn.execute("SELECT classification, COUNT(*) AS count FROM company_matches GROUP BY classification")
    counts = {classification: 0 for classification in CLASSIFICATIONS}
    for row in await cursor.fetchall():
        counts[row["classification"]] = row["count"]
    return {"counts": counts, "total": sum(counts.values()), "updated_at": utc_now()}


@app.get("/api/companies")
async def companies(limit: int = Query(100, ge=1, le=1000), classification: str | None = None) -> dict[str, Any]:
    assert db_conn is not None
    if classification:
        cursor = await db_conn.execute(
            "SELECT * FROM company_matches WHERE classification = ? ORDER BY latest_event_at DESC LIMIT ?",
            (classification, limit),
        )
    else:
        cursor = await db_conn.execute(
            "SELECT * FROM company_matches ORDER BY latest_event_at DESC LIMIT ?",
            (limit,),
        )
    result = []
    for row in await cursor.fetchall():
        item = dict(row)
        item["companies_house_url"] = f"https://find-and-update.company-information.service.gov.uk/company/{row['company_number']}"
        result.append(item)
    return {"companies": result, "count": len(result)}


@app.get("/api/companies/{company_number}/events")
async def company_events(company_number: str) -> dict[str, Any]:
    assert db_conn is not None
    cursor = await db_conn.execute(
        "SELECT * FROM company_events WHERE company_number = ? ORDER BY published_at DESC",
        (company_number,),
    )
    return {"company_number": company_number, "events": [dict(row) for row in await cursor.fetchall()]}


@app.get("/stream")
async def stream() -> StreamingResponse:
    queue: asyncio.Queue = asyncio.Queue()
    sse_clients.append(queue)

    async def generate():
        try:
            while True:
                payload = await queue.get()
                yield f"data: {json.dumps(payload)}\n\n"
        finally:
            if queue in sse_clients:
                sse_clients.remove(queue)

    return StreamingResponse(generate(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    return """
<!doctype html><html><head><meta charset='utf-8'><title>Companies House Change Monitor</title>
<style>body{font-family:Arial;margin:24px;color:#172033}table{border-collapse:collapse;width:100%;font-size:14px}th,td{padding:9px;border-bottom:1px solid #ddd;text-align:left}th{background:#f3f4f6}.pill{padding:4px 7px;border-radius:12px;background:#e0e7ff}.toolbar{display:flex;gap:10px;margin:15px 0}button,select{padding:8px}</style></head>
<body><h1>Companies House Change Monitor</h1><div id='metrics'></div><div class='toolbar'><select id='classification'><option value=''>All classifications</option></select><button onclick='load()'>Refresh</button></div><table><thead><tr><th>Company</th><th>Classification</th><th>Latest event</th><th>Companies House</th></tr></thead><tbody id='rows'></tbody></table>
<script>
const classifications=['SH01 only','SH01 + new individual PSC','SH01 + new RLE','SH01 + new PSC/RLE','New PSC/RLE only','PSC statement only'];
for(const c of classifications){const o=document.createElement('option');o.value=c;o.textContent=c;document.querySelector('#classification').appendChild(o)}
async function load(){const c=document.querySelector('#classification').value;const url='/api/companies?limit=250'+(c?'&classification='+encodeURIComponent(c):'');const data=await (await fetch(url)).json();document.querySelector('#rows').innerHTML=data.companies.map(x=>`<tr><td><a href='/api/companies/${x.company_number}/events'>${x.company_name||''}</a><br><small>${x.company_number}</small></td><td><span class='pill'>${x.classification}</span></td><td>${x.latest_event_at||''}</td><td><a target='_blank' href='${x.companies_house_url}'>Open</a></td></tr>`).join('');const m=await (await fetch('/api/metrics')).json();document.querySelector('#metrics').textContent='Total matched companies: '+m.total+' | '+Object.entries(m.counts).map(([k,v])=>k+': '+v).join(' | ')}
load();setInterval(load,30000);new EventSource('/stream').onmessage=load;
</script></body></html>"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)

