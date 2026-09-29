import asyncio
import json
import os
import re
from datetime import date, datetime, timezone
from typing import Any

import asyncpg
import httpx
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

DATABASE_URL = os.getenv("DATABASE_URL", "")
REST_API_KEY = os.getenv("REST_API_KEY", "")
PORT = int(os.getenv("PORT", "8000"))
DB_POOL: asyncpg.Pool | None = None
CLASSIFICATIONS = ["SH01 only", "SH01 + new individual PSC", "SH01 + new RLE", "SH01 + new PSC/RLE", "New PSC/RLE only", "PSC statement only"]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def serialise(value: Any) -> Any:
    return value.isoformat() if isinstance(value, (datetime, date)) else value


def classify(categories: set[str]) -> str | None:
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
        CREATE TABLE IF NOT EXISTS company_profiles (
            company_number TEXT PRIMARY KEY, company_name TEXT,
            incorporation_date DATE, sic_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
            profile_updated_at TIMESTAMPTZ NOT NULL,
            profile_source TEXT NOT NULL DEFAULT 'rest-profile'
        );
        CREATE TABLE IF NOT EXISTS company_events (
            id BIGSERIAL PRIMARY KEY, company_number TEXT NOT NULL, company_name TEXT,
            event_category TEXT NOT NULL, event_type TEXT, resource_kind TEXT,
            resource_id TEXT NOT NULL, resource_uri TEXT, filing_type TEXT,
            filing_description TEXT, psc_kind TEXT, psc_name TEXT, statement_type TEXT,
            event_date TEXT, published_at TIMESTAMPTZ NOT NULL, raw_data JSONB NOT NULL,
            UNIQUE(resource_kind, resource_id, event_type)
        );
        CREATE INDEX IF NOT EXISTS idx_company_events_company ON company_events(company_number);
        CREATE TABLE IF NOT EXISTS company_matches (
            company_number TEXT PRIMARY KEY, company_name TEXT, classification TEXT NOT NULL,
            has_sh01 BOOLEAN NOT NULL DEFAULT FALSE, has_new_individual_psc BOOLEAN NOT NULL DEFAULT FALSE,
            has_new_rle BOOLEAN NOT NULL DEFAULT FALSE, has_psc_statement BOOLEAN NOT NULL DEFAULT FALSE,
            first_event_at TIMESTAMPTZ, latest_event_at TIMESTAMPTZ, updated_at TIMESTAMPTZ NOT NULL
        );
    """)


async def refresh_company_match(company_number: str) -> None:
    assert DB_POOL is not None
    rows = await DB_POOL.fetch("SELECT * FROM company_events WHERE company_number=$1 ORDER BY published_at", company_number)
    if not rows:
        return
    categories = {row["event_category"] for row in rows}
    classification = classify(categories)
    if not classification:
        return
    profile = await DB_POOL.fetchrow("SELECT company_name FROM company_profiles WHERE company_number=$1", company_number)
    company_name = (profile["company_name"] if profile and profile["company_name"] else None) or rows[-1]["company_name"] or company_number
    await DB_POOL.execute("""
        INSERT INTO company_matches (company_number, company_name, classification, has_sh01, has_new_individual_psc, has_new_rle, has_psc_statement, first_event_at, latest_event_at, updated_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT(company_number) DO UPDATE SET company_name=EXCLUDED.company_name, classification=EXCLUDED.classification,
            has_sh01=EXCLUDED.has_sh01, has_new_individual_psc=EXCLUDED.has_new_individual_psc,
            has_new_rle=EXCLUDED.has_new_rle, has_psc_statement=EXCLUDED.has_psc_statement,
            first_event_at=EXCLUDED.first_event_at, latest_event_at=EXCLUDED.latest_event_at, updated_at=EXCLUDED.updated_at
    """, company_number, company_name, classification, "SH01" in categories, "NEW_INDIVIDUAL_PSC" in categories, "NEW_RLE" in categories, "PSC_STATEMENT" in categories, rows[0]["published_at"], rows[-1]["published_at"], utc_now())


async def backfill_matches() -> int:
    assert DB_POOL is not None
    rows = await DB_POOL.fetch("SELECT DISTINCT company_number FROM company_events")
    for row in rows:
        await refresh_company_match(row["company_number"])
    return len(rows)


app = FastAPI(title="Companies House Change Monitor Diagnostic")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.on_event("startup")
async def startup() -> None:
    global DB_POOL
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")
    if not REST_API_KEY:
        raise RuntimeError("REST_API_KEY is not set")
    DB_POOL = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=10)
    await initialise_database()
    asyncio.create_task(backfill_matches())


@app.on_event("shutdown")
async def shutdown() -> None:
    if DB_POOL:
        await DB_POOL.close()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "timestamp": utc_now().isoformat()}


@app.get("/api/debug/diagnostics")
async def diagnostics() -> dict[str, Any]:
    assert DB_POOL is not None
    event_rows = await DB_POOL.fetch("SELECT DISTINCT company_number FROM company_events ORDER BY company_number LIMIT 20")
    profile_rows = await DB_POOL.fetch("SELECT company_number, company_name, incorporation_date, sic_codes FROM company_profiles ORDER BY company_number LIMIT 20")
    match_rows = await DB_POOL.fetch("SELECT company_number, company_name, classification FROM company_matches ORDER BY latest_event_at DESC LIMIT 20")
    categories = await DB_POOL.fetch("SELECT event_category, COUNT(*) AS count FROM company_events GROUP BY event_category ORDER BY event_category")
    return {
        "counts": {
            "event_rows": await DB_POOL.fetchval("SELECT COUNT(*) FROM company_events"),
            "profile_rows": await DB_POOL.fetchval("SELECT COUNT(*) FROM company_profiles"),
            "match_rows": await DB_POOL.fetchval("SELECT COUNT(*) FROM company_matches"),
            "event_companies": await DB_POOL.fetchval("SELECT COUNT(DISTINCT company_number) FROM company_events"),
            "profile_companies": await DB_POOL.fetchval("SELECT COUNT(DISTINCT company_number) FROM company_profiles"),
            "event_profile_join": await DB_POOL.fetchval("SELECT COUNT(DISTINCT e.company_number) FROM company_events e INNER JOIN company_profiles p ON p.company_number=e.company_number"),
            "event_match_join": await DB_POOL.fetchval("SELECT COUNT(DISTINCT e.company_number) FROM company_events e INNER JOIN company_matches m ON m.company_number=e.company_number"),
        },
        "event_categories": [dict(row) for row in categories],
        "sample_event_numbers": [row["company_number"] for row in event_rows],
        "sample_profiles": [{"company_number": row["company_number"], "company_name": row["company_name"], "incorporation_date": serialise(row["incorporation_date"]), "sic_codes": row["sic_codes"]} for row in profile_rows],
        "sample_matches": [dict(row) for row in match_rows],
    }


@app.get("/api/debug/event-classifications")
async def event_classifications() -> dict[str, Any]:
    assert DB_POOL is not None
    rows = await DB_POOL.fetch("SELECT company_number, ARRAY_AGG(DISTINCT event_category) AS categories, COUNT(*) AS event_count FROM company_events GROUP BY company_number ORDER BY MAX(published_at) DESC LIMIT 100")
    result = []
    for row in rows:
        categories = set(row["categories"] or [])
        result.append({"company_number": row["company_number"], "categories": sorted(categories), "calculated_classification": classify(categories), "event_count": row["event_count"]})
    return {"sample_count": len(result), "companies": result}


@app.post("/api/admin/backfill")
async def admin_backfill() -> dict[str, Any]:
    return {"processed_company_numbers": await backfill_matches()}


@app.get("/api/metrics")
async def metrics() -> dict[str, Any]:
    assert DB_POOL is not None
    rows = await DB_POOL.fetch("SELECT classification, COUNT(*) AS count FROM company_matches GROUP BY classification")
    counts = {classification: 0 for classification in CLASSIFICATIONS}
    for row in rows:
        counts[row["classification"]] = row["count"]
    return {"counts": counts, "total": sum(counts.values()), "updated_at": utc_now().isoformat()}


@app.get("/api/companies")
async def companies(limit: int = Query(100, ge=1, le=1000), classification: str | None = None) -> dict[str, Any]:
    assert DB_POOL is not None
    if classification:
        rows = await DB_POOL.fetch("SELECT m.*, p.incorporation_date, p.sic_codes FROM company_matches m LEFT JOIN company_profiles p ON p.company_number=m.company_number WHERE m.classification=$1 ORDER BY m.latest_event_at DESC LIMIT $2", classification, limit)
    else:
        rows = await DB_POOL.fetch("SELECT m.*, p.incorporation_date, p.sic_codes FROM company_matches m LEFT JOIN company_profiles p ON p.company_number=m.company_number ORDER BY m.latest_event_at DESC LIMIT $1", limit)
    result = []
    for row in rows:
        item = {key: serialise(value) for key, value in dict(row).items()}
        item["sic_codes"] = row["sic_codes"] or []
        item["companies_house_url"] = f"https://find-and-update.company-information.service.gov.uk/company/{row['company_number']}"
        result.append(item)
    return {"companies": result, "count": len(result)}


@app.get("/api/companies/{company_number}/events")
async def company_events(company_number: str) -> dict[str, Any]:
    assert DB_POOL is not None
    rows = await DB_POOL.fetch("SELECT * FROM company_events WHERE company_number=$1 ORDER BY published_at DESC", company_number)
    return {"company_number": company_number, "events": [{key: serialise(value) for key, value in dict(row).items()} for row in rows]}


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    return """<!doctype html><html><head><meta charset='utf-8'><title>Companies House Change Monitor</title><style>body{font-family:Arial;margin:24px;color:#172033}table{border-collapse:collapse;width:100%;font-size:14px}th,td{padding:9px;border-bottom:1px solid #ddd;text-align:left}th{background:#f3f4f6}.pill{padding:4px 7px;border-radius:12px;background:#e0e7ff}.toolbar{display:flex;gap:10px;margin:15px 0}button,select{padding:8px}</style></head><body><h1>Companies House Change Monitor</h1><p>Diagnostic version. SIC codes and incorporation date are informational only.</p><div id='metrics'></div><div class='toolbar'><select id='classification'><option value=''>All classifications</option></select><button onclick='load()'>Refresh</button></div><table><thead><tr><th>Company</th><th>Classification</th><th>SIC code(s)</th><th>Incorporated</th><th>Latest event</th><th>Companies House</th></tr></thead><tbody id='rows'></tbody></table><script>const cs=['SH01 only','SH01 + new individual PSC','SH01 + new RLE','SH01 + new PSC/RLE','New PSC/RLE only','PSC statement only'];for(const c of cs){let o=document.createElement('option');o.value=c;o.textContent=c;document.querySelector('#classification').appendChild(o)}async function load(){let c=document.querySelector('#classification').value;let d=await(await fetch('/api/companies?limit=250'+(c?'&classification='+encodeURIComponent(c):''))).json();document.querySelector('#rows').innerHTML=d.companies.map(x=>`<tr><td><a href='/api/companies/${x.company_number}/events'>${x.company_name||''}</a><br><small>${x.company_number}</small></td><td><span class='pill'>${x.classification}</span></td><td>${(x.sic_codes||[]).join(', ')}</td><td>${x.incorporation_date||'Not available'}</td><td>${x.latest_event_at||''}</td><td><a target='_blank' href='${x.companies_house_url}'>Open</a></td></tr>`).join('');let m=await(await fetch('/api/metrics')).json();document.querySelector('#metrics').textContent='Total matched companies: '+m.total+' | '+Object.entries(m.counts).map(([k,v])=>k+': '+v).join(' | ')}load();setInterval(load,30000);</script></body></html>"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
