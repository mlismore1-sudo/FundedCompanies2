import asyncio
import json
import os
from datetime import date, datetime
from typing import Any

import asyncpg
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse

DATABASE_URL = os.getenv("DATABASE_URL", "")
PORT = int(os.getenv("PORT", "8000"))
DB_POOL: asyncpg.Pool | None = None
CLASSIFICATIONS = [
    "SH01 only",
    "SH01 + new individual PSC",
    "SH01 + new RLE",
    "SH01 + new PSC/RLE",
    "New PSC/RLE only",
    "PSC statement only",
]


def serialise(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def safe_json(value: Any) -> str:
    return json.dumps(value, default=serialise)


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
        CREATE INDEX IF NOT EXISTS idx_company_events_published
            ON company_events(published_at DESC);
        CREATE INDEX IF NOT EXISTS idx_company_events_company
            ON company_events(company_number);
        CREATE TABLE IF NOT EXISTS company_profiles (
            company_number TEXT PRIMARY KEY,
            company_name TEXT,
            incorporation_date DATE,
            sic_codes JSONB NOT NULL DEFAULT '[]'::jsonb,
            profile_updated_at TIMESTAMPTZ,
            profile_source TEXT NOT NULL DEFAULT 'optional'
        );
        CREATE TABLE IF NOT EXISTS company_matches (
            company_number TEXT PRIMARY KEY,
            company_name TEXT,
            classification TEXT NOT NULL,
            has_sh01 BOOLEAN NOT NULL DEFAULT FALSE,
            has_new_individual_psc BOOLEAN NOT NULL DEFAULT FALSE,
            has_new_rle BOOLEAN NOT NULL DEFAULT FALSE,
            has_psc_statement BOOLEAN NOT NULL DEFAULT FALSE,
            first_event_at TIMESTAMPTZ,
            latest_event_at TIMESTAMPTZ NOT NULL,
            updated_at TIMESTAMPTZ NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_company_matches_latest
            ON company_matches(latest_event_at DESC);
    """)


async def refresh_company_match(company_number: str) -> None:
    assert DB_POOL is not None
    rows = await DB_POOL.fetch(
        """
        SELECT *
        FROM company_events
        WHERE company_number = $1
        ORDER BY published_at
        """,
        company_number,
    )

    if not rows:
        return

    categories = {row["event_category"] for row in rows}

    if {"SH01", "NEW_INDIVIDUAL_PSC", "NEW_RLE"}.issubset(categories):
        classification = "SH01 + new PSC/RLE"
    elif {"SH01", "NEW_INDIVIDUAL_PSC"}.issubset(categories):
        classification = "SH01 + new individual PSC"
    elif {"SH01", "NEW_RLE"}.issubset(categories):
        classification = "SH01 + new RLE"
    elif "SH01" in categories:
        classification = "SH01 only"
    elif categories & {"NEW_INDIVIDUAL_PSC", "NEW_RLE"}:
        classification = "New PSC/RLE only"
    elif "PSC_STATEMENT" in categories:
        classification = "PSC statement only"
    else:
        return

    profile = await DB_POOL.fetchrow(
        """
        SELECT company_name
        FROM company_profiles
        WHERE company_number = $1
        """,
        company_number,
    )

    company_name = (
        profile["company_name"]
        if profile and profile["company_name"]
        else rows[-1]["company_name"]
        or company_number
    )

    await DB_POOL.execute(
        """
        INSERT INTO company_matches (
            company_number,
            company_name,
            classification,
            has_sh01,
            has_new_individual_psc,
            has_new_rle,
            has_psc_statement,
            first_event_at,
            latest_event_at,
            updated_at
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT(company_number) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            classification = EXCLUDED.classification,
            has_sh01 = EXCLUDED.has_sh01,
            has_new_individual_psc = EXCLUDED.has_new_individual_psc,
            has_new_rle = EXCLUDED.has_new_rle,
            has_psc_statement = EXCLUDED.has_psc_statement,
            first_event_at = EXCLUDED.first_event_at,
            latest_event_at = EXCLUDED.latest_event_at,
            updated_at = EXCLUDED.updated_at
        """,
        company_number,
        company_name,
        classification,
        "SH01" in categories,
        "NEW_INDIVIDUAL_PSC" in categories,
        "NEW_RLE" in categories,
        "PSC_STATEMENT" in categories,
        rows[0]["published_at"],
        rows[-1]["published_at"],
        datetime.now().astimezone(),
    )


app = FastAPI(title="Companies House Live Change Monitor")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup() -> None:
    global DB_POOL

    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set")

    DB_POOL = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=10,
    )

    await initialise_database()
    print("Live monitor started without backfill", flush=True)


@app.on_event("shutdown")
async def shutdown() -> None:
    if DB_POOL:
        await DB_POOL.close()


@app.get("/health")
async def health() -> dict[str, str]:
    return {
        "status": "ok",
        "timestamp": datetime.now().astimezone().isoformat(),
    }


@app.get("/api/metrics")
async def metrics() -> dict[str, Any]:
    assert DB_POOL is not None

    rows = await DB_POOL.fetch(
        """
        SELECT classification, COUNT(*) AS count
        FROM company_matches
        GROUP BY classification
        """
    )

    counts = {classification: 0 for classification in CLASSIFICATIONS}

    for row in rows:
        counts[row["classification"]] = row["count"]

    return {
        "counts": counts,
        "total": sum(counts.values()),
        "updated_at": datetime.now().astimezone().isoformat(),
    }


@app.get("/api/companies")
async def companies(
    limit: int = Query(100, ge=1, le=1000),
    classification: str | None = None,
) -> dict[str, Any]:
    assert DB_POOL is not None

    if classification:
        rows = await DB_POOL.fetch(
            """
            SELECT
                m.*,
                p.incorporation_date,
                p.sic_codes
            FROM company_matches m
            LEFT JOIN company_profiles p
                ON p.company_number = m.company_number
            WHERE m.classification = $1
            ORDER BY m.latest_event_at DESC
            LIMIT $2
            """,
            classification,
            limit,
        )
    else:
        rows = await DB_POOL.fetch(
            """
            SELECT
                m.*,
                p.incorporation_date,
                p.sic_codes
            FROM company_matches m
            LEFT JOIN company_profiles p
                ON p.company_number = m.company_number
            ORDER BY m.latest_event_at DESC
            LIMIT $1
            """,
            limit,
        )

    result = []

    for row in rows:
        item = {
            key: serialise(value)
            for key, value in dict(row).items()
        }
        item["sic_codes"] = row["sic_codes"] or []
        item["companies_house_url"] = (
            "https://find-and-update.company-information.service.gov.uk/"
            f"company/{row['company_number']}"
        )
        result.append(item)

    return {
        "companies": result,
        "count": len(result),
    }


@app.get("/api/companies/{company_number}/events")
async def company_events(company_number: str) -> dict[str, Any]:
    assert DB_POOL is not None

    rows = await DB_POOL.fetch(
        """
        SELECT *
        FROM company_events
        WHERE company_number = $1
        ORDER BY published_at DESC
        """,
        company_number,
    )

    events = [
        {
            key: serialise(value)
            for key, value in dict(row).items()
        }
        for row in rows
    ]

    return {
        "company_number": company_number,
        "events": events,
    }


@app.get("/api/debug")
async def debug() -> dict[str, Any]:
    assert DB_POOL is not None

    return {
        "event_count": await DB_POOL.fetchval(
            "SELECT COUNT(*) FROM company_events"
        ),
        "match_count": await DB_POOL.fetchval(
            "SELECT COUNT(*) FROM company_matches"
        ),
        "mode": "live-only",
        "backfill": False,
    }


@app.get("/", response_class=HTMLResponse)
async def dashboard() -> str:
    return """
<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Companies House Live Change Monitor</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 24px;
            color: #172033;
        }
        h1 {
            margin-bottom: 8px;
        }
        .status {
            color: #4b5563;
            margin-bottom: 16px;
        }
        .toolbar {
            display: flex;
            gap: 10px;
            margin: 15px 0;
        }
        button, select {
            padding: 8px;
            font-size: 14px;
        }
        table {
            border-collapse: collapse;
            width: 100%;
            font-size: 14px;
        }
        th, td {
            padding: 9px;
            border-bottom: 1px solid #ddd;
            text-align: left;
            vertical-align: top;
        }
        th {
            background: #f3f4f6;
        }
        .pill {
            padding: 4px 7px;
            border-radius: 12px;
            background: #e0e7ff;
            display: inline-block;
        }
        .error {
            color: #b91c1c;
        }
        @media (max-width: 900px) {
            table {
                font-size: 12px;
            }
            th, td {
                padding: 6px;
            }
        }
    </style>
</head>
<body>
    <h1>Companies House Live Change Monitor</h1>
    <p class="status">
        Live-only mode. SIC codes and incorporation date are displayed when available.
        Refreshing every 3 seconds.
    </p>

    <div id="metrics">Loading metrics...</div>

    <div class="toolbar">
        <label for="classification">Classification:</label>
        <select id="classification">
            <option value="">All classifications</option>
        </select>
        <button id="refresh" type="button">Refresh now</button>
    </div>

    <table>
        <thead>
            <tr>
                <th>Company</th>
                <th>Classification</th>
                <th>SIC code(s)</th>
                <th>Incorporated</th>
                <th>Latest event</th>
                <th>Companies House</th>
            </tr>
        </thead>
        <tbody id="rows">
            <tr>
                <td colspan="6">Loading companies...</td>
            </tr>
        </tbody>
    </table>

    <script>
        const classificationSelect =
            document.getElementById("classification");
        const rowsElement =
            document.getElementById("rows");
        const metricsElement =
            document.getElementById("metrics");
        const refreshButton =
            document.getElementById("refresh");

        const classifications = [
            "SH01 only",
            "SH01 + new individual PSC",
            "SH01 + new RLE",
            "SH01 + new PSC/RLE",
            "New PSC/RLE only",
            "PSC statement only"
        ];

        for (const item of classifications) {
            const option = document.createElement("option");
            option.value = item;
            option.textContent = item;
            classificationSelect.appendChild(option);
        }

        function escapeHtml(value) {
            return String(value ?? "")
                .replaceAll("&", "&amp;")
                .replaceAll("<", "&lt;")
                .replaceAll(">", "&gt;")
                .replaceAll('"', "&quot;")
                .replaceAll("'", "&#039;");
        }

        function showError(message) {
            rowsElement.innerHTML = `
                <tr>
                    <td colspan="6" class="error">
                        Dashboard error: ${escapeHtml(message)}
                    </td>
                </tr>
            `;
        }

        async function loadCompanies() {
            try {
                const selected = classificationSelect.value;
                const params = new URLSearchParams({
                    limit: "250"
                });

                if (selected) {
                    params.set("classification", selected);
                }

                const companyResponse = await fetch(
                    `/api/companies?${params.toString()}`,
                    { cache: "no-store" }
                );

                if (!companyResponse.ok) {
                    throw new Error(
                        `Companies API returned ${companyResponse.status}`
                    );
                }

                const companyData =
                    await companyResponse.json();

                if (!Array.isArray(companyData.companies)) {
                    throw new Error(
                        "Companies API returned an unexpected response"
                    );
                }

                if (companyData.companies.length === 0) {
                    rowsElement.innerHTML = `
                        <tr>
                            <td colspan="6">
                                No companies are currently available.
                            </td>
                        </tr>
                    `;
                } else {
                    rowsElement.innerHTML =
                        companyData.companies.map(company => {
                            const sicCodes =
                                (company.sic_codes || []).join(", ");
                            const number =
                                encodeURIComponent(
                                    company.company_number || ""
                                );
                            const companyUrl =
                                escapeHtml(
                                    company.companies_house_url || "#"
                                );

                            return `
                                <tr>
                                    <td>
                                        <a href="/api/companies/${number}/events">
                                            ${escapeHtml(
                                                company.company_name
                                            )}
                                        </a>
                                        <br>
                                        <small>
                                            ${escapeHtml(
                                                company.company_number
                                            )}
                                        </small>
                                    </td>
                                    <td>
                                        <span class="pill">
                                            ${escapeHtml(
                                                company.classification
                                            )}
                                        </span>
                                    </td>
                                    <td>
                                        ${escapeHtml(sicCodes)}
                                    </td>
                                    <td>
                                        ${escapeHtml(
                                            company.incorporation_date ||
                                            "Not available"
                                        )}
                                    </td>
                                    <td>
                                        ${escapeHtml(
                                            company.latest_event_at || ""
                                        )}
                                    </td>
                                    <td>
                                        <a
                                            target="_blank"
                                            rel="noopener"
                                            href="${companyUrl}">
                                            Open
                                        </a>
                                    </td>
                                </tr>
                            `;
                        }).join("");
                }

                const metricsResponse = await fetch(
                    "/api/metrics",
                    { cache: "no-store" }
                );

                if (!metricsResponse.ok) {
                    throw new Error(
                        `Metrics API returned ${metricsResponse.status}`
                    );
                }

                const metrics =
                    await metricsResponse.json();

                metricsElement.textContent =
                    `Total live matches: ${metrics.total} | ` +
                    Object.entries(metrics.counts)
                        .map(([name, count]) =>
                            `${name}: ${count}`
                        )
                        .join(" | ");
            } catch (error) {
                console.error("Dashboard load failed:", error);
                showError(error.message || "Unknown error");
            }
        }

        classificationSelect.addEventListener(
            "change",
            loadCompanies
        );

        refreshButton.addEventListener(
            "click",
            loadCompanies
        );

        loadCompanies();
        setInterval(loadCompanies, 3000);
    </script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
    )
