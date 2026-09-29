Companies House Change Monitor
PostgreSQL-backed Companies House change monitor using separate REST and Streaming API credentials.

Classifications
SH01 only

SH01 + new individual PSC

SH01 + new RLE

SH01 + new PSC/RLE

New PSC/RLE only

PSC statement only

Dashboard filters
The dashboard includes only companies that:

Were incorporated within the last 730 days.

Have at least one of these SIC codes: 62011, 62012, 63110, 63120, 72110, 72190, 21100, 21200.

The dashboard displays the matching SIC codes and incorporation date.

Files
main.py: FastAPI dashboard, REST profile lookups, filters, and API.

stream_worker.py: Companies House Streaming API consumer.

requirements.txt: Python dependencies.

Dockerfile: container definition.

render.yaml: Render web and worker services.

Environment variables
Web service

text
REST_API_KEY=Companies House REST API key
DATABASE_URL=shared PostgreSQL connection URL
FILING_SSE_URL=https://stream.companieshouse.gov.uk/filings
PSC_SSE_URL=https://stream.companieshouse.gov.uk/persons-with-significant-control
PSC_STATEMENTS_SSE_URL=confirmed PSC statement URL
PORT=8000
Worker service

text
STREAM_API_KEY=Companies House Streaming API key
DATABASE_URL=the same shared PostgreSQL connection URL
FILING_SSE_URL=https://stream.companieshouse.gov.uk/filings
PSC_SSE_URL=https://stream.companieshouse.gov.uk/persons-with-significant-control
PSC_STATEMENTS_SSE_URL=confirmed PSC statement URL
Do not commit API keys, stream keys, passwords, or database URLs to GitHub.

Deployment
Deploy the repository as a Render Blueprint using render.yaml. Set REST_API_KEY on the web service and STREAM_API_KEY on the worker. Set exactly the same DATABASE_URL on both services.

The web service fetches company profiles from the Companies House REST API to retrieve date_of_creation and sic_codes. The worker listens to the filing and PSC streams. The web service performs an automatic profile backfill at startup for existing events.

Local run
bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export REST_API_KEY="your-rest-api-key"
export STREAM_API_KEY="your-streaming-api-key"
export DATABASE_URL="postgresql://user:password@localhost:5432/companies"
python main.py
In another terminal, run:

bash
export STREAM_API_KEY="your-streaming-api-key"
export DATABASE_URL="postgresql://user:password@localhost:5432/companies"
python stream_worker.py
Diagnostics
/health: service health.

/api/debug: event, profile, and match counts.

/api/metrics: counts by classification.

/api/companies: filtered dashboard data.

/api/admin/backfill: manually starts profile backfill with a POST request.
