Companies House Change Monitor
This version monitors Companies House filing and PSC streams rather than incorporations.

Classifications
SH01 only

SH01 + new individual PSC

SH01 + new RLE

SH01 + new PSC/RLE

New PSC/RLE only

PSC statement only

Files
main.py: FastAPI dashboard and read API.

stream_worker.py: Companies House stream consumer and event normaliser.

requirements.txt: Python dependencies.

render.yaml: Render deployment configuration.

Dockerfile: Container definition.

Important deployment note
The supplied Dockerfile starts the dashboard only. The worker must run at the same time as the dashboard. On Render, use either a second background worker service running python stream_worker.py, or change the container entrypoint to run a supervisor that starts both processes. A second worker service is safer because it avoids running two stream consumers if the web service scales.

For a Render background worker, add this service to render.yaml:

text
  - type: worker
    name: companies-house-change-worker
    env: docker
    region: frankfurt
    plan: starter
    dockerContext: .
    dockerfilePath: ./Dockerfile
    dockerCommand: python stream_worker.py
    envVars:
      - key: API_KEY
        sync: false
      - key: DATABASE_FILE
        value: /data/companies.db
      - key: FILING_SSE_URL
        value: https://stream.companieshouse.gov.uk/filings
      - key: PSC_SSE_URL
        value: https://stream.companieshouse.gov.uk/persons-with-significant-control
      - key: PSC_STATEMENTS_SSE_URL
        value: https://stream.companieshouse.gov.uk/persons-with-significant-control-statements
    disk:
      name: change-monitor-data
      mountPath: /data
      sizeGB: 1
If the web service and worker use separate Render services, attach the same persistent database storage only if the Render plan supports shared storage. Otherwise use a managed PostgreSQL database and replace the SQLite queries with PostgreSQL queries.
