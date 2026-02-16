# NYC TLC Taxi Trips — ETL Pipeline (Docker + PostgreSQL)

An end-to-end ETL pipeline using NYC TLC Yellow Taxi trip records:
- **Ingest** monthly trip data + zone lookup
- **Clean & validate**
- **Model** analytics-ready tables (fact + dimension)
- **Load** into PostgreSQL
- **Validate** with post-load checks & sample analytics

## Repo structure
- `notebooks/` — the project notebook (explain + explore)
- `src/etl.py` — runnable ETL entrypoint (what Docker executes)
- `docker-compose.yml` — starts Postgres + ETL runner
- `.env.example` — environment variables template

## Quickstart (Docker) — recommended
### 1) Install Docker Desktop
- Windows/macOS: install Docker Desktop and ensure it is running.

### 2) Get the code
```bash
git clone <YOUR_GITHUB_REPO_URL>
cd nyc-tlc-taxi-etl-pipeline
```

### 3) Create your `.env`
```bash
cp .env.example .env
```
Open `.env` and set:
- `PG_PASSWORD` (choose any password you like)
- `TRIPS_MONTH` (example: `2023-01`)

### 4) Run the pipeline
```bash
docker compose up --build
```
What you should see:
- Postgres starts
- ETL downloads the selected month + zone lookup (first run)
- Tables are created and loaded
- Post-load checks are printed

### 5) Re-run only the ETL (after the first run)
```bash
docker compose run --rm etl
```

### 6) Stop services
```bash
docker compose down
```

## How to verify it worked
### Option A: Inspect logs
The ETL prints:
- row counts
- null checks
- example aggregations (top zones, payment types, etc.)

### Option B: Connect to Postgres
Using `psql` (optional):
```bash
docker exec -it nyc_taxi_pg psql -U postgres -d taxi_db
```
Example queries:
```sql
\dt
SELECT COUNT(*) FROM fact_taxi_trips;
SELECT * FROM dim_taxi_zones LIMIT 5;
```

## Local (non-Docker) run (optional)
```bash
python -m venv .venv
# Windows:
.\.venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
python -m src.etl
```

## Notes
- **Do not commit `.env`**. Only commit `.env.example`.
- Data is downloaded from official NYC TLC sources (AWS CloudFront mirror).
