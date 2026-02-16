"""
Runnable ETL entrypoint for the NYC TLC Taxi pipeline.

This script is intentionally "production-ish":
- configuration via environment variables
- idempotent table creation
- basic data quality checks after load

It is designed to be executed via:
  docker compose up --build
or locally:
  python -m src.etl
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import requests
from tqdm import tqdm
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

CLOUDFRONT_BASE = "https://d37ci6vzurychx.cloudfront.net"

def download(url: str, dst: Path, chunk: int = 1024 * 1024) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists() and dst.stat().st_size > 0:
        return
    r = requests.get(url, stream=True, timeout=60)
    r.raise_for_status()
    total = int(r.headers.get("content-length", 0))
    with open(dst, "wb") as f, tqdm(total=total, unit="B", unit_scale=True, desc=f"Downloading {dst.name}") as pbar:
        for part in r.iter_content(chunk_size=chunk):
            if part:
                f.write(part)
                pbar.update(len(part))

@dataclass
class Config:
    pg_user: str
    pg_password: str
    pg_host: str
    pg_port: str
    pg_db: str
    trips_month: str  # YYYY-MM

    @property
    def db_url(self) -> str:
        return f"postgresql+psycopg2://{self.pg_user}:{self.pg_password}@{self.pg_host}:{self.pg_port}/{self.pg_db}"

    @property
    def trips_url(self) -> str:
        # Yellow taxi trip records (Parquet)
        return f"{CLOUDFRONT_BASE}/trip-data/yellow_tripdata_{self.trips_month}.parquet"

    @property
    def zones_url(self) -> str:
        return f"{CLOUDFRONT_BASE}/misc/taxi_zone_lookup.csv"

    @property
    def trips_path(self) -> Path:
        return RAW_DIR / f"yellow_tripdata_{self.trips_month}.parquet"

    @property
    def zones_path(self) -> Path:
        return RAW_DIR / "taxi_zone_lookup.csv"

def load_config() -> Config:
    # Load .env if present (safe in Docker + local runs)
    load_dotenv()

    pg_user = os.getenv("PG_USER")
    pg_password = os.getenv("PG_PASSWORD")
    pg_host = os.getenv("PG_HOST", "localhost")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB")
    trips_month = os.getenv("TRIPS_MONTH", "2023-01")

    missing = [k for k, v in {
        "PG_USER": pg_user,
        "PG_PASSWORD": pg_password,
        "PG_DB": pg_db,
    }.items() if not v]
    if missing:
        raise SystemExit(
            "Missing required environment variables: " + ", ".join(missing) +
            "\nCreate a .env from .env.example and try again."
        )

    return Config(pg_user, pg_password, pg_host, pg_port, pg_db, trips_month)

def basic_clean(trips: pd.DataFrame) -> pd.DataFrame:
    # Standard NYC TLC column set varies by year; use what exists.
    # Minimal universally safe cleaning:
    if "trip_distance" in trips.columns:
        trips = trips[trips["trip_distance"].fillna(0) > 0]
    if "fare_amount" in trips.columns:
        trips = trips[trips["fare_amount"].fillna(0) >= 0]
    # Convert datetimes if present
    for c in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        if c in trips.columns:
            trips[c] = pd.to_datetime(trips[c], errors="coerce")
    return trips

def create_tables(engine) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS dim_taxi_zones (
        locationid INTEGER PRIMARY KEY,
        borough TEXT,
        zone TEXT,
        service_zone TEXT
    );

    CREATE TABLE IF NOT EXISTS fact_taxi_trips (
        -- Keep schema flexible: store key fields + a JSON blob of extras if needed later.
        trip_id BIGSERIAL PRIMARY KEY,
        vendorid INTEGER,
        tpep_pickup_datetime TIMESTAMP,
        tpep_dropoff_datetime TIMESTAMP,
        passenger_count DOUBLE PRECISION,
        trip_distance DOUBLE PRECISION,
        pulocationid INTEGER,
        dolocationid INTEGER,
        fare_amount DOUBLE PRECISION,
        total_amount DOUBLE PRECISION,
        payment_type INTEGER
    );

    CREATE INDEX IF NOT EXISTS idx_fact_pickup_dt ON fact_taxi_trips (tpep_pickup_datetime);
    CREATE INDEX IF NOT EXISTS idx_fact_pu ON fact_taxi_trips (pulocationid);
    CREATE INDEX IF NOT EXISTS idx_fact_do ON fact_taxi_trips (dolocationid);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

def load_dim_zones(engine, zones: pd.DataFrame) -> None:
    zones.columns = [c.strip().lower() for c in zones.columns]
    # Expected columns: LocationID, Borough, Zone, service_zone
    zones = zones.rename(columns={"locationid": "locationid"})
    zones["locationid"] = zones["locationid"].astype(int)
    zones = zones[["locationid", "borough", "zone", "service_zone"]].drop_duplicates()
        # Truncate to keep PK/indexes, then append
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dim_taxi_zones;"))
    zones.to_sql("dim_taxi_zones", engine, if_exists="append", index=False)

def load_fact_trips(engine, trips: pd.DataFrame) -> None:
    # Map to our stable subset of columns if present
    colmap = {
        "VendorID": "vendorid",
        "tpep_pickup_datetime": "tpep_pickup_datetime",
        "tpep_dropoff_datetime": "tpep_dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "PULocationID": "pulocationid",
        "DOLocationID": "dolocationid",
        "fare_amount": "fare_amount",
        "total_amount": "total_amount",
        "payment_type": "payment_type",
    }
    # TLC sometimes uses different case; normalize
    trips_cols = {c.lower(): c for c in trips.columns}
    resolved = {}
    for src, dst in colmap.items():
        # match case-insensitively
        key = src.lower()
        if key in trips_cols:
            resolved[trips_cols[key]] = dst

    fact = trips.rename(columns=resolved)
    keep = [v for v in resolved.values()]
    fact = fact[[c for c in keep if c in fact.columns]].copy()

    # Make loads idempotent per run: truncate then append
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE fact_taxi_trips;"))
    fact.to_sql("fact_taxi_trips", engine, if_exists="append", index=False, method="multi", chunksize=50_000)

def post_load_checks(engine) -> None:
    checks = {
        "fact_rows": "SELECT COUNT(*) FROM fact_taxi_trips;",
        "zones_rows": "SELECT COUNT(*) FROM dim_taxi_zones;",
        "null_pu": "SELECT COUNT(*) FROM fact_taxi_trips WHERE pulocationid IS NULL;",
        "null_pickup_dt": "SELECT COUNT(*) FROM fact_taxi_trips WHERE tpep_pickup_datetime IS NULL;",
        "neg_trip_distance": "SELECT COUNT(*) FROM fact_taxi_trips WHERE trip_distance < 0;",
        "neg_total_amount": "SELECT COUNT(*) FROM fact_taxi_trips WHERE total_amount < 0;",
        "orphan_pu_zones": """
            SELECT COUNT(*)
            FROM fact_taxi_trips f
            LEFT JOIN dim_taxi_zones z ON f.pulocationid = z.locationid
            WHERE z.locationid IS NULL;
        """,
        "top_pu_zones": """
            SELECT z.zone, COUNT(*) AS trips
            FROM fact_taxi_trips f
            LEFT JOIN dim_taxi_zones z ON f.pulocationid = z.locationid
            GROUP BY z.zone
            ORDER BY trips DESC
            LIMIT 10;
        """,
    }
    with engine.begin() as conn:
        print("\n--- Post-load checks ---")
        for name, q in checks.items():
            res = conn.execute(text(q)).fetchall()
            print(f"\n{name}:")
            for row in res:
                print(row)

def main() -> None:
    cfg = load_config()
    print(f"Config: month={cfg.trips_month} db={cfg.pg_user}@{cfg.pg_host}:{cfg.pg_port}/{cfg.pg_db}")

    # Download raw inputs (cached in ./data/raw)
    download(cfg.trips_url, cfg.trips_path)
    download(cfg.zones_url, cfg.zones_path)

    # Load data
    trips = pd.read_parquet(cfg.trips_path)
    zones = pd.read_csv(cfg.zones_path)

    trips = basic_clean(trips)

    engine = create_engine(cfg.db_url, future=True)

    create_tables(engine)
    load_dim_zones(engine, zones)
    load_fact_trips(engine, trips)

    post_load_checks(engine)
    print("\nDone.")

if __name__ == "__main__":
    main()
