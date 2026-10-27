import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_PATH = PROJECT_ROOT / "data" / "raw"

url = URL.create(
    "postgresql+psycopg2",
    username=os.getenv("PG_USER", "postgres"),
    password=os.getenv("PG_PASSWORD", "postgres"),
    host=os.getenv("PG_HOST", "localhost"),
    port=int(os.getenv("PG_PORT", "5434")),        
    database=os.getenv("PG_DB", "realestate"),       
)

engine = create_engine(url)

tables = {
    "raw_propertyfinder": RAW_PATH / "propertyfinder.csv",
    "raw_egypt_real_estate_listings": RAW_PATH / "egypt_real_estate_listings.csv",
    "raw_usd_egp_monthly": RAW_PATH / "usd_egp_monthly.csv",
    "raw_egypt_inflation_monthly_2025_2026": RAW_PATH / "egypt_inflation_monthly_2025_2026.csv",
    "raw_egypt_locations": RAW_PATH / "egypt_locations.csv",
}

with engine.begin() as con:
    con.execute(text("CREATE SCHEMA IF NOT EXISTS bronze"))

for table_name, csv_path in tables.items():
    df = pd.read_csv(csv_path)
    df.to_sql(
        table_name,
        engine,
        schema="bronze",
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=1000,
    )

    with engine.connect() as con:
        count = con.execute(text(f"SELECT COUNT(*) FROM bronze.{table_name}")).scalar()
    print(f"Loaded bronze.{table_name}: {count} rows")

engine.dispose()
print("Bronze load complete")