import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct, col, count, isnan, lit, trim, when
)


spark = SparkSession.builder \
    .appName("ComprehensiveDataProfiling") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

db_url = "jdbc:postgresql://postgres:5432/realestate"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

bronze_tables = [
    "bronze.raw_egypt_inflation_monthly_2025_2026",
    "bronze.raw_egypt_locations",
    "bronze.raw_egypt_real_estate_listings",
    "bronze.raw_listings_stream",
    "bronze.raw_propertyfinder",
    "bronze.raw_usd_egp_monthly",
    "bronze.stg_listings"
]
silver_catalog = spark.read.jdbc(
    url=db_url,
    table=(
        "(SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'silver' AND table_type = 'BASE TABLE') "
        "AS silver_catalog"
    ),
    properties=db_properties
)
tables = bronze_tables + [
    f"silver.{row['table_name']}" for row in silver_catalog.collect()
]

key_columns = {
    "bronze.raw_egypt_inflation_monthly_2025_2026": ["year_month"],
    "bronze.raw_egypt_locations": ["city_id"],
    "bronze.raw_listings_stream": ["listing_id"],
    "bronze.raw_propertyfinder": ["listing_id"],
    "bronze.raw_usd_egp_monthly": ["year_month"],
    "bronze.stg_listings": ["listing_id"],
    "silver.egypt_inflation_monthly": ["year_month"],
    "silver.egypt_locations": ["city_id"],
    "silver.egypt_real_estate_listings": ["url"],
    "silver.listings": ["listing_id"],
    "silver.listings_stream": ["listing_id"],
    "silver.propertyfinder_listings": ["listing_id"],
    "silver.usd_egp_monthly": ["year_month"],
    "silver.location": ["location_id"],
    "silver.property": ["property_id"],
    "silver.broker": ["broker_id"],
    "silver.agent": ["agent_id"],
    "silver.listing_amenity": ["listing_id", "amenity"],
    "silver.listing": ["listing_id"]
}

report_file = "/app/profiling_report.txt"
original_stdout = sys.stdout
try:
    with open(report_file, "w", encoding="utf-8") as report:
        sys.stdout = report
        for table in tables:
            print(f"\n{'=' * 60}")
            print(f" Comprehensive Profiling for Table: {table} ")
            print(f"{'=' * 60}")

            try:
                profile_table = table
                if table == "bronze.raw_egypt_real_estate_listings":
                    profile_table = (
                        '(SELECT "    url" AS url, price, description, location, '
                        'type, size, bedrooms, bathrooms, available_from, '
                        'payment_method, down_payment '
                        'FROM bronze.raw_egypt_real_estate_listings) '
                        'AS raw_egypt_real_estate_listings'
                    )
                df = spark.read.jdbc(
                    url=db_url, table=profile_table, properties=db_properties
                )
                total_rows = df.count()
                print(f"Total Rows: {total_rows}")
                print(f"Total Columns: {len(df.columns)}")

                if total_rows <= 100000:
                    distinct_rows = df.dropDuplicates().count()
                    duplicate_rows = total_rows - distinct_rows
                    duplicate_pct = (
                        duplicate_rows / total_rows * 100 if total_rows else 0
                    )
                    print(
                        f"Exact Duplicate Rows: {duplicate_rows} "
                        f"({duplicate_pct:.2f}% of data)"
                    )
                else:
                    print(
                        "Exact Duplicate Rows: skipped for large table; "
                        "business-key duplicates are checked below"
                    )

                table_keys = key_columns.get(table, [])
                if table == "bronze.raw_egypt_real_estate_listings":
                    table_keys = [
                        column_name for column_name in df.columns
                        if column_name.strip() == "url"
                    ][:1]
                if table_keys and all(key in df.columns for key in table_keys):
                    has_null_key = col(table_keys[0]).isNull()
                    for key in table_keys[1:]:
                        has_null_key = has_null_key | col(key).isNull()
                    keyed_rows = df.filter(~has_null_key)
                    null_key_rows = df.filter(has_null_key).count()
                    distinct_keys = keyed_rows.select(*table_keys).dropDuplicates().count()
                    duplicate_key_rows = keyed_rows.count() - distinct_keys
                    print(f"Business Key: {', '.join(table_keys)}")
                    print(f"Null Business Key Rows: {null_key_rows}")
                    print(f"Duplicate Business Key Rows: {duplicate_key_rows}")

                print("\n--- Schema ---")
                df.printSchema()

                print("\n--- Missing Values (Nulls / NaNs / Blank Strings) ---")
                missing_exprs = []
                for column_name, data_type in df.dtypes:
                    value = col(column_name)
                    missing = value.isNull()
                    if data_type in ("double", "float"):
                        missing = missing | isnan(value)
                    elif data_type == "string":
                        missing = missing | (trim(value) == "")
                    missing_exprs.append(
                        count(when(missing, lit(1))).alias(column_name)
                    )
                df.select(missing_exprs).show(vertical=True)

                print("\n--- Approximate Distinct Values Count ---")
                distinct_exprs = [
                    approx_count_distinct(col(column_name)).alias(column_name)
                    for column_name in df.columns
                ]
                df.select(distinct_exprs).show(vertical=True)

                quality_exprs = []
                price_column = "price_egp" if "price_egp" in df.columns else "price"
                if table.startswith("silver.") and price_column in df.columns:
                    quality_exprs.append(
                        count(
                            when(
                                col(price_column).isNull() | (col(price_column) <= 0),
                                lit(1)
                            )
                        ).alias("invalid_or_missing_price_rows")
                    )
                if table.startswith("silver.") and "area_value" in df.columns:
                    quality_exprs.append(
                        count(
                            when(
                                col("area_value").isNotNull() & (col("area_value") <= 0),
                                lit(1)
                            )
                        ).alias("invalid_area_rows")
                    )
                for column_name, lower_bound, upper_bound in [
                    ("lat", -90.0, 90.0),
                    ("lon", -180.0, 180.0)
                ]:
                    if table.startswith("silver.") and column_name in df.columns:
                        quality_exprs.append(
                            count(
                                when(
                                    col(column_name).isNotNull()
                                    & ~col(column_name).between(lower_bound, upper_bound),
                                    lit(1)
                                )
                            ).alias(f"invalid_{column_name}_rows")
                        )
                if quality_exprs:
                    print("\n--- Silver Rule Violations ---")
                    df.agg(*quality_exprs).show(vertical=True)

                numeric_columns = [
                    column_name
                    for column_name, data_type in df.dtypes
                    if data_type in ("byte", "short", "integer", "long", "float", "double")
                    or data_type.startswith("decimal")
                ]
                print("\n--- Numeric Summary Statistics ---")
                if numeric_columns:
                    df.select(*numeric_columns).summary().show()
                else:
                    print("No numeric columns")
            except Exception as error:
                print(f"Failed to profile {table}. Error: {error}")
finally:
    sys.stdout = original_stdout
    spark.stop()

with open(report_file, "r", encoding="utf-8") as report:
    report_lines = [line.rstrip() for line in report]
with open(report_file, "w", encoding="utf-8", newline="") as report:
    report.write("\n".join(report_lines) + "\n")

print(f"\nProfiling is complete. Open '{report_file}' for the full report.")
