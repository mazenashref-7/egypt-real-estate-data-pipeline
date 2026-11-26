import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, trim, initcap, lower, current_timestamp,
    regexp_replace, when, coalesce, to_timestamp, date_format,
    max as spark_max
)
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, StringType

spark = SparkSession.builder \
    .appName("01_CleanListingsStream") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .config("spark.sql.shuffle.partitions", "16") \
    .getOrCreate()

db_url = "jdbc:postgresql://postgres:5432/realestate"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

BRONZE_TABLE = "bronze.raw_listings_stream"
SILVER_TABLE = "silver.listings_stream"

SKIP_INITCAP = [
    "url", "link", "image", "video", "description", "email",
    "id", "date", "month", "phone", "whatsapp", "reference", "code", "rera"
]
BOOL_COLS = [
    "is_premium", "is_verified", "is_featured",
    "is_new_construction", "is_direct_from_dev", "is_exclusive",
    "has_view_360", "agent_is_super"
]
INT_COLS = ["bedrooms", "bathrooms", "images_count"]

# ---------------------------------------------------------------
# Incremental logic: find the last _ingested_at already in silver
# ---------------------------------------------------------------
try:
    last_ts = (spark.read.jdbc(db_url, SILVER_TABLE, properties=db_properties)
               .agg(spark_max("_ingested_at")).collect()[0][0])
except Exception:
    last_ts = None  # silver table does not exist yet -> initial load

df = spark.read.jdbc(db_url, BRONZE_TABLE, properties=db_properties)

if last_ts is None:
    write_mode = "overwrite"   # initial load
    print("Mode: INITIAL load (full)")
else:
    write_mode = "append"      # incremental load
    df = df.filter(col("_ingested_at") > lit(last_ts))
    print(f"Mode: INCREMENTAL load (rows after {last_ts})")

df = df.cache()
initial_count = df.count()

if initial_count == 0:
    print("No new rows, nothing to do")
    df.unpersist()
    spark.stop()
    sys.exit(0)

# ---------------------------------------------------------------
# Cleaning (unchanged)
# ---------------------------------------------------------------
if "listing_id" in df.columns:
    df = df.dropDuplicates(["listing_id"])

for field in df.schema.fields:
    c = field.name
    if isinstance(field.dataType, StringType):
        df = df.withColumn(c, trim(col(c)))
        df = df.withColumn(c, when(col(c) == "", None).otherwise(col(c)))
        if not any(k in c.lower() for k in SKIP_INITCAP):
            df = df.withColumn(c, initcap(col(c)))

for c in ["price_egp", "area_value", "lat", "lon"]:
    if c in df.columns:
        cleaned = regexp_replace(col(c).cast(StringType()), "[^0-9.-]", "")
        df = df.withColumn(c, when(cleaned == "", None).otherwise(cleaned).cast(DoubleType()))

for c in INT_COLS:
    if c in df.columns:
        cleaned = regexp_replace(col(c).cast(StringType()), "[^0-9-]", "")
        df = df.withColumn(c, when(cleaned == "", None).otherwise(cleaned).cast(IntegerType()))

for c in BOOL_COLS:
    if c in df.columns:
        v = lower(trim(col(c).cast(StringType())))
        df = df.withColumn(
            c,
            when(v.isin("true", "1", "yes", "y", "t"), True)
            .when(v.isin("false", "0", "no", "n", "f"), False)
            .otherwise(None)
            .cast(BooleanType())
        )

for c in ["price_currency", "price_period", "area_unit"]:
    if c in df.columns:
        df = df.withColumn(c, lower(trim(col(c).cast(StringType()))))
        df = df.withColumn(c, when(col(c) == "", None).otherwise(col(c)))

if "price_egp" in df.columns and "price_currency" in df.columns:
    df = df.withColumn(
        "price_currency",
        when(col("price_currency").isNull() & col("price_egp").isNotNull(), "egp")
        .otherwise(col("price_currency"))
    )

if "scraped_at" in df.columns:
    cleaned_scraped = trim(regexp_replace(lower(col("scraped_at").cast(StringType())), "z$", ""))
    df = df.withColumn(
        "scraped_at",
        coalesce(
            to_timestamp(cleaned_scraped, "yyyy-MM-dd't'HH:mm:ss.SSSSSS"),
            to_timestamp(cleaned_scraped, "yyyy-MM-dd't'HH:mm:ss.SSS"),
            to_timestamp(cleaned_scraped, "yyyy-MM-dd't'HH:mm:ss"),
            to_timestamp(cleaned_scraped, "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(col("scraped_at"))
        )
    )

listed_ts = None
if "listed_date" in df.columns:
    cleaned_listed = trim(regexp_replace(lower(col("listed_date").cast(StringType())), "z$", ""))
    listed_ts = coalesce(
        to_timestamp(cleaned_listed, "yyyy-MM-dd't'HH:mm:ss.SSSSSS"),
        to_timestamp(cleaned_listed, "yyyy-MM-dd't'HH:mm:ss.SSS"),
        to_timestamp(cleaned_listed, "yyyy-MM-dd't'HH:mm:ss"),
        to_timestamp(cleaned_listed, "yyyy-MM-dd"),
        to_timestamp(col("listed_date"))
    )

ref_ts = coalesce(
    col("scraped_at") if "scraped_at" in df.columns else None,
    listed_ts,
    to_timestamp(col("_ingested_at")) if "_ingested_at" in df.columns else None
)
df = df.withColumn("listing_year_month", date_format(ref_ts, "yyyy-MM"))

if "price_egp" in df.columns:
    df = df.filter(col("price_egp").isNotNull() & (col("price_egp") > 0))
if "area_value" in df.columns:
    df = df.filter(col("area_value").isNull() | (col("area_value") > 0))

df = df.withColumn("_silver_processed_at", current_timestamp()).cache()
final_count = df.count()

# ---------------------------------------------------------------
# Write: overwrite on first run, append afterwards
# ---------------------------------------------------------------
df.write.jdbc(db_url, SILVER_TABLE, mode=write_mode, properties=db_properties)
df.unpersist()
spark.stop()
print(f"✅ Done {SILVER_TABLE} ({write_mode}): {initial_count} -> {final_count}")