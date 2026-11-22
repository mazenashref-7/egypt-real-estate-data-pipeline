from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, initcap, lower, current_timestamp,
    regexp_replace, when, coalesce, to_timestamp, date_format, format_string, floor
)
from pyspark.sql.types import DoubleType, IntegerType, BooleanType, StringType
from _common import deduplicate_by_key

spark = SparkSession.builder \
    .appName("02_CleanPropertyFinder") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

db_url = "jdbc:postgresql://postgres:5432/realestate"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

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
ID_PHONE_COLS = ["internal_id", "agent_id", "broker_id", "broker_phone", "contact_phone", "contact_whatsapp"]

df = spark.read.jdbc(db_url, "bronze.raw_propertyfinder", properties=db_properties)
initial_count = df.count()

for c in ID_PHONE_COLS:
    if c in df.columns:
        if isinstance(df.schema[c].dataType, DoubleType):
            df = df.withColumn(
                c,
                when(col(c).isNull(), None).otherwise(format_string("%.0f", col(c)))
            )
        else:
            df = df.withColumn(c, regexp_replace(col(c).cast(StringType()), r"\.0$", ""))

for field in df.schema.fields:
    c = field.name
    if isinstance(field.dataType, StringType):
        df = df.withColumn(c, trim(col(c)))
        df = df.withColumn(c, when(col(c) == "", None).otherwise(col(c)))
        if not any(k in c.lower() for k in SKIP_INITCAP):
            df = df.withColumn(c, initcap(col(c)))

if "listing_id" in df.columns:
    df = df.filter(col("listing_id").isNotNull())

for c in ["price_egp", "area_value", "lat", "lon"]:
    if c in df.columns:
        cleaned = regexp_replace(col(c).cast(StringType()), "[^0-9.-]", "")
        df = df.withColumn(c, when(cleaned == "", None).otherwise(cleaned).cast(DoubleType()))

for c in INT_COLS:
    if c in df.columns:
        cleaned = regexp_replace(col(c).cast(StringType()), "[^0-9.-]", "")
        parsed = cleaned.cast(DoubleType())
        df = df.withColumn(
            c,
            when(
                parsed.isNotNull() & (parsed >= 0) & (parsed == floor(parsed)),
                parsed.cast(IntegerType())
            ).otherwise(None)
        )

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

for date_c in ["listed_date", "scraped_at"]:
    if date_c in df.columns:
        raw_date = col(date_c)
        cleaned_date = trim(
            regexp_replace(lower(raw_date.cast(StringType())), "z$", "")
        )
        parsed_date = coalesce(
            to_timestamp(cleaned_date, "yyyy-MM-dd't'HH:mm:ss.SSSSSS"),
            to_timestamp(cleaned_date, "yyyy-MM-dd't'HH:mm:ss.SSS"),
            to_timestamp(cleaned_date, "yyyy-MM-dd't'HH:mm:ss"),
            to_timestamp(cleaned_date, "yyyy-MM-dd HH:mm:ss.SSSSSS"),
            to_timestamp(cleaned_date, "yyyy-MM-dd HH:mm:ss.SSS"),
            to_timestamp(cleaned_date, "yyyy-MM-dd HH:mm:ss"),
            to_timestamp(cleaned_date, "yyyy-MM-dd"),
            to_timestamp(cleaned_date, "d/M/yyyy")
        )
        df = df.withColumn(date_c, parsed_date)

ref_ts = coalesce(
    col("listed_date") if "listed_date" in df.columns else None,
    col("scraped_at") if "scraped_at" in df.columns else None
)
df = df.withColumn("listing_year_month", date_format(ref_ts, "yyyy-MM"))

if "price_egp" in df.columns:
    df = df.filter(col("price_egp").isNotNull() & (col("price_egp") > 0))
if "area_value" in df.columns:
    df = df.filter(col("area_value").isNull() | (col("area_value") > 0))

for c, lower_bound, upper_bound in [
    ("lat", -90.0, 90.0),
    ("lon", -180.0, 180.0)
]:
    if c in df.columns:
        df = df.withColumn(
            c,
            when(col(c).between(lower_bound, upper_bound), col(c)).otherwise(None)
        )

df = deduplicate_by_key(df, ["listing_id"], ["scraped_at", "listed_date"])
df = df.withColumn("_silver_processed_at", current_timestamp()).cache()
final_count = df.count()

df.write.jdbc(db_url, "silver.propertyfinder_listings", mode="overwrite", properties=db_properties)
df.unpersist()
spark.stop()
print(f"✅ Done silver.propertyfinder_listings: {initial_count} -> {final_count}")