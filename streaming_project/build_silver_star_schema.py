from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, md5, concat_ws, coalesce, lower, trim, split, explode,
    regexp_replace, to_timestamp, current_timestamp, date_format
)

from silver_cleaners._common import deduplicate_by_key


spark = SparkSession.builder \
    .appName("07_BuildSilverStarSchema") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

db_url = "jdbc:postgresql://postgres:5432/realestate"
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}


def parse_timestamp(column_name):
    value = col(column_name)
    normalized = trim(regexp_replace(lower(value.cast("string")), "z$", ""))
    return coalesce(
        to_timestamp(normalized, "yyyy-MM-dd't'HH:mm:ss.SSSSSS"),
        to_timestamp(normalized, "yyyy-MM-dd't'HH:mm:ss.SSS"),
        to_timestamp(normalized, "yyyy-MM-dd't'HH:mm:ss"),
        to_timestamp(normalized, "yyyy-MM-dd HH:mm:ss.SSSSSS"),
        to_timestamp(normalized, "yyyy-MM-dd HH:mm:ss.SSS"),
        to_timestamp(normalized, "yyyy-MM-dd HH:mm:ss"),
        to_timestamp(normalized, "yyyy-MM-dd"),
        to_timestamp(normalized, "d/M/yyyy"),
    )


def read(table, source):
    df = spark.read.jdbc(db_url, table, properties=db_properties)
    for column_name in ["listed_date", "scraped_at"]:
        if column_name in df.columns:
            df = df.withColumn(column_name, parse_timestamp(column_name))
    return df.withColumn("source", lit(source))


def write(df, table):
    (
        df.withColumn("_silver_processed_at", current_timestamp())
        .write.jdbc(db_url, table, mode="overwrite", properties=db_properties)
    )


def normalized(column_name):
    return coalesce(lower(trim(col(column_name))), lit("~"))


df = read("silver.propertyfinder_listings", "propertyfinder") \
    .unionByName(
        read("silver.listings_stream", "listings_stream"),
        allowMissingColumns=True
    )

for column_name in ["agent_id", "broker_id"]:
    if column_name in df.columns:
        df = df.withColumn(
            column_name,
            regexp_replace(col(column_name).cast("string"), r"\.0$", "")
        )

df = df.persist(StorageLevel.DISK_ONLY)
listing_count = df.count()

df = df.withColumn(
    "location_id",
    md5(concat_ws("|", normalized("city"), normalized("town"),
                  normalized("district"), normalized("subdistrict")))
).withColumn(
    "property_id",
    md5(concat_ws("|", normalized("category"), normalized("property_type"),
                  normalized("offering_type")))
)

location_candidates = df.select(
    "location_id", "city", "town", "district", "subdistrict", "lat", "lon",
    "scraped_at", "_ingested_at"
)
location = deduplicate_by_key(
    location_candidates, ["location_id"], ["scraped_at", "_ingested_at"]
).drop("scraped_at", "_ingested_at")

property_df = df.select(
    "property_id", "category", "property_type", "offering_type"
).dropDuplicates(["property_id"])

agent_candidates = df.filter(col("agent_id").isNotNull()).select(
    "agent_id", "agent_name", "agent_email", "broker_id",
    "scraped_at", "_ingested_at"
)
agent = deduplicate_by_key(
    agent_candidates, ["agent_id"], ["scraped_at", "_ingested_at"]
).drop("scraped_at", "_ingested_at")

broker_candidates = df.filter(col("broker_id").isNotNull()).select(
    "broker_id", "broker_name", "broker_email", "broker_phone",
    "scraped_at", "_ingested_at"
)
broker = deduplicate_by_key(
    broker_candidates, ["broker_id"], ["scraped_at", "_ingested_at"]
).drop("scraped_at", "_ingested_at")

listing_amenity = df.select(
    "listing_id",
    explode(
        split(coalesce(col("amenities"), lit("")), r"[,|]")
    ).alias("amenity")
).withColumn("amenity", lower(trim(col("amenity")))) \
 .filter(col("amenity") != "") \
 .dropDuplicates(["listing_id", "amenity"])

listing = df.drop(
    "city", "town", "district", "subdistrict", "category", "property_type",
    "offering_type", "agent_name", "agent_email", "broker_id", "broker_name",
    "broker_email", "broker_phone", "amenities"
).withColumn(
    "year_month",
    date_format(
        coalesce(col("listed_date"), col("scraped_at"), col("_ingested_at")),
        "yyyy-MM"
    )
)

write(location, "silver.location")
write(property_df, "silver.property")
write(broker, "silver.broker")
write(agent, "silver.agent")
write(listing_amenity, "silver.listing_amenity")
write(listing, "silver.listing")

df.unpersist()
spark.stop()
print(f"✅ Done silver star schema: {listing_count} listings")
