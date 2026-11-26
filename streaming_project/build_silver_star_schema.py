from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, lit, md5, concat_ws, coalesce, lower, trim, row_number,
    split, explode, regexp_replace, to_timestamp, current_timestamp,date_format
)

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


def read(table, source):
    df = spark.read.jdbc(db_url, table, properties=db_properties).withColumn("source", lit(source))
    for c in ["listed_date", "scraped_at"]:
        df = df.withColumn(c, to_timestamp(col(c)))
    return df


def write(df, table):
    df = df.withColumn("_silver_processed_at", current_timestamp())
    df.write.jdbc(db_url, table, mode="overwrite", properties=db_properties)


def n(c):
    return coalesce(lower(trim(col(c))), lit("~"))


df = read("silver.propertyfinder_listings", "propertyfinder") \
    .unionByName(read("silver.listings_stream", "listings_stream"), allowMissingColumns=True)
df = df.withColumn("completion_status", coalesce(col("completion_status"), col("completion_stats"))).drop("completion_stats")
for c in ["agent_id", "broker_id"]:
    df = df.withColumn(c, regexp_replace(col(c).cast("string"), r"\.0$", ""))

w = Window.partitionBy("listing_id").orderBy(col("scraped_at").desc_nulls_last())
df = df.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

df = df.withColumn("location_id", md5(concat_ws("|", n("city"), n("town"), n("district"), n("subdistrict")))) \
       .withColumn("property_id", md5(concat_ws("|", n("category"), n("property_type"), n("offering_type")))) \
       .cache()

location = df.select("location_id", "city", "town", "district", "subdistrict", "lat", "lon") \
    .dropDuplicates(["location_id"])

property_df = df.select("property_id", "category", "property_type", "offering_type") \
    .dropDuplicates(["property_id"])

agent = df.filter(col("agent_id").isNotNull()) \
    .select("agent_id", "agent_name", "agent_email", "broker_id") \
    .dropDuplicates(["agent_id"])

broker = df.filter(col("broker_id").isNotNull()) \
    .select("broker_id", "broker_name", "broker_email", "broker_phone") \
    .dropDuplicates(["broker_id"])

listing_amenity = df.select("listing_id", explode(split(col("amenities"), ",")).alias("amenity")) \
    .withColumn("amenity", lower(trim(col("amenity")))) \
    .filter(col("amenity") != "") \
    .dropDuplicates(["listing_id", "amenity"])

listing = df.drop("city", "town", "district", "subdistrict", "category", "property_type",
                  "offering_type", "agent_name", "agent_email", "broker_id", "broker_name",
                  "broker_email", "broker_phone", "amenities",
                  "price_currency", "area_unit", "listing_year_month") \
            .withColumn("year_month", date_format(col("listed_date"), "yyyy-MM"))

write(location, "silver.location")
write(property_df, "silver.property")
write(broker, "silver.broker")
write(agent, "silver.agent")
write(listing_amenity, "silver.listing_amenity")
write(listing, "silver.listing")

df.unpersist()
spark.stop()
print("✅ Done star schema")