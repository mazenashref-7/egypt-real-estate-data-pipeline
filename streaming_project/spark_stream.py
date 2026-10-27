import csv
from pyspark.sql import SparkSession, functions as F

URL = "jdbc:postgresql://postgres:5432/realestate"   
USER, PWD = "postgres", "postgres"

spark = (SparkSession.builder
         .appName("listings_stream")
         .config("spark.jars.packages",
                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3")
         .config("spark.jars.ivy", "/tmp/ivy")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

with open("/app/listings.csv", encoding="utf-8-sig") as f:
    cols = next(csv.reader(f))
schema   = ", ".join(f"`{c}` STRING" for c in cols)
col_list = ", ".join(f'"{c}"' for c in cols)

def run_sql(sql):
    jvm = spark._jvm
    loader = jvm.Thread.currentThread().getContextClassLoader()
    driver = loader.loadClass("org.postgresql.Driver").newInstance()
    props = jvm.java.util.Properties()
    props.setProperty("user", USER)
    props.setProperty("password", PWD)
    conn = driver.connect(URL, props)
    try:
        st = conn.createStatement()
        st.execute(sql)
        st.close()
    finally:
        conn.close()

col_defs = ", ".join(
    f'"{c}" TEXT PRIMARY KEY' if c == "listing_id" else f'"{c}" TEXT'
    for c in cols
)
run_sql(f'CREATE TABLE IF NOT EXISTS bronze.raw_listings_stream '
        f'({col_defs}, "_ingested_at" TIMESTAMP)')

raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", "kafka:29092")
       .option("subscribe", "listings_raw")
       .option("startingOffsets", "earliest")
       .option("maxOffsetsPerTrigger", 5000)       
       .load())

parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
             .select("d.*")
             .filter(F.col("listing_id").isNotNull()))  

def write_batch(df, batch_id):
    n = df.count()
    print(f"batch {batch_id}: received {n} rows", flush=True)
    if n == 0:
        return

    (df.withColumn("_ingested_at", F.current_timestamp())
       .write.format("jdbc")
       .option("url", URL).option("dbtable", "bronze.stg_listings")
       .option("user", USER).option("password", PWD)
       .option("driver", "org.postgresql.Driver")
       .mode("overwrite").save())
    run_sql(f'''
        INSERT INTO bronze.raw_listings_stream ({col_list}, "_ingested_at")
        SELECT DISTINCT ON ("listing_id") {col_list}, "_ingested_at"
        FROM bronze.stg_listings
        ON CONFLICT ("listing_id") DO NOTHING
    ''')

query = (parsed.writeStream
         .foreachBatch(write_batch)
         .option("checkpointLocation", "/app/checkpoints/listings")
         .trigger(processingTime="10 seconds")
         .start())
query.awaitTermination()