import geohash as gh
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, year, month, dayofmonth
from pyspark.sql.types import StringType

# ── Config ────────────────────────────────────────────────────────────────────
RESTAURANT_INPUT = "/home/ubuntu/data/output/restaurant_geohashed/"
WEATHER_INPUT    = "/home/ubuntu/data/weather/"
OUTPUT_DIR       = "/home/ubuntu/data/output/restaurant_weather_joined"
GEOHASH_PRECISION = 4

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RestaurantWeatherJoinETL")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Geohash UDF ───────────────────────────────────────────────────────────────
def generate_geohash(lat: float, lng: float) -> str:
    if lat is None or lng is None:
        return None
    try:
        return gh.encode(lat, lng, precision=GEOHASH_PRECISION)
    except Exception:
        return None

geohash_udf = udf(generate_geohash, StringType())

# ── Extract ───────────────────────────────────────────────────────────────────
df_restaurant = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(RESTAURANT_INPUT)
)
print(f"✔ Restaurant rows loaded: {df_restaurant.count()}")

# Read weather — Spark automatically picks up year/month/day partition columns
df_weather = spark.read.parquet(WEATHER_INPUT)
print(f"✔ Weather rows loaded: {df_weather.count()}")

# ── Transform ─────────────────────────────────────────────────────────────────

# 1. Generate geohash for weather data from its lat/lng
df_weather = df_weather.withColumn(
    "geohash",
    geohash_udf(col("lat"), col("lng"))
)

# 2. Deduplicate weather by geohash + date to avoid data multiplication on join.
#    We keep one representative row per (geohash, wthr_date) — avg temperatures
#    are already aggregated so we just drop duplicates on the join key.
df_weather_deduped = df_weather.dropDuplicates(["geohash", "wthr_date"])
print(f"✔ Weather rows after deduplication: {df_weather_deduped.count()}")

# 3. Rename weather lat/lng to avoid column name collision with restaurant lat/lng
df_weather_deduped = (
    df_weather_deduped
    .withColumnRenamed("lat", "wthr_lat")
    .withColumnRenamed("lng", "wthr_lng")
)

# 4. Left-join restaurant with weather on geohash.
df_joined = df_restaurant.join(
    df_weather_deduped,
    on="geohash",
    how="left"
)

# 5. Add year/month/day partition columns derived from wthr_date (format: YYYY-MM-DD).
#    Falls back to weather partition columns if wthr_date is null.
df_joined = (
    df_joined
    .withColumn("year",  year(col("wthr_date").cast("date")))
    .withColumn("month", month(col("wthr_date").cast("date")))
    .withColumn("day",   dayofmonth(col("wthr_date").cast("date")))
)

print(f"✔ Joined dataset row count: {df_joined.count()}")
df_joined.printSchema()
df_joined.show(10, truncate=False)

# ── Idempotency ───────────────────────────────────────────────────────────────
# Using mode("overwrite") with partitionBy ensures the job can be safely re-run.
# Spark will overwrite only the affected partitions, not the entire output.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# ── Load — partitioned Parquet ────────────────────────────────────────────────
(
    df_joined
    .write
    .mode("overwrite")
    .partitionBy("year", "month", "day")
    .parquet(OUTPUT_DIR)
)

print(f"✔ Output written to: {OUTPUT_DIR}")
print(f"  Partitioned by: year / month / day")
spark.stop()
