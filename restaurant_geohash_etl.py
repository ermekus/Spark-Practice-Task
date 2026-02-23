import geohash
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_DIR  = "/home/ubuntu/data/output/restaurant_geocoded"   
OUTPUT_DIR = "/home/ubuntu/data/output/restaurant_geohashed"
GEOHASH_PRECISION = 4                                 

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RestaurantGeohashETL")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Extract ───────────────────────────────────────────────────────────────────
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_DIR)
)

print(f"✔ Total rows loaded: {df.count()}")
df.printSchema()

# ── Transform — Geohash UDF ───────────────────────────────────────────────────
def generate_geohash(lat: float, lng: float) -> str:
    """
    Encode latitude and longitude into a 4-character geohash.    
    """
    if lat is None or lng is None:
        return None
    try:
        return geohash.encode(lat, lng, precision=GEOHASH_PRECISION)
    except Exception as e:
        print(f"  ✘ Geohash failed for ({lat}, {lng}): {e}")
        return None

geohash_udf = udf(generate_geohash, StringType())

df_with_geohash = df.withColumn(
    "geohash",
    geohash_udf(col("lat").cast("double"), col("lng").cast("double"))
)

# Report any rows where geohash could not be generated
missing = df_with_geohash.filter(col("geohash").isNull()).count()
print(f"✔ Rows with geohash generated: {df_with_geohash.filter(col('geohash').isNotNull()).count()}")
print(f"✔ Rows without geohash (null lat/lng): {missing}")

df_with_geohash.show(20, truncate=False)

# ── Load ──────────────────────────────────────────────────────────────────────
(
    df_with_geohash
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(OUTPUT_DIR)
)

print(f"✔ Output written to: {OUTPUT_DIR}")
spark.stop()
