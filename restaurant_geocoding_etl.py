import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, DoubleType

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_DIR   = "/home/ubuntu/data/output/restaurant_combined.csv"
OUTPUT_DIR  = "/home/ubuntu/data/output/restaurant_geocoded"
API_KEY     = "2e61cac148e5472b8d5f07ac430733a5"   
API_URL     = "https://api.opencagedata.com/geocode/v1/json"

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RestaurantGeocodingETL")
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

total = df.count()
print(f"✔ Total rows loaded: {total}")
df.printSchema()

# ── Inspect nulls ─────────────────────────────────────────────────────────────
null_count = df.filter(df.lat.isNull() | df.lng.isNull()).count()
print(f"✔ Rows with missing lat/lng: {null_count}")

if null_count == 0:
    print("✔ No missing coordinates found. No geocoding needed.")
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(OUTPUT_DIR)
    spark.stop()
    exit(0)

# ── Geocoding via OpenCage REST API ───────────────────────────────────────────
def geocode(franchise_name: str, city: str, country: str):
    """
    Call OpenCage API with 'franchise_name, city, country' query.
    Returns (lat, lng) tuple or (None, None) on failure.
    """
    if not city and not country:
        return (None, None)
    query = ", ".join(filter(None, [franchise_name, city, country]))
    try:
        response = requests.get(
            API_URL,
            params={"q": query, "key": API_KEY, "limit": 1, "no_annotations": 1},
            timeout=10
        )
        data = response.json()
        if data["total_results"] > 0:
            geometry = data["results"][0]["geometry"]
            return (float(geometry["lat"]), float(geometry["lng"]))
    except Exception as e:
        print(f"  ✘ Geocoding failed for '{query}': {e}")
    return (None, None)

# Register as a Spark UDF returning struct(lat, lng)
geocode_schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lng", DoubleType(), True),
])
geocode_udf = udf(geocode, geocode_schema)


# ── Transform ─────────────────────────────────────────────────────────────────
# Split rows into those that need geocoding and those that don't
df_ok      = df.filter(df.lat.isNotNull() & df.lng.isNotNull())
df_missing = df.filter(df.lat.isNull() | df.lng.isNull())

print(f"  → Rows to geocode: {df_missing.count()}")

# Apply geocoding UDF only to rows with missing coords
df_geocoded = (
    df_missing
    .withColumn("_geo", geocode_udf(col("franchise_name"), col("city"), col("country")))
    .withColumn("lat",  col("_geo.lat"))
    .withColumn("lng",  col("_geo.lng"))
    .drop("_geo")
)

# Show what was resolved vs still missing
resolved   = df_geocoded.filter(df_geocoded.lat.isNotNull() & df_geocoded.lng.isNotNull()).count()
unresolved = df_geocoded.filter(df_geocoded.lat.isNull() | df_geocoded.lng.isNull()).count()
print(f"  → Successfully geocoded: {resolved}")
print(f"  → Still unresolved after API call: {unresolved}")

df_geocoded.show(20, truncate=False)

# Combine enriched rows back with the clean rows
df_final = df_ok.unionByName(df_geocoded)
print(f"✔ Final dataset row count: {df_final.count()}")

# ── Load ──────────────────────────────────────────────────────────────────────
(
    df_final
    .coalesce(1)
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(OUTPUT_DIR)
)

print(f"✔ Output written to: {OUTPUT_DIR}")
spark.stop()
