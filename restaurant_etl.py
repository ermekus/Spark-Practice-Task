import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name

# ── Config ────────────────────────────────────────────────────────────────────
INPUT_DIR  = "/home/ubuntu/data/"   
OUTPUT_DIR = "/home/ubuntu/data/output/restaurant_combined.csv"

# ── Spark Session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("RestaurantETL")
    .master("local[*]")               # use all available CPU cores on EC2
    .config("spark.sql.shuffle.partitions", "4")  # keep it light for local mode
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ── Extract ───────────────────────────────────────────────────────────────────
# Read all CSVs in the directory at once — Spark handles the union automatically
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_DIR)
)

print(f"✔ Total rows loaded: {df.count()}")
print(f"✔ Schema:")
df.printSchema()

# ── Transform ─────────────────────────────────────────────────────────────────
# Drop exact duplicate rows (same restaurant appearing in multiple files)
df_clean = df.dropDuplicates()

# drop rows where critical fields are null
df_clean = df_clean.dropna(subset=["id", "franchise_id", "country", "city"])

print(f"✔ Rows after deduplication & cleaning: {df_clean.count()}")
df_clean.show(10, truncate=False)

# ── Load ──────────────────────────────────────────────────────────────────────
# Write combined dataset as a single CSV file to local filesystem
(
    df_clean
    .coalesce(1)                        # merge into one output file
    .write
    .mode("overwrite")
    .option("header", "true")
    .csv(OUTPUT_DIR)
)

print(f"✔ Output written to: {OUTPUT_DIR}")
spark.stop()