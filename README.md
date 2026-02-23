# Spark Practice Task

A PySpark-based ETL pipeline that ingests restaurant data from CSV files, enriches it with geolocation coordinates via the OpenCage Geocoding API, generates geohashes, and joins the result with weather data. The final enriched dataset is stored in partitioned Parquet format.

---

## Table of Contents

- [Architecture](#architecture)
- [Project Structure](#project-structure)
- [Prerequisites & Installation](#prerequisites--installation)
- [Configuration](#configuration)
- [How to Run the Pipeline](#how-to-run-the-pipeline)
- [How to Run Tests](#how-to-run-tests)
- [Output Data Structure](#output-data-structure)

---

## Architecture

The pipeline consists of four sequential ETL jobs:

```
Raw CSV Files (5x)
       │
       ▼
┌─────────────────────┐
│  restaurant_etl     │  Union all CSVs → deduplicate → clean nulls
└─────────────────────┘
       │
       ▼
┌──────────────────────────┐
│  restaurant_geocoding_etl │  Detect null lat/lng → enrich via OpenCage API
└──────────────────────────┘
       │
       ▼
┌─────────────────────────┐
│  restaurant_geohash_etl  │  Generate 4-character geohash from lat/lng
└─────────────────────────┘
       │
       ▼
┌──────────────────────────────┐
│  restaurant_weather_join_etl  │  Left-join with weather data on geohash
└──────────────────────────────┘
       │
       ▼
Partitioned Parquet Output (year / month / day)
```

**Key design decisions:**
- Weather data is deduplicated on `(geohash, wthr_date)` before joining to prevent row multiplication
- Left join preserves all restaurants even when no weather data matches
- Output uses `partitionOverwriteMode=dynamic` for idempotent reruns
- OpenCage API is called only for rows with missing coordinates

---

## Project Structure

```
spark-practice-task/
├── restaurant_etl.py                 # Job 1: Union & clean CSV files
├── restaurant_geocoding_etl.py       # Job 2: Enrich null coordinates via API
├── restaurant_geohash_etl.py         # Job 3: Generate geohashes
├── restaurant_weather_join_etl.py    # Job 4: Join with weather data
├── run_pipeline.sh                   # Bash script to run all jobs sequentially
├── test_restaurant_etl.py            # Unit tests (pytest + pyspark)
├── logs/                             # Auto-generated job logs
└── README.md
```

---

## Prerequisites & Installation

### System Requirements

- Ubuntu 22.04 / 24.04
- Python 3.10+
- Java 17 (required by Spark)

### Install Java

```bash
sudo apt update
sudo apt install -y openjdk-17-jdk
```

### Set up Python virtual environment

```bash
sudo apt install -y python3-venv
python3 -m venv ~/spark-env
source ~/spark-env/bin/activate
```

### Install dependencies

```bash
pip install pyspark requests python-geohash pytest
```

Or system-wide (without venv):

```bash
pip install pyspark requests python-geohash pytest --break-system-packages
```

### Set JAVA_HOME (if needed)

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

---

## Configuration

Before running the pipeline, update the following variables at the top of each script:

| Script | Variable | Description |
|---|---|---|
| `restaurant_etl.py` | `INPUT_DIR` | Path to folder containing the 5 restaurant CSV files |
| `restaurant_etl.py` | `OUTPUT_DIR` | Output path for cleaned/unioned data |
| `restaurant_geocoding_etl.py` | `INPUT_DIR` | Output of Job 1 |
| `restaurant_geocoding_etl.py` | `OUTPUT_DIR` | Output path for geocoded data |
| `restaurant_geocoding_etl.py` | `API_KEY` | Your OpenCage Geocoding API key |
| `restaurant_geohash_etl.py` | `INPUT_DIR` | Output of Job 2 |
| `restaurant_geohash_etl.py` | `OUTPUT_DIR` | Output path for geohashed data |
| `restaurant_weather_join_etl.py` | `RESTAURANT_INPUT` | Output of Job 3 |
| `restaurant_weather_join_etl.py` | `WEATHER_INPUT` | Root path of partitioned weather Parquet |
| `restaurant_weather_join_etl.py` | `OUTPUT_DIR` | Final output path |

### OpenCage API Key

Sign up for a free API key at [opencagedata.com](https://opencagedata.com). The free tier allows up to 2,500 requests/day.

### Weather Data

Weather data is expected in partitioned Parquet format with the following directory structure:

```
weather/
└── year=2016/
    └── month=10/
        ├── day=1/
        ├── day=2/
        ├── day=3/
        └── day=4/
```

Expected weather schema:

```
root
 |-- lng: double (nullable = true)
 |-- lat: double (nullable = true)
 |-- avg_tmpr_f: double (nullable = true)
 |-- avg_tmpr_c: double (nullable = true)
 |-- wthr_date: string (nullable = true)
```

---

## How to Run the Pipeline

### Run all jobs at once (recommended)

```bash
chmod +x run_pipeline.sh
./run_pipeline.sh
```

The script runs all four jobs sequentially, stops immediately if any job fails, and writes timestamped logs to the `logs/` directory.

### Run individual jobs

```bash
spark-submit restaurant_etl.py
spark-submit restaurant_geocoding_etl.py
spark-submit restaurant_geohash_etl.py
spark-submit restaurant_weather_join_etl.py
```

### Run with virtual environment

```bash
source ~/spark-env/bin/activate
spark-submit restaurant_etl.py
```

Or set permanently:

```bash
export PYSPARK_PYTHON=~/spark-env/bin/python
```

---

## How to Run Tests

```bash
# Activate venv if using one
source ~/spark-env/bin/activate

# Run all tests with verbose output
pytest test_restaurant_etl.py -v
```

The test suite contains **18 unit tests** across three test classes:

| Class | Tests | What's covered |
|---|---|---|
| `TestGeocodingETL` | 6 | Null detection, API success/failure mocking, row count after union |
| `TestGeohashETL` | 6 | Hash length, known coordinate values, null handling, column presence |
| `TestWeatherJoinETL` | 6 | Geohash generation, deduplication, left join integrity, schema validation, column collision |

---

## Output Data Structure

### Intermediate outputs

| Stage | Format | Location |
|---|---|---|
| Job 1 — Cleaned restaurants | CSV | `OUTPUT_DIR/restaurant` |
| Job 2 — Geocoded restaurants | CSV | `OUTPUT_DIR/restaurant_geocoded` |
| Job 3 — Geohashed restaurants | CSV | `OUTPUT_DIR/restaurant_geohashed` |

### Final output

The enriched dataset is stored in **Parquet format**, partitioned by date:

```
restaurant_weather_joined/
└── year=2016/
    └── month=10/
        ├── day=1/
        │   └── part-00000-*.parquet
        ├── day=2/
        ├── day=3/
        └── day=4/
```

### Final schema

| Field | Type | Source |
|---|---|---|
| `id` | long | Restaurant |
| `franchise_id` | long | Restaurant |
| `franchise_name` | string | Restaurant |
| `restaurant_franchise_id` | long | Restaurant |
| `country` | string | Restaurant |
| `city` | string | Restaurant |
| `lat` | double | Restaurant |
| `lng` | double | Restaurant |
| `geohash` | string | Generated (4-char) |
| `wthr_lat` | double | Weather |
| `wthr_lng` | double | Weather |
| `avg_tmpr_f` | double | Weather |
| `avg_tmpr_c` | double | Weather |
| `wthr_date` | string | Weather |
| `year` | integer | Partition key |
| `month` | integer | Partition key |
| `day` | integer | Partition key |
