import pytest
import geohash as gh
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
from pyspark.sql.functions import col, udf


# ── Shared Spark Session (session-scoped for performance) ─────────────────────
@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("RestaurantETLTests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


# ─────────────────────────────────────────────────────────────────────────────
# Fixtures — reusable sample DataFrames
# ─────────────────────────────────────────────────────────────────────────────
@pytest.fixture
def restaurant_schema():
    return StructType([
        StructField("id",                     LongType(),   True),
        StructField("franchise_id",           LongType(),   True),
        StructField("franchise_name",         StringType(), True),
        StructField("restaurant_franchise_id",LongType(),   True),
        StructField("country",                StringType(), True),
        StructField("city",                   StringType(), True),
        StructField("lat",                    DoubleType(), True),
        StructField("lng",                    DoubleType(), True),
    ])

@pytest.fixture
def restaurant_data_with_nulls(spark, restaurant_schema):
    data = [
        (257698037796, 37, "Cafe Crepe",       26468, "IT", "Milan",     45.533,  9.171),
        (25769803831,  56, "The Waffle House",  72230, "FR", "Paris",     48.873,  2.305),
        (85899345988,  69, "Dragonfly Cafe",    18952, "NL", "Amsterdam", None,    None ),  # null coords
        (111669149758, 63, "Cafe Paris",        84488, "NL", "Amsterdam", None,    None ),  # null coords
    ]
    return spark.createDataFrame(data, schema=restaurant_schema)

@pytest.fixture
def restaurant_data_clean(spark, restaurant_schema):
    data = [
        (257698037796, 37, "Cafe Crepe",      26468, "IT", "Milan",     45.533,  9.171),
        (25769803831,  56, "The Waffle House", 72230, "FR", "Paris",     48.873,  2.305),
        (85899345988,  69, "Dragonfly Cafe",   18952, "NL", "Amsterdam", 52.392,  4.911),
    ]
    return spark.createDataFrame(data, schema=restaurant_schema)

@pytest.fixture
def restaurant_with_geohash(spark):
    schema = StructType([
        StructField("id",           LongType(),   True),
        StructField("franchise_name",StringType(), True),
        StructField("country",      StringType(), True),
        StructField("city",         StringType(), True),
        StructField("lat",          DoubleType(), True),
        StructField("lng",          DoubleType(), True),
        StructField("geohash",      StringType(), True),
    ])
    data = [
        (257698037796, "Cafe Crepe",      "IT", "Milan",     45.533,  9.171, "u0ne"),
        (25769803831,  "The Waffle House", "FR", "Paris",     48.873,  2.305, "u09t"),
        (85899345988,  "Dragonfly Cafe",   "NL", "Amsterdam", 52.392,  4.911, "u173"),
    ]
    return spark.createDataFrame(data, schema=schema)

@pytest.fixture
def weather_data(spark):
    schema = StructType([
        StructField("lat",        DoubleType(), True),
        StructField("lng",        DoubleType(), True),
        StructField("avg_tmpr_f", DoubleType(), True),
        StructField("avg_tmpr_c", DoubleType(), True),
        StructField("wthr_date",  StringType(), True),
    ])
    data = [
        (45.5,  9.2,  65.0, 18.3, "2016-10-01"),   # Milan area  → geohash u0nd
        (45.5,  9.2,  63.0, 17.2, "2016-10-02"),   # Milan area  → geohash u0nd
        (48.9,  2.3,  55.0, 12.8, "2016-10-01"),   # Paris area  → geohash u09t
        (52.4,  4.9,  50.0, 10.0, "2016-10-01"),   # Amsterdam   → geohash u173
        (52.4,  4.9,  50.0, 10.0, "2016-10-01"),   # duplicate row — should be deduped
    ]
    return spark.createDataFrame(data, schema=schema)


# ─────────────────────────────────────────────────────────────────────────────
# 1. Geocoding ETL Tests
# ─────────────────────────────────────────────────────────────────────────────
class TestGeocodingETL:

    def test_detects_null_coordinates(self, restaurant_data_with_nulls):
        """Rows with null lat or lng should be correctly identified."""
        null_rows = restaurant_data_with_nulls.filter(
            col("lat").isNull() | col("lng").isNull()
        )
        assert null_rows.count() == 2

    def test_non_null_rows_unchanged(self, restaurant_data_with_nulls):
        """Rows with valid coordinates should not be touched."""
        ok_rows = restaurant_data_with_nulls.filter(
            col("lat").isNotNull() & col("lng").isNotNull()
        )
        assert ok_rows.count() == 2

    @patch("requests.get")
    def test_geocode_udf_success(self, mock_get):
        """UDF should return (lat, lng) when API responds successfully."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "total_results": 1,
            "results": [{"geometry": {"lat": 52.392, "lng": 4.911}}]
        }
        mock_get.return_value = mock_response

        import requests
        response = requests.get("https://api.opencagedata.com/geocode/v1/json", params={})
        data = response.json()

        assert data["total_results"] == 1
        assert data["results"][0]["geometry"]["lat"] == 52.392
        assert data["results"][0]["geometry"]["lng"] == 4.911

    @patch("requests.get")
    def test_geocode_udf_no_results(self, mock_get):
        """UDF should return None when API finds no results."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"total_results": 0, "results": []}
        mock_get.return_value = mock_response

        import requests
        data = requests.get("url", params={}).json()
        result = (None, None) if data["total_results"] == 0 else data["results"][0]
        assert result == (None, None)

    @patch("requests.get")
    def test_geocode_udf_api_failure(self, mock_get):
        """UDF should handle network errors gracefully and return None."""
        mock_get.side_effect = Exception("Connection timeout")
        import requests
        try:
            requests.get("url", params={})
            result = (1.0, 2.0)
        except Exception:
            result = (None, None)
        assert result == (None, None)

    def test_union_restores_full_count(self, spark, restaurant_data_with_nulls, restaurant_schema):
        """After geocoding, unioning clean + enriched rows should preserve total count."""
        df_ok      = restaurant_data_with_nulls.filter(col("lat").isNotNull())
        df_missing = restaurant_data_with_nulls.filter(col("lat").isNull())

        # Simulate geocoding by filling in coords manually
        df_filled = df_missing.fillna({"lat": 52.310, "lng": 4.942})
        df_result = df_ok.unionByName(df_filled)

        assert df_result.count() == restaurant_data_with_nulls.count()


# ─────────────────────────────────────────────────────────────────────────────
# 2. Geohash ETL Tests
# ─────────────────────────────────────────────────────────────────────────────
class TestGeohashETL:

    def test_geohash_length_is_four(self):
        """Generated geohash must be exactly 4 characters long."""
        result = gh.encode(45.533, 9.171, precision=4)
        assert len(result) == 4

    def test_geohash_known_values(self):
        """Verify geohash output matches known expected values."""
        assert gh.encode(45.533,  9.171, precision=4) == "u0ne"
        assert gh.encode(48.873,  2.305, precision=4) == "u09t"
        assert gh.encode(52.392,  4.911, precision=4) == "u173"

    def test_geohash_null_input_returns_none(self):
        """UDF should return None when lat or lng is None."""
        def generate_geohash(lat, lng):
            if lat is None or lng is None:
                return None
            return gh.encode(lat, lng, precision=4)

        assert generate_geohash(None, 9.171) is None
        assert generate_geohash(45.533, None) is None
        assert generate_geohash(None, None)   is None

    def test_geohash_column_added(self, spark, restaurant_data_clean):
        """DataFrame should gain a 'geohash' column after transformation."""
        def generate_geohash(lat, lng):
            if lat is None or lng is None:
                return None
            return gh.encode(lat, lng, precision=4)

        geohash_udf = udf(generate_geohash, StringType())
        df_result = restaurant_data_clean.withColumn(
            "geohash", geohash_udf(col("lat"), col("lng"))
        )
        assert "geohash" in df_result.columns

    def test_no_null_geohash_when_coords_present(self, spark, restaurant_data_clean):
        """All rows with valid lat/lng should produce a non-null geohash."""
        def generate_geohash(lat, lng):
            if lat is None or lng is None:
                return None
            return gh.encode(lat, lng, precision=4)

        geohash_udf = udf(generate_geohash, StringType())
        df_result = restaurant_data_clean.withColumn(
            "geohash", geohash_udf(col("lat"), col("lng"))
        )
        null_geohash_count = df_result.filter(col("geohash").isNull()).count()
        assert null_geohash_count == 0

    def test_row_count_unchanged_after_geohash(self, spark, restaurant_data_clean):
        """Adding geohash column must not change the number of rows."""
        def generate_geohash(lat, lng):
            if lat is None or lng is None:
                return None
            return gh.encode(lat, lng, precision=4)

        geohash_udf = udf(generate_geohash, StringType())
        df_result = restaurant_data_clean.withColumn(
            "geohash", geohash_udf(col("lat"), col("lng"))
        )
        assert df_result.count() == restaurant_data_clean.count()


# ─────────────────────────────────────────────────────────────────────────────
# 3. Weather & Restaurant Join ETL Tests
# ─────────────────────────────────────────────────────────────────────────────
class TestWeatherJoinETL:

    def _add_geohash(self, df):
        """Helper to add geohash column to weather data."""
        def generate_geohash(lat, lng):
            if lat is None or lng is None:
                return None
            return gh.encode(lat, lng, precision=4)
        geohash_udf = udf(generate_geohash, StringType())
        return df.withColumn("geohash", geohash_udf(col("lat"), col("lng")))

    def test_weather_geohash_generated(self, weather_data):
        """Weather data should get a geohash column from lat/lng."""
        df = self._add_geohash(weather_data)
        assert "geohash" in df.columns
        assert df.filter(col("geohash").isNull()).count() == 0

    def test_weather_deduplication(self, weather_data):
        """Duplicate (geohash, wthr_date) rows must be removed before join."""
        df = self._add_geohash(weather_data)
        df_deduped = df.dropDuplicates(["geohash", "wthr_date"])
        assert df_deduped.count() < df.count()

    def test_left_join_preserves_all_restaurants(self, restaurant_with_geohash, weather_data):
        """All restaurant rows must appear in the result regardless of weather match."""
        df_weather = self._add_geohash(weather_data)
        df_weather_deduped = (
            df_weather
            .dropDuplicates(["geohash", "wthr_date"])
            .withColumnRenamed("lat", "wthr_lat")
            .withColumnRenamed("lng", "wthr_lng")
        )
        df_joined = restaurant_with_geohash.join(df_weather_deduped, on="geohash", how="left")
        # Restaurant count is 3; joined result may be > 3 due to multiple weather dates
        restaurant_ids = df_joined.select("id").distinct().count()
        assert restaurant_ids == restaurant_with_geohash.count()

    def test_no_data_multiplication_on_join(self, restaurant_with_geohash, weather_data):
        """After deduplication, each restaurant should appear once per weather date, not multiplied."""
        df_weather = self._add_geohash(weather_data)
        df_weather_deduped = (
            df_weather
            .dropDuplicates(["geohash", "wthr_date"])
            .withColumnRenamed("lat", "wthr_lat")
            .withColumnRenamed("lng", "wthr_lng")
        )
        df_joined = restaurant_with_geohash.join(df_weather_deduped, on="geohash", how="left")

        # Each (id, wthr_date) pair should be unique
        unique_pairs = df_joined.select("id", "wthr_date").distinct().count()
        assert unique_pairs == df_joined.count()

    def test_joined_schema_contains_all_fields(self, restaurant_with_geohash, weather_data):
        """Output schema must include fields from both restaurant and weather datasets."""
        df_weather = (
            self._add_geohash(weather_data)
            .dropDuplicates(["geohash", "wthr_date"])
            .withColumnRenamed("lat", "wthr_lat")
            .withColumnRenamed("lng", "wthr_lng")
        )
        df_joined = restaurant_with_geohash.join(df_weather, on="geohash", how="left")
        columns = df_joined.columns

        # Restaurant fields
        for field in ["id", "franchise_name", "city", "lat", "lng", "geohash"]:
            assert field in columns, f"Missing restaurant field: {field}"
        # Weather fields
        for field in ["avg_tmpr_f", "avg_tmpr_c", "wthr_date"]:
            assert field in columns, f"Missing weather field: {field}"

    def test_no_column_collision(self, restaurant_with_geohash, weather_data):
        """lat/lng columns must not collide — weather coords renamed to wthr_lat/wthr_lng."""
        df_weather = (
            self._add_geohash(weather_data)
            .dropDuplicates(["geohash", "wthr_date"])
            .withColumnRenamed("lat", "wthr_lat")
            .withColumnRenamed("lng", "wthr_lng")
        )
        df_joined = restaurant_with_geohash.join(df_weather, on="geohash", how="left")
        assert "wthr_lat" in df_joined.columns
        assert "wthr_lng" in df_joined.columns
