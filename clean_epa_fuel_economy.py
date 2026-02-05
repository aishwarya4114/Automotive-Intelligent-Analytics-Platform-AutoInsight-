"""
Clean EPA Fuel Economy Data
Reads raw data from S3, minimal cleaning needed (data is already high quality)

Output columns standardized for Snowflake joins:
- make, model, year (consistent across all datasets)
- vehicle_id (same generation formula everywhere)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, upper, trim, regexp_replace, 
    current_timestamp, lit, coalesce, concat_ws, sha2
)
from pyspark.sql.types import IntegerType, FloatType
from dotenv import load_dotenv
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.common_transformations import (
    standardize_manufacturer_names,
    clean_text_field,
    remove_duplicates,
    add_processing_metadata,
    filter_top_manufacturers,
    cleanup_s3_path
)

def clean_fuel_economy(spark, input_path, output_path):
    """
    Main cleaning function for EPA Fuel Economy data
    
    Note: This data is already very clean!
    - Already filtered to 2020-2024
    - Already filtered to top 15 manufacturers
    - Zero NULL values in critical fields
    
    Minimal transformations needed:
    1. Standardize make names (to match NHTSA exactly)
    2. Clean model field
    3. Create vehicle_id for joins
    4. Select essential columns
    5. Rename for Snowflake consistency
    """
    
    print("="*60)
    print("EPA FUEL ECONOMY - SPARK CLEANING PIPELINE")
    print("Input: 2020-2024 data (pre-filtered, high quality)")
    print("Target: Top 15 manufacturers (already filtered)")
    print("Output: Snowflake-ready with standardized columns")
    print("="*60)
    
    # Read raw data
    print("\nReading raw data from: {}".format(input_path))
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)
    
    initial_count = df.count()
    initial_cols = len(df.columns)
    print("  Loaded {:,} records".format(initial_count))
    print("  Initial columns: {}".format(initial_cols))
    
    # 1. Standardize manufacturer names (to match NHTSA exactly)
    print("\nStep 1: Standardizing manufacturer names...")
    df = standardize_manufacturer_names(df, 'make')
    
    # Verify all manufacturers mapped correctly
    df = filter_top_manufacturers(df, 'make_standardized')
    
    # 2. Clean MODEL field
    print("\nStep 2: Cleaning MODEL field...")
    df = clean_text_field(df, 'model')
    
    # 3. Validate YEAR (should already be clean)
    print("\nStep 3: Validating YEAR field...")
    df = df.withColumn(
        'year_clean',
        when((col('year') < 2020) | (col('year') > 2025), None)
        .otherwise(col('year').cast(IntegerType()))
    )
    
    # Check for any invalid years
    invalid_count = df.filter(col('year_clean').isNull()).count()
    if invalid_count > 0:
        print("  Warning: {} records with invalid years (filtered out)".format(invalid_count))
        df = df.filter(col('year_clean').isNotNull())
    
    # 4. Standardize fuel type categories
    print("\nStep 4: Standardizing fuel type categories...")
    
    # Create simplified fuel category
    df = df.withColumn(
        'fuel_category',
        when(col('fuel_type1') == 'Electricity', lit('Electric'))
        .when(col('fuel_type2') == 'Electricity', lit('Plug-in Hybrid'))
        .when(col('fuel_type2') == 'E85', lit('Flex-Fuel'))
        .when(col('fuel_type1').contains('Diesel'), lit('Diesel'))
        .when(col('fuel_type1').contains('Gasoline'), lit('Gasoline'))
        .otherwise(lit('Other'))
    )
    
    # 5. Create efficiency score (unified metric for all fuel types)
    print("\nStep 5: Creating unified efficiency metric...")
    
    # For gas vehicles: use mpg_combined
    # For electric: use mpge_combined
    # Prioritize electric efficiency
    df = df.withColumn(
        'efficiency_score',
        coalesce(
            col('mpge_combined'),
            col('mpg_combined').cast(FloatType())
        )
    )
    
    # 6. Data quality flags
    print("\nStep 6: Adding data quality flags...")
    
    df = df.withColumn(
        'is_electric',
        when(col('fuel_type1') == 'Electricity', True).otherwise(False)
    )
    
    df = df.withColumn(
        'is_hybrid',
        when(col('fuel_type2') == 'Electricity', True).otherwise(False)
    )
    
    df = df.withColumn(
        'is_flex_fuel',
        when(col('fuel_type2') == 'E85', True).otherwise(False)
    )
    
    # 7. Remove duplicates (if any)
    print("\nStep 7: Removing duplicates...")
    before_dedup = df.count()
    df = remove_duplicates(df, ['epa_id'])
    after_dedup = df.count()
    duplicates_removed = before_dedup - after_dedup
    print("  Removed {:,} duplicate records".format(duplicates_removed))
    
    # 8. STANDARDIZE COLUMN NAMES FOR SNOWFLAKE
    print("\nStep 8: Standardizing column names for Snowflake...")
    
    df = df.withColumn('make', col('make_standardized'))
    df = df.withColumn('model', col('model_clean'))
    df = df.withColumn('year', col('year_clean'))
    
    # 9. Create vehicle_id using STANDARDIZED columns
    print("\nStep 9: Creating standardized vehicle_id...")
    df = df.withColumn(
        'vehicle_id',
        sha2(
            concat_ws("_",
                coalesce(col('make').cast("string"), lit("")),
                coalesce(col('model').cast("string"), lit("")),
                coalesce(col('year').cast("string"), lit(""))
            ),
            256
        )
    )
    
    # Create fuel_economy_id from epa_id
    df = df.withColumn(
        'fuel_economy_id',
        sha2(col('epa_id').cast("string"), 256)
    )
    
    # 10. Add metadata
    print("\nStep 10: Adding metadata...")
    df = add_processing_metadata(df, 'EPA_FUEL_ECONOMY')
    
    # 11. Select final columns (18 essential columns)
    print("\nStep 11: Selecting final columns (Snowflake-ready)...")
    
    df_final = df.select(
        # IDs
        col('fuel_economy_id'),
        col('epa_id'),
        col('vehicle_id'),
        
        # Vehicle Info (STANDARDIZED for joins)
        col('make'),
        col('model'),
        col('year'),
        col('vehicle_class'),
        
        # Fuel Type
        col('fuel_type'),
        col('fuel_type1').alias('primary_fuel_type'),
        col('fuel_type2').alias('secondary_fuel_type'),
        col('fuel_category'),
        
        # Efficiency - Gasoline/Hybrid
        col('mpg_city').cast(IntegerType()),
        col('mpg_highway').cast(IntegerType()),
        col('mpg_combined').cast(IntegerType()),
        
        # Efficiency - Electric
        col('mpge_combined').cast(FloatType()).alias('mpge_combined'),
        col('electric_range').cast(IntegerType()),
        
        # Unified efficiency metric
        col('efficiency_score'),
        
        # Engine/Drivetrain
        col('cylinders').cast(IntegerType()),
        col('drive_type').alias('drive_train'),
        col('transmission'),
        
        # Environmental
        col('greenhouse_gas_score').cast(IntegerType()),
        col('co2_tailpipe_gpm').cast(FloatType()).alias('co2_tailpipe'),
        
        # Flags
        col('is_electric'),
        col('is_hybrid'),
        col('is_flex_fuel'),
        
        # Metadata
        col('processed_at'),
        col('data_source'),
        col('processing_version')
    )
    
    # 12. Final validation
    final_count = df_final.count()
    final_cols = len(df_final.columns)
    
    print("\nCleaning Summary:")
    print("  Initial records: {:,}".format(initial_count))
    print("  Final records: {:,}".format(final_count))
    print("  Retention rate: {:.1f}%".format(final_count / initial_count * 100))
    print("  Initial columns: {}".format(initial_cols))
    print("  Final columns: {} (selected essentials)".format(final_cols))
    
    # Show fuel type distribution
    print("\n  Fuel Type Distribution:")
    fuel_dist = df_final.groupBy('fuel_category').count().orderBy('count', ascending=False).collect()
    for row in fuel_dist:
        print("    {}: {:,}".format(row['fuel_category'], row['count']))
    
    # Manufacturer distribution
    print("\n  Top Manufacturers:")
    mfr_counts = df_final.groupBy('make').count() \
        .orderBy('count', ascending=False).collect()
    for row in mfr_counts:
        print("    {}: {:,}".format(row['make'], row['count']))
    
    # Year distribution
    print("\n  Records by Year:")
    year_counts = df_final.groupBy('year').count().orderBy('year').collect()
    for row in year_counts:
        print("    {}: {:,}".format(row['year'], row['count']))
    
    # 13. CLEANUP OLD S3 DATA
    print("\nPreparing to write cleaned data...")
    cleanup_s3_path(spark, output_path)
    
    # 14. Write new data
    print("Writing cleaned data to: {}".format(output_path))
    df_final.write.mode('overwrite').parquet(output_path)
    
    # Cleanup and write CSV
    csv_output_path = output_path.replace('/parquet/', '/csv/')
    cleanup_s3_path(spark, csv_output_path)
    
    print("Writing CSV to: {}".format(csv_output_path))
    df_final.coalesce(1).write.mode('overwrite') \
        .option('header', 'true') \
        .csv(csv_output_path)
    
    print("\nEPA Fuel Economy cleaning complete!")
    print("="*60)
    
    return df_final


def main():
    """
    Main execution function
    """
    load_dotenv()
    
    is_local = os.getenv('SPARK_ENV', 'local') == 'local'
    
    print("\nStarting EPA Fuel Economy Cleaning")
    if is_local:
        print("Running in LOCAL mode")
    
    # Initialize Spark
    builder = SparkSession.builder \
        .appName("EPA_Fuel_Economy_Cleaning") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Add S3 JARs for local mode
    if is_local:
        print("Loading AWS S3 connectors...")
        builder = builder \
            .config("spark.jars.packages", 
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID', '')) \
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY', '')) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    # S3 paths
    S3_BUCKET = os.getenv('S3_BUCKET_NAME', 'your-bucket-name')
    INPUT_PATH = "s3a://{}/epa/raw/*.csv".format(S3_BUCKET)
    OUTPUT_PATH = "s3a://{}/epa/cleaned/fuel_economy/parquet/".format(S3_BUCKET)
    
    print("Input: {}".format(INPUT_PATH))
    print("Output: {}".format(OUTPUT_PATH))
    
    # Run cleaning
    df_clean = clean_fuel_economy(spark, INPUT_PATH, OUTPUT_PATH)
    
    # Show sample
    print("\nSample of cleaned data (Snowflake-ready):")
    df_clean.select(
        'fuel_economy_id', 'make', 'model', 'year', 
        'fuel_category', 'mpg_combined', 'efficiency_score'
    ).show(10, truncate=False)
    
    # Verify vehicle_id consistency
    print("\nVehicle ID verification (first 5):")
    df_clean.select('vehicle_id', 'make', 'model', 'year').show(5, truncate=False)
    
    # Show EV sample
    print("\nElectric vehicle sample:")
    df_clean.filter(col('is_electric') == True) \
        .select('make', 'model', 'year', 'mpge_combined', 'electric_range') \
        .show(5, truncate=False)
    
    spark.stop()


if __name__ == "__main__":
    main()
