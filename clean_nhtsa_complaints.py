"""
Clean NHTSA Complaints Data with Sentiment Analysis
Reads raw data from S3, cleans it, adds sentiment analysis, and writes back to S3

Output columns standardized for Snowflake joins:
- make, model, year (consistent across all datasets)
- vehicle_id (same generation formula everywhere)
- sentiment_score, sentiment_label (NEW!)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, upper, trim, regexp_replace, to_date, 
    current_timestamp, lit, year, length, coalesce,
    concat_ws, sha2
)
from pyspark.sql.types import IntegerType, FloatType, BooleanType
from dotenv import load_dotenv
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.common_transformations import (
    standardize_manufacturer_names,
    clean_date_field,
    clean_text_field,
    remove_duplicates,
    add_processing_metadata,
    filter_top_manufacturers,
    cleanup_s3_path
)

# NEW: Import sentiment analysis
from sentiment.spark_udfs import add_sentiment_analysis

def convert_yn_to_boolean(df, column_name):
    """
    Convert Y/N strings to boolean True/False
    """
    boolean_col = f"{column_name.lower()}_flag"
    
    df = df.withColumn(
        boolean_col,
        when(upper(trim(col(column_name))) == 'Y', True)
        .when(upper(trim(col(column_name))) == 'N', False)
        .otherwise(None)
    )
    
    return df

def create_severity_level(df):
    """
    Create severity level based on crash, fire, injuries, deaths
    """
    df = df.withColumn(
        'severity_level',
        when(col('deaths') > 0, lit('CRITICAL'))
        .when(col('injured') > 0, lit('HIGH'))
        .when((col('crash_flag') == True) | (col('fire_flag') == True), lit('MEDIUM'))
        .otherwise(lit('LOW'))
    )
    
    df = df.withColumn(
        'has_incident',
        when(
            (col('crash_flag') == True) | 
            (col('fire_flag') == True) | 
            (col('injured') > 0) | 
            (col('deaths') > 0),
            True
        ).otherwise(False)
    )
    
    return df

def clean_complaints(spark, input_path, output_path):
    """
    Main cleaning function for NHTSA Complaints data
    
    Output standardized for Snowflake:
    - Column names: make, model, year
    - vehicle_id: Generated from (make, model, year)
    - sentiment_score: Custom sentiment analysis (NEW!)
    """
    
    print("="*60)
    print("NHTSA COMPLAINTS - SPARK CLEANING PIPELINE WITH SENTIMENT")
    print("Input: 2020-2025 data (pre-filtered)")
    print("Target: Top 15 manufacturers")
    print("Output: Snowflake-ready with sentiment analysis")
    print("="*60)
    
    # Read raw data
    print("\nReading raw data from: {}".format(input_path))
    
    df = spark.read \
        .option("header", "true") \
        .option("sep", ",") \
        .option("inferSchema", "true") \
        .option("charset", "ISO-8859-1") \
        .csv(input_path)
    
    initial_count = df.count()
    initial_cols = len(df.columns)
    print("  Loaded {:,} records".format(initial_count))
    print("  Initial columns: {}".format(initial_cols))
    
    # 1. Standardize manufacturer names
    print("\nStep 1: Standardizing manufacturer names...")
    df = standardize_manufacturer_names(df, 'MFR_NAME')
    df = standardize_manufacturer_names(df, 'MAKETXT')
    
    # Use MAKETXT_standardized as primary make
    df = df.withColumn(
        'make_primary',
        coalesce(col('MAKETXT_standardized'), col('MFR_NAME_standardized'))
    )
    
    # 2. Filter to top 15 manufacturers
    print("\nStep 2: Filtering to top 15 manufacturers...")
    before_filter = df.count()
    df = df.filter(col('make_primary').isNotNull())
    df = filter_top_manufacturers(df, 'make_primary')
    
    # 3. Clean and validate YEAR
    print("\nStep 3: Validating YEAR field...")
    
    # Use YEARTXT_clean if exists, otherwise clean YEARTXT
    if 'YEARTXT_clean' in df.columns:
        df = df.withColumn('year_clean', col('YEARTXT_clean').cast(IntegerType()))
    else:
        df = df.withColumn(
            'year_clean',
            when(col('YEARTXT').isNull(), None)
            .otherwise(col('YEARTXT').cast(IntegerType()))
        )
    
    # Validate year range
    df = df.withColumn(
        'year_clean',
        when((col('year_clean') < 1990) | (col('year_clean') > 2030), None)
        .otherwise(col('year_clean'))
    )
    
    # Filter to 2020-2025 if not already done
    df = df.filter((col('year_clean') >= 2020) & (col('year_clean') <= 2025))
    
    # 4. Clean MODEL
    print("\nStep 4: Cleaning MODEL field...")
    df = clean_text_field(df, 'MODELTXT')
    
    # 5. Clean date fields
    print("\nStep 5: Cleaning date fields...")
    
    # FAILDATE
    df = df.withColumn(
        'fail_date',
        when(col('FAILDATE').isNull(), None)
        .when(length(col('FAILDATE').cast("string")) != 8, None)
        .otherwise(to_date(col('FAILDATE').cast("string"), "yyyyMMdd"))
    )
    
    # DATEA (date added)
    df = df.withColumn(
        'date_added',
        when(col('DATEA').isNull(), None)
        .when(length(col('DATEA').cast("string")) != 8, None)
        .otherwise(to_date(col('DATEA').cast("string"), "yyyyMMdd"))
    )
    
    # 6. Convert Y/N fields to boolean
    print("\nStep 6: Converting Y/N fields to boolean...")
    yn_columns = ['CRASH', 'FIRE', 'POLICE_RPT_YN', 'ORIG_OWNER_YN', 
                  'ANTI_BRAKES_YN', 'CRUISE_CONT_YN', 'REPAIRED_YN', 
                  'VEHICLES_TOWED_YN']
    
    for yn_col in yn_columns:
        if yn_col in df.columns:
            df = convert_yn_to_boolean(df, yn_col)
    
    # 7. Convert numeric fields
    print("\nStep 7: Converting numeric fields...")
    
    # INJURED
    df = df.withColumn(
        'injured',
        when(col('INJURED').isNull(), lit(0))
        .otherwise(col('INJURED').cast(IntegerType()))
    )
    
    # DEATHS
    df = df.withColumn(
        'deaths',
        when(col('DEATHS').isNull(), lit(0))
        .otherwise(col('DEATHS').cast(IntegerType()))
    )
    
    # MILES
    df = df.withColumn(
        'mileage',
        when(col('MILES').isNull(), None)
        .when((col('MILES').cast(FloatType()) < 0) | (col('MILES').cast(FloatType()) > 500000), None)
        .otherwise(col('MILES').cast(FloatType()))
    )
    
    # VEH_SPEED
    df = df.withColumn(
        'vehicle_speed',
        when(col('VEH_SPEED').isNull(), None)
        .when((col('VEH_SPEED').cast(FloatType()) < 0) | (col('VEH_SPEED').cast(FloatType()) > 200), None)
        .otherwise(col('VEH_SPEED').cast(FloatType()))
    )
    
    # 8. Clean text fields
    print("\nStep 8: Cleaning text fields...")
    df = clean_text_field(df, 'CDESCR')
    df = clean_text_field(df, 'COMPDESC')
    
    # 9. Create severity level
    print("\nStep 9: Creating severity level...")
    df = create_severity_level(df)
    
    # 10. Data quality flags
    print("\nStep 10: Adding data quality flags...")
    df = df.withColumn(
        'missing_critical_data',
        when(
            col('make_primary').isNull() | 
            col('year_clean').isNull() |
            col('CDESCR_clean').isNull(),
            True
        ).otherwise(False)
    )
    
    # 11. Remove duplicates
    print("\nStep 11: Removing duplicates...")
    before_dedup = df.count()
    df = remove_duplicates(df, ['CMPLID'])
    after_dedup = df.count()
    duplicates_removed = before_dedup - after_dedup
    print("  Removed {:,} duplicate records".format(duplicates_removed))
    
    # 12. STANDARDIZE COLUMN NAMES FOR SNOWFLAKE
    print("\nStep 12: Standardizing column names for Snowflake...")
    
    df = df.withColumn('make', col('make_primary'))
    df = df.withColumn('model', col('MODELTXT_clean'))
    df = df.withColumn('year', col('year_clean'))
    
    # 13. Create vehicle_id using STANDARDIZED columns
    print("\nStep 13: Creating standardized vehicle_id...")
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
    
    # Create complaint_id
    df = df.withColumn(
        'complaint_id',
        sha2(col('CMPLID').cast("string"), 256)
    )
    
    # 14. Add metadata
    print("\nStep 14: Adding metadata...")
    df = add_processing_metadata(df, 'NHTSA_COMPLAINTS')
    
    # ============================================================
    # NEW STEP 15: APPLY CUSTOM SENTIMENT ANALYSIS
    # ============================================================
    print("\n" + "="*60)
    print("Step 15: Applying Custom Sentiment Analysis")
    print("="*60)
    print("This will add 6 new columns:")
    print("  - sentiment_score")
    print("  - sentiment_label")
    print("  - sentiment_confidence")
    print("  - complaint_embedding")
    print("  - negation_count")
    print("  - safety_keyword_count")
    
    df = add_sentiment_analysis(
        df,
        text_column='CDESCR_clean',  # Use the cleaned complaint description
        include_embeddings=True,      # Generate 768-dim vectors
        include_features=True          # Extract linguistic features
    )
    
    # ============================================================
    # NEW STEP 16: CREATE SENTIMENT CATEGORIES
    # ============================================================
    print("\n" + "="*60)
    print("Step 16: Creating Sentiment Categories")
    print("="*60)
    
    df = df.withColumn(
        'sentiment_category',
        when(col('sentiment_score') < -0.5, lit('VERY_NEGATIVE'))
        .when(col('sentiment_score') < -0.2, lit('NEGATIVE'))
        .when(col('sentiment_score') < 0.2, lit('NEUTRAL'))
        .when(col('sentiment_score') < 0.5, lit('POSITIVE'))
        .otherwise(lit('VERY_POSITIVE'))
    )
    
    # Show sentiment distribution
    print("\n📊 Sentiment Distribution:")
    try:
        sentiment_dist = df.groupBy('sentiment_category').count() \
            .orderBy('count', ascending=False) \
            .collect()
        
        for row in sentiment_dist:
            print("   {}: {:,}".format(row['sentiment_category'], row['count']))
    except Exception as e:
        print("   (Distribution will be calculated after processing)")
    
    # ============================================================
    # STEP 17: SELECT FINAL COLUMNS (WITH SENTIMENT)
    # ============================================================
    print("\n" + "="*60)
    print("Step 17: Selecting Final Columns (Snowflake-ready)")
    print("="*60)
    
    df_final = df.select(
        # IDs
        col('complaint_id'),
        col('CMPLID').alias('nhtsa_complaint_id'),
        col('vehicle_id'),
        
        # Vehicle Info (STANDARDIZED)
        col('make'),
        col('model'),
        col('year'),
        
        # Complaint Details
        col('fail_date'),
        col('date_added'),
        col('mileage'),
        col('vehicle_speed'),
        
        # Component
        col('COMPDESC').alias('component_description'),
        
        # Incident Flags
        col('crash_flag'),
        col('fire_flag'),
        col('injured'),
        col('deaths'),
        col('has_incident'),
        col('severity_level'),
        
        # Text
        col('CDESCR_clean').alias('complaint_description'),
        
        # ============================================================
        # NEW: SENTIMENT ANALYSIS COLUMNS
        # ============================================================
        col('sentiment_score'),
        col('sentiment_label'),
        col('sentiment_confidence'),
        col('sentiment_category'),
        col('complaint_embedding'),
        col('negation_count'),
        col('safety_keyword_count'),
        # ============================================================
        
        # Vehicle Details
        col('VIN').alias('vin'),
        col('DRIVE_TRAIN').alias('drive_train'),
        col('FUEL_TYPE').alias('fuel_type'),
        
        # Other Flags
        col('orig_owner_yn_flag').alias('is_original_owner'),
        col('repaired_yn_flag').alias('was_repaired'),
        col('vehicles_towed_yn_flag').alias('was_towed'),
        
        # Quality
        col('missing_critical_data'),
        
        # Metadata
        col('processed_at'),
        col('data_source'),
        col('processing_version')
    )
    
    # 18. Final validation
    final_count = df_final.count()
    final_cols = len(df_final.columns)
    
    print("\n" + "="*60)
    print("CLEANING SUMMARY")
    print("="*60)
    print("  Initial records: {:,}".format(initial_count))
    print("  Final records: {:,}".format(final_count))
    print("  Retention rate: {:.1f}%".format(final_count / initial_count * 100))
    print("  Initial columns: {}".format(initial_cols))
    print("  Final columns: {} (+6 sentiment columns)".format(final_cols))
    
    # Severity distribution
    print("\n  Severity Distribution:")
    severity_counts = df_final.groupBy('severity_level').count() \
        .orderBy('count', ascending=False).collect()
    for row in severity_counts:
        print("    {}: {:,}".format(row['severity_level'], row['count']))
    
    # Sentiment distribution (final)
    print("\n  Sentiment Distribution:")
    sentiment_counts = df_final.groupBy('sentiment_category').count() \
        .orderBy('count', ascending=False).collect()
    for row in sentiment_counts:
        print("    {}: {:,}".format(row['sentiment_category'], row['count']))
    
    # Manufacturer distribution
    print("\n  Top Manufacturers:")
    mfr_counts = df_final.groupBy('make').count() \
        .orderBy('count', ascending=False).collect()
    for row in mfr_counts:
        print("    {}: {:,}".format(row['make'], row['count']))
    
    # 19. CLEANUP OLD S3 DATA
    print("\n" + "="*60)
    print("WRITING TO S3")
    print("="*60)
    print("Preparing to write cleaned data with sentiment...")
    cleanup_s3_path(spark, output_path)
    
    # 20. Write new data
    print("Writing cleaned data to: {}".format(output_path))
    df_final.write.mode('overwrite').parquet(output_path)
    
    # Cleanup and write CSV
    csv_output_path = output_path.replace('/parquet/', '/csv/')
    cleanup_s3_path(spark, csv_output_path)
    
    print("Writing CSV to: {}".format(csv_output_path))
    df_final.coalesce(1).write.mode('overwrite') \
        .option('header', 'true') \
        .csv(csv_output_path)
    
    print("\n" + "="*60)
    print("✅ COMPLAINTS CLEANING COMPLETE (WITH SENTIMENT)!")
    print("="*60)
    print("Output includes:")
    print("  ✓ Standard columns (make, model, year, etc.)")
    print("  ✓ Sentiment analysis (score, label, confidence)")
    print("  ✓ Embeddings (768-dim vectors)")
    print("  ✓ Linguistic features (negations, safety keywords)")
    print("="*60)
    
    return df_final


def main():
    """
    Main execution function
    """
    load_dotenv()
    
    is_local = os.getenv('SPARK_ENV', 'local') == 'local'
    
    print("\n" + "="*60)
    print("NHTSA COMPLAINTS CLEANING WITH SENTIMENT ANALYSIS")
    print("="*60)
    if is_local:
        print("Running in LOCAL mode")
    
    # Initialize Spark
    builder = SparkSession.builder \
        .appName("NHTSA_Complaints_Cleaning_With_Sentiment") \
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
    INPUT_PATH = "s3a://{}/nhtsa/raw/complaints/*.csv".format(S3_BUCKET)
    OUTPUT_PATH = "s3a://{}/nhtsa/cleaned/complaints/parquet/".format(S3_BUCKET)
    
    print("Input: {}".format(INPUT_PATH))
    print("Output: {}".format(OUTPUT_PATH))
    
    # Run cleaning with sentiment
    df_clean = clean_complaints(spark, INPUT_PATH, OUTPUT_PATH)
    
    # Show sample with sentiment
    print("\n" + "="*60)
    print("SAMPLE DATA (with sentiment)")
    print("="*60)
    df_clean.select(
        'complaint_id', 
        'make', 
        'model', 
        'year', 
        'sentiment_score',
        'sentiment_category',
        'severity_level'
    ).show(10, truncate=False)
    
    # Verify vehicle_id
    print("\nVehicle ID verification (first 5):")
    df_clean.select('vehicle_id', 'make', 'model', 'year').show(5, truncate=False)
    
    # Show sentiment statistics
    print("\nSentiment Statistics:")
    df_clean.select('sentiment_score').describe().show()
    
    spark.stop()


if __name__ == "__main__":
    main()