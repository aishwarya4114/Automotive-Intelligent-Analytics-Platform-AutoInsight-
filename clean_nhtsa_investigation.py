"""
Clean NHTSA Investigations Data
Reads raw data from S3, cleans it, and writes back to S3 cleaned folder

Output columns standardized for Snowflake joins:
- make, model, year (consistent across all datasets)
- vehicle_id (same generation formula everywhere)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, upper, trim, regexp_replace, to_date, 
    current_timestamp, lit, datediff, year, length, coalesce,
    concat_ws, sha2
)
from pyspark.sql.types import IntegerType
from dotenv import load_dotenv
import sys
import os

# Add utils to path
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

def normalize_component_name(df):
    """
    Normalize component names to standardized categories
    """
    df = df.withColumn(
        'component_category',
        when(upper(col('COMPNAME')).contains('BRAKE'), lit('BRAKING_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('ENGINE'), lit('ENGINE_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('POWERTRAIN'), lit('ENGINE_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('FUEL'), lit('ENGINE_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('AIRBAG'), lit('RESTRAINT_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('AIR BAG'), lit('RESTRAINT_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('SEAT BELT'), lit('RESTRAINT_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('SRS'), lit('RESTRAINT_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('ELECTRICAL'), lit('ELECTRICAL_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('WIRING'), lit('ELECTRICAL_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('BATTERY'), lit('ELECTRICAL_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('STEERING'), lit('STEERING_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('SUSPENSION'), lit('STEERING_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('TIRE'), lit('TIRE_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('WHEEL'), lit('TIRE_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('FORWARD COLLISION'), lit('ADAS_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('LANE DEPARTURE'), lit('ADAS_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('ADAS'), lit('ADAS_SYSTEM'))
        .when(upper(col('COMPNAME')).contains('AUTOPILOT'), lit('ADAS_SYSTEM'))
        .otherwise(lit('OTHER'))
    )
    
    return df

def clean_investigations(spark, input_path, output_path):
    """
    Main cleaning function for NHTSA Investigations data
    
    Output standardized for Snowflake:
    - Column names: make, model, year (lowercase, consistent)
    - vehicle_id: Generated from (make, model, year)
    """
    
    print("="*60)
    print("NHTSA INVESTIGATIONS - SPARK CLEANING PIPELINE")
    print("Input: 2020-2025 data (pre-filtered)")
    print("Target: Top 15 manufacturers")
    print("Output: Snowflake-ready with standardized columns")
    print("="*60)
    
    # Read raw data from S3
    print(f"\n📥 Reading raw data from: {input_path}")
    
    df = spark.read \
        .option("header", "true") \
        .option("sep", ",") \
        .option("inferSchema", "true") \
        .option("charset", "ISO-8859-1") \
        .csv(input_path)
    
    initial_count = df.count()
    print(f"  ✓ Loaded {initial_count:,} records (2020-2025)")
    
    # 1. Standardize manufacturer names
    print("\n🔧 Step 1: Standardizing manufacturer names...")
    df = standardize_manufacturer_names(df, 'MAKE')
    df = standardize_manufacturer_names(df, 'MFR_NAME')
    
    # 2. Filter to top 15 manufacturers
    print("\n🔧 Step 2: Filtering to top 15 manufacturers...")
    df = filter_top_manufacturers(df, 'MAKE_standardized')
    
    # 3. Clean and validate YEAR
    print("\n🔧 Step 3: Validating YEAR field...")
    df = df.withColumn(
        'year_clean',
        when(col('YEAR') == '9999', None)
        .when(col('YEAR') == 9999, None)
        .when(col('YEAR').isNull(), None)
        .otherwise(col('YEAR').cast(IntegerType()))
    )
    
    df = df.withColumn(
        'year_clean',
        when((col('year_clean') < 1990) | (col('year_clean') > 2030), None)
        .otherwise(col('year_clean'))
    )
    
    # 4. Clean date fields
    print("\n🔧 Step 4: Cleaning date fields...")
    df = clean_date_field(df, 'ODATE')
    df = clean_date_field(df, 'CDATE')
    
    df = df.withColumn('open_year', year(col('ODATE_clean')))
    
    # 5. Clean text fields
    print("\n🔧 Step 5: Cleaning text fields...")
    df = clean_text_field(df, 'SUBJECT')
    df = clean_text_field(df, 'SUMMARY')
    df = clean_text_field(df, 'MODEL')
    
    # 6. Normalize component names
    print("\n🔧 Step 6: Normalizing component names...")
    df = normalize_component_name(df)
    
    # 7. Create calculated fields
    print("\n🔧 Step 7: Creating calculated fields...")
    
    df = df.withColumn(
        'investigation_status',
        when(col('CDATE_clean').isNull(), 'OPEN').otherwise('CLOSED')
    )
    
    df = df.withColumn(
        'investigation_duration_days',
        when(col('CDATE_clean').isNotNull(),
             datediff(col('CDATE_clean'), col('ODATE_clean')))
        .otherwise(None)
    )
    
    df = df.withColumn(
        'long_investigation_flag',
        when(col('investigation_duration_days') > 365, True).otherwise(False)
    )
    
    # 8. Handle campaign numbers
    print("\n🔧 Step 8: Processing campaign numbers...")
    df = df.withColumn(
        'has_recall',
        when(col('CAMPNO').isNotNull() & (trim(col('CAMPNO')) != ''), True)
        .otherwise(False)
    )
    
    df = df.withColumn(
        'campno_clean',
        when(col('CAMPNO').isNotNull(), trim(col('CAMPNO')))
        .otherwise(None)
    )
    
    # 9. Data quality flags
    print("\n🔧 Step 9: Adding data quality flags...")
    df = df.withColumn(
        'missing_critical_data',
        when(
            col('MAKE_standardized').isNull() | 
            col('year_clean').isNull() | 
            col('component_category').isNull(),
            True
        ).otherwise(False)
    )
    
    df = df.withColumn(
        'date_inconsistency',
        when(
            col('CDATE_clean').isNotNull() & 
            (col('CDATE_clean') < col('ODATE_clean')),
            True
        ).otherwise(False)
    )
    
    # 10. Remove duplicates
    print("\n🔧 Step 10: Removing duplicates...")
    before_dedup = df.count()
    df = remove_duplicates(df, ['NHTSA_ACTION_NUMBER'])
    after_dedup = df.count()
    duplicates_removed = before_dedup - after_dedup
    print(f"  ✓ Removed {duplicates_removed:,} duplicate records")
    
    # 11. STANDARDIZE COLUMN NAMES FOR SNOWFLAKE
    print("\n🔧 Step 11: Standardizing column names for Snowflake...")
    
    # Rename to standard names: make, model, year
    df = df.withColumn('make', col('MAKE_standardized'))
    df = df.withColumn('model', col('MODEL_clean'))
    df = df.withColumn('year', col('year_clean'))
    df = df.withColumn('manufacturer_name', col('MFR_NAME_standardized'))
    
    # 12. Create vehicle_id using STANDARDIZED columns
    print("\n🔧 Step 12: Creating standardized vehicle_id...")
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
    
    # Create investigation_id
    df = df.withColumn(
        'investigation_id',
        sha2(col('NHTSA_ACTION_NUMBER').cast("string"), 256)
    )
    
    # 13. Add processing metadata
    print("\n🔧 Step 13: Adding metadata...")
    df = add_processing_metadata(df, 'NHTSA_INVESTIGATIONS')
    
    # 14. Select final columns with STANDARDIZED NAMES
    print("\n🔧 Step 14: Selecting final columns (Snowflake-ready)...")
    
    df_final = df.select(
        # IDs
        col('investigation_id'),
        col('NHTSA_ACTION_NUMBER').alias('nhtsa_action_number'),
        col('vehicle_id'),
        
        # Vehicle Info (STANDARDIZED for joins!)
        col('make'),                    # ✅ Standard
        col('manufacturer_name'),       # ✅ Standard
        col('model'),                   # ✅ Standard
        col('year'),                    # ✅ Standard
        
        # Component
        col('COMPNAME').alias('component_name'),
        col('component_category'),
        
        # Dates (standardized naming)
        col('ODATE_clean').alias('open_date'),
        col('CDATE_clean').alias('close_date'),
        col('open_year'),
        
        # Campaign
        col('campno_clean').alias('campaign_number'),
        col('has_recall'),
        
        # Text
        col('SUBJECT_clean').alias('subject'),
        col('SUMMARY_clean').alias('summary'),
        
        # Calculated
        col('investigation_status'),
        col('investigation_duration_days'),
        col('long_investigation_flag'),
        
        # Quality Flags
        col('missing_critical_data'),
        col('date_inconsistency'),
        
        # Metadata
        col('processed_at'),
        col('data_source'),
        col('processing_version')
    )
    
    # 15. Final validation
    final_count = df_final.count()
    print(f"\n📊 Cleaning Summary:")
    print(f"  Initial records: {initial_count:,}")
    print(f"  Final records: {final_count:,}")
    print(f"  Records dropped: {initial_count - final_count:,}")
    print(f"  Retention rate: {(final_count / initial_count * 100):.1f}%")
    
    # Quality stats
    quality_stats = df_final.select(
        (col('missing_critical_data') == True).cast('int').alias('missing_data'),
        (col('date_inconsistency') == True).cast('int').alias('date_issues'),
        (col('investigation_status') == 'OPEN').cast('int').alias('open_investigations')
    ).agg({
        'missing_data': 'sum',
        'date_issues': 'sum',
        'open_investigations': 'sum'
    }).collect()[0]
    
    print(f"\n  Data Quality:")
    print(f"    Missing critical data: {quality_stats[0]:,}")
    print(f"    Date inconsistencies: {quality_stats[1]:,}")
    print(f"    Open investigations: {quality_stats[2]:,}")
    
    # Manufacturer distribution
    print(f"\n  Top Manufacturers:")
    mfr_counts = df_final.groupBy('make').count() \
        .orderBy('count', ascending=False).collect()
    for row in mfr_counts:
        print(f"    {row['make']}: {row['count']:,}")
    
    # 16. CLEANUP OLD S3 DATA (only from /cleaned/ path)
    print(f"\n💾 Preparing to write cleaned data...")
    
    # Delete old parquet files
    cleanup_s3_path(spark, output_path)
    
    # 17. Write new data to S3
    print(f"💾 Writing cleaned data to: {output_path}")
    df_final.write.mode('overwrite').parquet(output_path)
    
    # Cleanup and write CSV
    csv_output_path = output_path.replace('/parquet/', '/csv/')
    cleanup_s3_path(spark, csv_output_path)
    
    print(f"💾 Writing CSV to: {csv_output_path}")
    df_final.coalesce(1).write.mode('overwrite') \
        .option('header', 'true') \
        .csv(csv_output_path)
    
    print("\n✅ Investigations cleaning complete!")
    print("="*60)
    
    return df_final


def main():
    """
    Main execution function - environment-aware
    """
    load_dotenv()
    
    is_local = os.getenv('SPARK_ENV', 'local') == 'local'
    
    print(f"\n🚀 Starting NHTSA Investigations Cleaning")
    if is_local:
        print("🔧 Running in LOCAL mode")
    
    # Initialize Spark
    builder = SparkSession.builder \
        .appName("NHTSA_Investigations_Cleaning") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    # Add S3 JARs for local mode
    if is_local:
        print("📦 Loading AWS S3 connectors...")
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
    INPUT_PATH = f"s3a://{S3_BUCKET}/nhtsa/raw/investigations/*.csv"
    OUTPUT_PATH = f"s3a://{S3_BUCKET}/nhtsa/cleaned/investigations/parquet/"
    
    print(f"📥 Input: {INPUT_PATH}")
    print(f"📤 Output: {OUTPUT_PATH}")
    
    # Run cleaning
    df_clean = clean_investigations(spark, INPUT_PATH, OUTPUT_PATH)
    
    # Show sample with STANDARDIZED column names
    print("\n📋 Sample of cleaned data (Snowflake-ready):")
    df_clean.select(
        'investigation_id', 'make', 'model', 'year', 
        'investigation_status', 'open_date'
    ).show(10, truncate=False)
    
    # Verify vehicle_id consistency
    print("\n🔍 Vehicle ID verification (first 5):")
    df_clean.select('vehicle_id', 'make', 'model', 'year').show(5, truncate=False)
    
    spark.stop()


if __name__ == "__main__":
    main()