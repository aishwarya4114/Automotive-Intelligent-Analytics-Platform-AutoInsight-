"""
Clean NHTSA Safety Ratings Data
Reads raw data from S3, cleans it, and writes back to S3 cleaned folder

Output columns standardized for Snowflake joins:
- make, model, year (consistent across all datasets)
- vehicle_id (same generation formula everywhere)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, upper, trim, regexp_replace, to_date, 
    current_timestamp, lit, coalesce, length,
    concat_ws, sha2
)
from pyspark.sql.types import FloatType, IntegerType
from functools import reduce
from operator import add
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

def clean_star_rating(df, star_column):
    """Clean and standardize star rating columns"""
    cleaned_col = f"{star_column}_clean"
    
    df = df.withColumn(
        cleaned_col,
        when(col(star_column).isNull(), None)
        .when(upper(trim(col(star_column).cast("string"))).isin(['NOT RATED', 'N/A', 'NA', '', '-']), None)
        .otherwise(col(star_column).cast(FloatType()))
    )
    
    # Validate range 1-5
    df = df.withColumn(
        cleaned_col,
        when((col(cleaned_col) < 1) | (col(cleaned_col) > 5), None)
        .otherwise(col(cleaned_col))
    )
    
    return df

def clean_boolean_feature(df, feature_column):
    """Clean safety feature columns to boolean"""
    has_col = f"has_{feature_column.lower()}"
    
    df = df.withColumn(
        has_col,
        when(upper(trim(col(feature_column).cast("string"))).isin(['STANDARD', 'OPTIONAL', 'YES']), True)
        .otherwise(False)
    )
    
    return df

def clean_ratings(spark, input_path, output_path):
    """
    Main cleaning function for NHTSA Safety Ratings data
    
    Output standardized for Snowflake:
    - Column names: make, model, year (lowercase, consistent)
    - vehicle_id: Generated from (make, model, year)
    """
    
    print("="*60)
    print("NHTSA SAFETY RATINGS - SPARK CLEANING PIPELINE")
    print("Input: 2020-2025 data (pre-filtered)")
    print("Target: Top 15 manufacturers")
    print("Output: Snowflake-ready with standardized columns")
    print("="*60)
    
    # Read raw data
    print(f"\n📥 Reading raw data from: {input_path}")
    df = spark.read \
        .option("header", "true") \
        .option("sep", ",") \
        .option("inferSchema", "true") \
        .option("charset", "ISO-8859-1") \
        .csv(input_path)
    
    initial_count = df.count()
    initial_cols = len(df.columns)
    print(f"  ✓ Loaded {initial_count:,} records")
    print(f"  ✓ Initial columns: {initial_cols}")
    
    # 1. Select relevant columns
    print("\n🔧 Step 1: Selecting relevant columns...")
    
    relevant_columns = [
        'MAKE', 'MODEL', 'MODEL_YR', 'BODY_STYLE', 'VEHICLE_TYPE', 
        'VEHICLE_CLASS', 'DRIVE_TRAIN', 'NUM_OF_SEATING',
        'CURB_WEIGHT', 'MIN_GROSS_WEIGHT', 'MAX_GROSS_WEIGHT',
        'OVERALL_STARS', 'OVERALL_FRNT_STARS', 'OVERALL_SIDE_STARS',
        'ROLLOVER_STARS', 'FRNT_DRIV_STARS', 'FRNT_PASS_STARS',
        'SIDE_DRIV_STARS', 'SIDE_PASS_STARS', 'SIDE_BARRIER_STAR',
        'SIDE_POLE_STARS',
        'ABS', 'NHTSA_ESC', 'BLIND_SPOT_DETECTION', 'NHTSA_BACKUP_CAMERA',
        'FRNT_COLLISION_WARNING', 'LANE_DEPARTURE_WARNING',
        'CRASH_IMMINENT_BRAKE', 'DYNAMIC_BRAKE_SUPPORT',
        'ADAPTIVE_CRUISE_CONTROL', 'DAY_RUN_LIGHTS',
        'HEAD_SAB', 'TORSO_SAB', 'PELVIS_SAB',
        'FRNT_TEST_NO', 'SIDE_TEST_NO', 'POLE_TEST_NO',
        'PRODUCTION_RELEASE'
    ]
    
    available_cols = [c for c in relevant_columns if c in df.columns]
    df = df.select(*available_cols)
    print(f"  ✓ Selected {len(available_cols)} columns")
    
    # 2. Standardize manufacturer
    print("\n🔧 Step 2: Standardizing manufacturer names...")
    df = standardize_manufacturer_names(df, 'MAKE')
    
    # 3. Filter to top 15
    print("\n🔧 Step 3: Filtering to top 15 manufacturers...")
    df = filter_top_manufacturers(df, 'MAKE_standardized')
    
    # 4. Clean MODEL_YR
    print("\n🔧 Step 4: Cleaning MODEL_YR...")
    df = df.withColumn(
        'year_clean',
        when(col('MODEL_YR').isNull(), None)
        .otherwise(col('MODEL_YR').cast(IntegerType()))
    )
    
    df = df.withColumn(
        'year_clean',
        when((col('year_clean') < 1990) | (col('year_clean') > 2030), None)
        .otherwise(col('year_clean'))
    )
    
    # 5. Clean MODEL
    print("\n🔧 Step 5: Cleaning MODEL field...")
    df = clean_text_field(df, 'MODEL')
    
    # 6. Clean star ratings
    print("\n🔧 Step 6: Cleaning star rating columns...")
    star_rating_columns = [
        'OVERALL_STARS', 'OVERALL_FRNT_STARS', 'OVERALL_SIDE_STARS',
        'ROLLOVER_STARS', 'FRNT_DRIV_STARS', 'FRNT_PASS_STARS',
        'SIDE_DRIV_STARS', 'SIDE_PASS_STARS', 'SIDE_BARRIER_STAR',
        'SIDE_POLE_STARS'
    ]
    
    for star_col in star_rating_columns:
        if star_col in df.columns:
            df = clean_star_rating(df, star_col)
    
    # 7. Calculate average star rating
    print("\n🔧 Step 7: Calculating average star rating...")
    clean_star_cols = [f"{c}_clean" for c in star_rating_columns if f"{c}_clean" in df.columns]
    
    if clean_star_cols:
        star_sum = reduce(add, [coalesce(col(c), lit(0)) for c in clean_star_cols])
        star_count = reduce(add, [when(col(c).isNotNull(), 1).otherwise(0) for c in clean_star_cols])
        
        df = df.withColumn(
            'average_star_rating',
            when(star_count > 0, star_sum / star_count).otherwise(None)
        )
    else:
        df = df.withColumn('average_star_rating', lit(None))
    
    # 8. Convert safety features to booleans
    print("\n🔧 Step 8: Converting safety features to booleans...")
    safety_features = [
        'ABS', 'NHTSA_ESC', 'BLIND_SPOT_DETECTION', 'NHTSA_BACKUP_CAMERA',
        'FRNT_COLLISION_WARNING', 'LANE_DEPARTURE_WARNING',
        'CRASH_IMMINENT_BRAKE', 'DYNAMIC_BRAKE_SUPPORT',
        'ADAPTIVE_CRUISE_CONTROL', 'DAY_RUN_LIGHTS'
    ]
    
    for feature in safety_features:
        if feature in df.columns:
            df = clean_boolean_feature(df, feature)
    
    # 9. Count advanced safety features
    print("\n🔧 Step 9: Counting advanced safety features...")
    advanced_features = [
        'has_frnt_collision_warning', 'has_lane_departure_warning',
        'has_crash_imminent_brake', 'has_blind_spot_detection'
    ]
    
    existing_advanced_features = [f for f in advanced_features if f in df.columns]
    
    if existing_advanced_features:
        safety_count_expr = reduce(
            add, 
            [when(col(f) == True, 1).otherwise(0) for f in existing_advanced_features]
        )
        df = df.withColumn('advanced_safety_count', safety_count_expr)
    else:
        df = df.withColumn('advanced_safety_count', lit(0))
    
    df = df.withColumn(
        'has_advanced_safety',
        when(col('advanced_safety_count') >= 2, True).otherwise(False)
    )
    
    # 10. Clean weight fields
    print("\n🔧 Step 10: Cleaning weight fields...")
    weight_cols = ['CURB_WEIGHT', 'MIN_GROSS_WEIGHT', 'MAX_GROSS_WEIGHT']
    
    for weight_col in weight_cols:
        if weight_col in df.columns:
            cleaned_weight = f"{weight_col.lower()}_clean"
            df = df.withColumn(
                cleaned_weight,
                when(col(weight_col).isNull(), None)
                .when((col(weight_col) < 1000) | (col(weight_col) > 15000), None)
                .otherwise(col(weight_col).cast(FloatType()))
            )
    
    # 11. Standardize body style
    print("\n🔧 Step 11: Standardizing body style and vehicle class...")
    
    if 'BODY_STYLE' in df.columns:
        df = df.withColumn(
            'body_style_clean',
            upper(trim(regexp_replace(col('BODY_STYLE'), '[^a-zA-Z0-9\\s]', '_')))
        )
    
    if 'VEHICLE_CLASS' in df.columns:
        df = df.withColumn(
            'vehicle_class_clean',
            upper(trim(col('VEHICLE_CLASS')))
        )
    
    # 12. Create safety tier
    print("\n🔧 Step 12: Creating safety tier categories...")
    df = df.withColumn(
        'safety_tier',
        when(col('average_star_rating') >= 4.5, 'EXCELLENT')
        .when(col('average_star_rating') >= 4.0, 'GOOD')
        .when(col('average_star_rating') >= 3.0, 'ACCEPTABLE')
        .when(col('average_star_rating') >= 2.0, 'MARGINAL')
        .when(col('average_star_rating').isNotNull(), 'POOR')
        .otherwise('NOT_RATED')
    )
    
    # 13. Data quality flags
    print("\n🔧 Step 13: Adding data quality flags...")
    df = df.withColumn(
        'missing_critical_data',
        when(
            col('MAKE_standardized').isNull() | 
            col('year_clean').isNull(),
            True
        ).otherwise(False)
    )
    
    df = df.withColumn(
        'has_test_data',
        when(col('average_star_rating').isNotNull(), True).otherwise(False)
    )
    
    # 14. Remove duplicates
    print("\n🔧 Step 14: Removing duplicates...")
    before_dedup = df.count()
    df = remove_duplicates(df, ['MAKE_standardized', 'MODEL_clean', 'year_clean'])
    after_dedup = df.count()
    duplicates_removed = before_dedup - after_dedup
    print(f"  ✓ Removed {duplicates_removed:,} duplicate records")
    
    # 15. STANDARDIZE COLUMN NAMES FOR SNOWFLAKE
    print("\n🔧 Step 15: Standardizing column names for Snowflake...")
    
    # Rename to standard names: make, model, year
    df = df.withColumn('make', col('MAKE_standardized'))
    df = df.withColumn('model', col('MODEL_clean'))
    df = df.withColumn('year', col('year_clean'))
    
    # 16. Create vehicle_id using STANDARDIZED columns
    print("\n🔧 Step 16: Creating standardized vehicle_id...")
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
    
    # Create rating_id (use vehicle_id + drive_train for uniqueness)
    df = df.withColumn(
        'rating_id',
        sha2(
            concat_ws("_",
                col('vehicle_id'),
                coalesce(col('DRIVE_TRAIN').cast("string"), lit(""))
            ),
            256
        )
    )
    
    # 17. Add metadata
    print("\n🔧 Step 17: Adding metadata...")
    df = add_processing_metadata(df, 'NHTSA_RATINGS')
    
    # 18. Select final columns with STANDARDIZED NAMES
    print("\n🔧 Step 18: Selecting final columns (Snowflake-ready)...")
    
    df_final = df.select(
        # IDs
        col('rating_id'),
        col('vehicle_id'),
        
        # Vehicle Info (STANDARDIZED for joins!)
        col('make'),                                        # ✅ Standard
        col('model'),                                       # ✅ Standard
        col('year'),                                        # ✅ Standard
        col('body_style_clean').alias('body_style'),
        col('vehicle_class_clean').alias('vehicle_class'),
        col('DRIVE_TRAIN').alias('drive_train'),
        col('NUM_OF_SEATING').alias('num_of_seating'),
        
        # Weight
        col('curb_weight_clean').alias('curb_weight'),
        col('min_gross_weight_clean').alias('min_gross_weight'),
        col('max_gross_weight_clean').alias('max_gross_weight'),
        
        # Star Ratings (standardized names)
        col('OVERALL_STARS_clean').alias('overall_stars'),
        col('OVERALL_FRNT_STARS_clean').alias('overall_front_stars'),
        col('OVERALL_SIDE_STARS_clean').alias('overall_side_stars'),
        col('ROLLOVER_STARS_clean').alias('rollover_stars'),
        col('FRNT_DRIV_STARS_clean').alias('front_driver_stars'),
        col('FRNT_PASS_STARS_clean').alias('front_passenger_stars'),
        col('SIDE_DRIV_STARS_clean').alias('side_driver_stars'),
        col('SIDE_PASS_STARS_clean').alias('side_passenger_stars'),
        col('average_star_rating'),
        col('safety_tier'),
        
        # Safety Features (boolean)
        col('has_abs'),
        col('has_nhtsa_esc').alias('has_esc'),
        col('has_blind_spot_detection'),
        col('has_nhtsa_backup_camera').alias('has_backup_camera'),
        col('has_frnt_collision_warning').alias('has_front_collision_warning'),
        col('has_lane_departure_warning'),
        col('has_crash_imminent_brake'),
        col('has_dynamic_brake_support'),
        col('has_adaptive_cruise_control'),
        col('has_advanced_safety'),
        col('advanced_safety_count'),
        
        # Quality
        col('missing_critical_data'),
        col('has_test_data'),
        
        # Metadata
        col('processed_at'),
        col('data_source'),
        col('processing_version')
    )
    
    # 19. Final validation
    final_count = df_final.count()
    final_cols = len(df_final.columns)
    
    print(f"\n📊 Cleaning Summary:")
    print(f"  Initial records: {initial_count:,}")
    print(f"  Final records: {final_count:,}")
    print(f"  Retention rate: {(final_count / initial_count * 100):.1f}%")
    print(f"  Initial columns: {initial_cols}")
    print(f"  Final columns: {final_cols}")
    
    # Safety tier distribution
    print(f"\n  Safety Tier Distribution:")
    tier_counts = df_final.groupBy('safety_tier').count().orderBy('count', ascending=False).collect()
    for row in tier_counts:
        print(f"    {row['safety_tier']}: {row['count']:,}")
    
    # Manufacturer distribution
    print(f"\n  Top Manufacturers:")
    mfr_counts = df_final.groupBy('make').count() \
        .orderBy('count', ascending=False).collect()
    for row in mfr_counts:
        print(f"    {row['make']}: {row['count']:,}")
    
    # 20. CLEANUP OLD S3 DATA (only from /cleaned/ path)
    print(f"\n💾 Preparing to write cleaned data...")
    
    # Delete old parquet files
    cleanup_s3_path(spark, output_path)
    
    # 21. Write new data
    print(f"💾 Writing cleaned data to: {output_path}")
    df_final.write.mode('overwrite').parquet(output_path)
    
    # Cleanup and write CSV
    csv_output_path = output_path.replace('/parquet/', '/csv/')
    cleanup_s3_path(spark, csv_output_path)
    
    print(f"💾 Writing CSV to: {csv_output_path}")
    df_final.coalesce(1).write.mode('overwrite') \
        .option('header', 'true') \
        .csv(csv_output_path)
    
    print("\n✅ Ratings cleaning complete!")
    print("="*60)
    
    return df_final


def main():
    """Main execution function - environment-aware"""
    load_dotenv()
    
    is_local = os.getenv('SPARK_ENV', 'local') == 'local'
    
    print(f"\n🚀 Starting NHTSA Ratings Cleaning")
    if is_local:
        print("🔧 Running in LOCAL mode")
    
    # Initialize Spark
    builder = SparkSession.builder \
        .appName("NHTSA_Ratings_Cleaning") \
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
    INPUT_PATH = f"s3a://{S3_BUCKET}/nhtsa/raw/ratings/*.csv"
    OUTPUT_PATH = f"s3a://{S3_BUCKET}/nhtsa/cleaned/ratings/parquet/"
    
    print(f"📥 Input: {INPUT_PATH}")
    print(f"📤 Output: {OUTPUT_PATH}")
    
    # Run cleaning
    df_clean = clean_ratings(spark, INPUT_PATH, OUTPUT_PATH)
    
    # Show sample with STANDARDIZED column names
    print("\n📋 Sample of cleaned data (Snowflake-ready):")
    df_clean.select(
        'rating_id', 'make', 'model', 'year', 
        'safety_tier', 'average_star_rating'
    ).show(10, truncate=False)
    
    # Verify vehicle_id consistency
    print("\n🔍 Vehicle ID verification (first 5):")
    df_clean.select('vehicle_id', 'make', 'model', 'year').show(5, truncate=False)
    
    spark.stop()


if __name__ == "__main__":
    main()