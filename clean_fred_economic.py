"""
Clean FRED Economic Indicators
Simple transformation: pivot long to wide format and write to cleaned folder
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit, current_timestamp, max as spark_max
from dotenv import load_dotenv
import os

load_dotenv()

def clean_fred_data(spark, input_path, output_path):
    """
    Pivot FRED data from long to wide format
    Simple cleaning - data is already high quality
    """
    
    print("="*60)
    print("FRED ECONOMIC INDICATORS - CLEANING")
    print("="*60)
    
    print("\nReading raw data from: {}".format(input_path))
    
    # Read combined file
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(input_path)
    
    initial_count = df.count()
    print("  Loaded {:,} records (long format)".format(initial_count))
    
    # Pivot from long to wide format
    print("\nPivoting from long to wide format...")
    df_wide = df.groupBy('date').pivot('series_id').agg(spark_max('value'))
    
    # Rename columns to standard names
    df_final = df_wide.select(
        to_date(col('date'), 'yyyy-MM-dd').alias('date'),
        col('TOTALSA').cast('float').alias('total_vehicle_sales'),
        col('ALTSALES').cast('float').alias('light_truck_sales'),
        col('DAUTOSAAR').cast('float').alias('domestic_auto_sales')
    )
    
    # Add metadata
    df_final = df_final.withColumn('data_source', lit('FRED'))
    df_final = df_final.withColumn('loaded_at', current_timestamp())
    
    # Sort by date
    df_final = df_final.orderBy('date')
    
    final_count = df_final.count()
    print("  Pivoted to {:,} rows (wide format)".format(final_count))
    
    # Show summary
    print("\nCleaning Summary:")
    print("  Initial records: {:,} (long format)".format(initial_count))
    print("  Final records: {:,} (wide format)".format(final_count))
    print("  Date range: {} to {}".format(
        df_final.select('date').first()[0],
        df_final.select('date').orderBy(col('date').desc()).first()[0]
    ))
    
    # Write to S3 cleaned folder
    print("\nWriting cleaned data to: {}".format(output_path))
    df_final.write.mode('overwrite').parquet(output_path)
    
    # Also write CSV
    csv_output_path = output_path.replace('/parquet/', '/csv/')
    print("Writing CSV to: {}".format(csv_output_path))
    df_final.coalesce(1).write.mode('overwrite') \
        .option('header', 'true') \
        .csv(csv_output_path)
    
    print("\nFRED cleaning complete!")
    print("="*60)
    
    return df_final


def main():
    """Main execution"""
    load_dotenv()
    
    is_local = os.getenv('SPARK_ENV', 'local') == 'local'
    
    print("\nStarting FRED Economic Indicators Cleaning")
    if is_local:
        print("Running in LOCAL mode")
    
    # Initialize Spark
    builder = SparkSession.builder \
        .appName("FRED_Economic_Cleaning") \
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
    INPUT_PATH = "s3a://{}/fred/raw/fred_economic_indicators.csv".format(S3_BUCKET)
    OUTPUT_PATH = "s3a://{}/fred/cleaned/economic_indicators/parquet/".format(S3_BUCKET)
    
    print("Input: {}".format(INPUT_PATH))
    print("Output: {}".format(OUTPUT_PATH))
    
    # Run cleaning
    df_clean = clean_fred_data(spark, INPUT_PATH, OUTPUT_PATH)
    
    # Show sample
    print("\nSample of cleaned data (Snowflake-ready):")
    df_clean.show(10, truncate=False)
    
    spark.stop()


if __name__ == "__main__":
    main()