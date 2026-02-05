"""
Common transformation functions used across multiple cleaning scripts
Optimized for top 15 manufacturers, 2020-2025 data
"""
 
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, upper, trim, when, regexp_replace, to_date, 
    current_timestamp, lit, length, coalesce, year
)

def standardize_manufacturer_names(df: DataFrame, make_column: str) -> DataFrame:
    """
    Standardize manufacturer names
    Uses explicit column references to avoid Spark parsing issues
    """
    
    # Create expression using explicit column reference
    make_col = col(make_column)
    make_upper = upper(trim(make_col))
    
    df = df.withColumn(
        f"{make_column}_standardized",
        
        # Tesla
        when(make_upper.isin('TESLA', 'TESLA MOTORS', 'TESLA, INC.', 'TESLA INC.'), lit('Tesla'))
        
        # Ford
        .when(make_upper.isin('FORD', 'FORD MOTOR COMPANY', 'FORD MOTOR CO.'), lit('Ford'))
        
        # Toyota
        .when(make_upper.isin('TOYOTA', 'TOYOTA MOTOR CORPORATION', 'TOYOTA MOTOR CORP', 'TOYOTA MOTOR CORP.'), lit('Toyota'))
        
        # GMC / General Motors
        .when(make_upper.isin('GMC', 'GENERAL MOTORS', 'GM', 'GENERAL MOTORS LLC', 'GENERAL MOTORS, LLC'), lit('GMC'))
        
        # Honda
        .when(make_upper.isin('HONDA', 'HONDA MOTOR CO.', 'HONDA (AMERICAN HONDA MOTOR CO.)', 'AMERICAN HONDA MOTOR CO.'), lit('Honda'))
        
        # Chevrolet
        .when(make_upper.isin('CHEVROLET', 'CHEVY'), lit('Chevrolet'))
        
        # Nissan
        .when(make_upper.isin('NISSAN', 'NISSAN NORTH AMERICA, INC.', 'NISSAN MOTOR COMPANY'), lit('Nissan'))
        
        # BMW
        .when(make_upper.isin('BMW', 'BMW OF NORTH AMERICA', 'BAYERISCHE MOTOREN WERKE'), lit('BMW'))
        
        # Mercedes-Benz
        .when(make_upper.isin('MERCEDES-BENZ', 'MERCEDES BENZ', 'MERCEDES-BENZ USA, LLC', 'DAIMLER', 'DAIMLER AG'), lit('Mercedes-Benz'))
        
        # Volkswagen
        .when(make_upper.isin('VOLKSWAGEN', 'VW'), lit('Volkswagen'))
        
        # Hyundai
        .when(make_upper.isin('HYUNDAI', 'HYUNDAI MOTOR AMERICA'), lit('Hyundai'))
        
        # Kia
        .when(make_upper.isin('KIA', 'KIA AMERICA, INC.', 'KIA MOTORS AMERICA'), lit('Kia'))
        
        # Subaru
        .when(make_upper.isin('SUBARU', 'SUBARU OF AMERICA'), lit('Subaru'))
        
        # Mazda
        .when(make_upper.isin('MAZDA'), lit('Mazda'))
        
        # Jeep
        .when(make_upper.isin('JEEP', 'FCA US LLC', 'FCA US, LLC', 'CHRYSLER (FCA US, LLC)', 'STELLANTIS'), lit('Jeep'))
        
        # Keep original if no match
        .otherwise(make_col)
    )
    
    return df
 
def clean_date_field(df: DataFrame, date_column: str, format: str = "yyyyMMdd") -> DataFrame:
    """
    Clean and standardize date fields with validation
    """
    cleaned_col_name = f"{date_column}_clean"
    
    df = df.withColumn(
        cleaned_col_name,
        when(col(date_column).isNull(), None)
        .when(col(date_column) == '99999999', None)
        .when(col(date_column) == '00000000', None)
        .when(col(date_column) == '', None)
        .when(col(date_column) == ' ', None)
        .when(length(col(date_column).cast("string")) != 8, None)
        .otherwise(to_date(col(date_column).cast("string"), format))
    )
    
    # Light validation
    df = df.withColumn(
        cleaned_col_name,
        when(year(col(cleaned_col_name)) < 1990, None)
        .otherwise(col(cleaned_col_name))
    )
    
    return df
 
def remove_duplicates(df: DataFrame, subset_columns: list) -> DataFrame:
    """Remove duplicate rows"""
    return df.dropDuplicates(subset_columns)
 
def clean_text_field(df: DataFrame, text_column: str) -> DataFrame:
    """
    Clean text fields: remove HTML, fix encoding, convert empty to NULL
    """
    cleaned_col_name = f"{text_column}_clean"
    
    # Remove HTML tags
    df = df.withColumn(
        cleaned_col_name,
        regexp_replace(col(text_column), '<[^>]*>', '')
    )
    
    # Fix encoding issues
    df = df.withColumn(
        cleaned_col_name,
        regexp_replace(col(cleaned_col_name), 'Ã¯Â¬', 'fi')
    )
    df = df.withColumn(
        cleaned_col_name,
        regexp_replace(col(cleaned_col_name), 'â€"', '-')
    )
    df = df.withColumn(
        cleaned_col_name,
        regexp_replace(col(cleaned_col_name), 'â€™', "'")
    )
    
    # Remove excessive whitespace and trim
    df = df.withColumn(
        cleaned_col_name,
        trim(regexp_replace(col(cleaned_col_name), '\\s+', ' '))
    )
    
    # Convert empty/space-only strings to NULL
    df = df.withColumn(
        cleaned_col_name,
        when(
            col(cleaned_col_name).isNull() | 
            (col(cleaned_col_name) == '') | 
            (col(cleaned_col_name) == ' ') |
            (length(col(cleaned_col_name)) == 0),
            None
        ).otherwise(col(cleaned_col_name))
    )
    
    return df
 
def add_processing_metadata(df: DataFrame, source_name: str) -> DataFrame:
    """Add metadata columns"""
    return df.withColumn("processed_at", current_timestamp()) \
             .withColumn("data_source", lit(source_name)) \
             .withColumn("processing_version", lit("1.0"))
 
def create_surrogate_key(df: DataFrame, key_columns: list, output_column: str) -> DataFrame:
    """Create surrogate key from multiple columns using hash"""
    from pyspark.sql.functions import concat_ws, sha2
    
    return df.withColumn(
        output_column,
        sha2(concat_ws("_", *[coalesce(col(c).cast("string"), lit("")) for c in key_columns]), 256)
    )

def filter_top_manufacturers(df: DataFrame, make_column_standardized: str = 'MAKE_standardized') -> DataFrame:
    """Filter to keep only top 15 manufacturers"""
    from .manufacturer_mapping import VALID_MANUFACTURERS
    
    print(f"  📊 Filtering to top 15 manufacturers...")
    before_count = df.count()
    
    df_filtered = df.filter(col(make_column_standardized).isin(VALID_MANUFACTURERS))
    
    after_count = df_filtered.count()
    filtered_out = before_count - after_count
    
    print(f"  ✓ Kept {after_count:,} records from top 15 manufacturers")
    print(f"  ✓ Filtered out {filtered_out:,} records from other manufacturers")
    
    return df_filtered

def cleanup_s3_path(spark, s3_path: str) -> None:
    """
    Delete existing CLEANED data at S3 path before writing new data
    ONLY deletes from /cleaned/ paths - will not touch /raw/ data
    """
    try:
        import boto3
        from botocore.exceptions import ClientError
        import os
        from urllib.parse import urlparse
        
        # Safety check - only allow deletion from /cleaned/ paths
        if '/cleaned/' not in s3_path:
            print(f"   Safety check: Will not delete from non-cleaned path: {s3_path}")
            return
        
        # Parse S3 path
        parsed = urlparse(s3_path)
        bucket_name = parsed.netloc
        s3_prefix = parsed.path.lstrip('/')
        
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        )
        
        print(f"   Checking for existing data at: {s3_path}")
        
        # List all objects with this prefix
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket_name, Prefix=s3_prefix)
        
        objects_to_delete = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects_to_delete.append({'Key': obj['Key']})
        
        # Delete objects if found
        if objects_to_delete:
            print(f"  Deleting {len(objects_to_delete)} existing files...")
            
            # Delete in batches of 1000 (S3 limit)
            for i in range(0, len(objects_to_delete), 1000):
                batch = objects_to_delete[i:i+1000]
                s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': batch}
                )
            
            print(f" Cleaned up {len(objects_to_delete)} old files from /cleaned/ path")
        else:
            print(f"  No existing data found (first run)")
            
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            print(f"  Bucket doesn't exist: {bucket_name}")
        else:
            print(f"  S3 cleanup warning: {e}")
    except Exception as e:
        print(f"  S3 cleanup warning: {e}")
        print(f"   Continuing anyway...")