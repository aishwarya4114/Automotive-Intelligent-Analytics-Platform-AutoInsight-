"""
Fetch latest FRED data and upload to S3
Creates ONE combined file only
"""
import requests
import pandas as pd
from datetime import datetime
import boto3
from dotenv import load_dotenv
import os

load_dotenv()

FRED_API_KEY = os.getenv('FRED_API_KEY', '6e7e57d57b89e48e146fdca043e7737b')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
S3_BUCKET = os.getenv('S3_BUCKET_NAME')

def fetch_fred_series(series_id, start_date='2020-01-01'):
    """Fetch FRED time series data"""
    
    end_date = datetime.now().strftime('%Y-%m-%d')
    
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        'series_id': series_id,
        'api_key': '6e7e57d57b89e48e146fdca043e7737b',
        'file_type': 'json',
        'observation_start': start_date,
        'observation_end': end_date
    }
    
    response = requests.get(url, params=params)
    
    print(f"Fetching {series_id}...")
    print(f"  Status: {response.status_code}")
    
    data = response.json()
    
    if 'error_message' in data:
        print(f"  ERROR: {data['error_message']}")
        return None
    
    if 'observations' not in data:
        print(f"  ERROR: No observations")
        return None
    
    # Convert to dataframe
    df = pd.DataFrame(data['observations'])
    df = df[['date', 'value']]
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df = df.dropna()
    df['series_id'] = series_id
    
    print(f"  Got {len(df)} records ({df['date'].min()} to {df['date'].max()})")
    
    return df

def upload_to_s3(local_file, s3_key):
    """Upload file to S3"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )
        
        print(f"\nUploading to S3...")
        print(f"  File: {local_file}")
        print(f"  Destination: s3://{S3_BUCKET}/{s3_key}")
        
        s3_client.upload_file(local_file, S3_BUCKET, s3_key)
        
        print(f"  Success!")
        
        return True
        
    except Exception as e:
        print(f"  ERROR: {e}")
        return False

# Main execution
if __name__ == "__main__":
    print("="*60)
    print("FRED DATA FETCH AND S3 UPLOAD")
    print("="*60)
    
    # Fetch all 3 series
    totalsa = fetch_fred_series('TOTALSA')
    altsales = fetch_fred_series('ALTSALES')
    dautosaar = fetch_fred_series('DAUTOSAAR')
    
    if all([totalsa is not None, altsales is not None, dautosaar is not None]):
        
        # Combine
        combined = pd.concat([totalsa, altsales, dautosaar], ignore_index=True)
        
        # Add series names
        series_names = {
            'TOTALSA': 'Total Vehicle Sales',
            'ALTSALES': 'Light Truck Sales',
            'DAUTOSAAR': 'Domestic Auto Sales'
        }
        combined['series_name'] = combined['series_id'].map(series_names)
        
        # Reorder columns
        combined = combined[['date', 'value', 'series_id', 'series_name']]
        
        # Sort
        combined = combined.sort_values(['date', 'series_id'])
        
        # Save locally
        local_file = 'fred_economic_indicators.csv'
        combined.to_csv(local_file, index=False)
        
        print("\n" + "="*60)
        print("LOCAL FILE CREATED")
        print("="*60)
        print(f"  Filename: {local_file}")
        print(f"  Records: {len(combined):,}")
        print(f"  Date range: {combined['date'].min()} to {combined['date'].max()}")
        print(f"  Series: {len(combined['series_id'].unique())} (TOTALSA, ALTSALES, DAUTOSAAR)")
        
        # Show sample
        print("\n  Sample data:")
        print(combined.head(10).to_string(index=False))
        
        # Upload to S3
        s3_key = 'fred/raw/fred_economic_indicators.csv'
        
        if upload_to_s3(local_file, s3_key):
            print("\n" + "="*60)
            print("COMPLETE!")
            print("="*60)
            print(f"  FRED data successfully uploaded to:")
            print(f"  s3://{S3_BUCKET}/{s3_key}")
        else:
            print("\nUpload failed - file saved locally as: {}".format(local_file))
        
    else:
        print("\nFailed to fetch FRED data")
        print("Check your API key in .env file")