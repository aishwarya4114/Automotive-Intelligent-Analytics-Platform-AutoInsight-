#!/usr/bin/env python3
"""
NHTSA Bulk Data Downloader - With S3 Upload
"""

import requests
import zipfile
import io
import os
from datetime import datetime
import pandas as pd
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class NHTSABulkDownloader:
    """Download and process bulk NHTSA data files"""
    
    def __init__(self, output_dir="nhtsa_data/nhtsa_bulk"):
        self.output_dir = output_dir
        self._create_directories()
        
        # Initialize S3 client
        self.s3_bucket = os.getenv('S3_BUCKET_NAME')
        self.s3_client = None
        
        if self.s3_bucket:
            try:
                self.s3_client = boto3.client('s3')
                print(f"✓ Connected to S3 bucket: {self.s3_bucket}")
            except Exception as e:
                print(f"⚠️  Could not connect to S3: {e}")
                print("   Will save locally only")
        else:
            print("⚠️  No S3_BUCKET_NAME in .env file")
            print("   Will save locally only")
        
        # Official NHTSA column names for complaints
        self.complaint_columns = [
            'CMPLID', 'ODINO', 'MFR_NAME', 'MAKETXT', 'MODELTXT', 'YEARTXT',
            'CRASH', 'FAILDATE', 'FIRE', 'INJURED', 'DEATHS', 'COMPDESC',
            'CITY', 'STATE', 'VIN', 'DATEA', 'LDATE', 'MILES', 'OCCURENCES',
            'CDESCR', 'CMPL_TYPE', 'POLICE_RPT_YN', 'PURCH_DT', 'ORIG_OWNER_YN',
            'ANTI_BRAKES_YN', 'CRUISE_CONT_YN', 'NUM_CYLS', 'DRIVE_TRAIN',
            'FUEL_SYS', 'FUEL_TYPE', 'TRANS_TYPE', 'VEH_SPEED', 'DOT',
            'TIRE_SIZE', 'LOC_OF_TIRE', 'TIRE_FAIL_TYPE', 'ORIG_EQUIP_YN',
            'MANUF_DT', 'SEAT_TYPE', 'RESTRAINT_TYPE', 'DEALER_NAME',
            'DEALER_TEL', 'DEALER_CITY', 'DEALER_STATE', 'DEALER_ZIP',
            'PROD_TYPE', 'REPAIRED_YN', 'MEDICAL_ATTN', 'VEHICLES_TOWED_YN'
        ]
        
        # Investigation columns
        self.investigation_columns = [
            'NHTSA_ACTION_NUMBER', 'MAKE', 'MODEL', 'YEAR', 'COMPNAME',
            'MFR_NAME', 'ODATE', 'CDATE', 'CAMPNO', 'SUBJECT', 'SUMMARY'
        ]
    
    def _create_directories(self):
        """Create output directories"""
        os.makedirs(f"{self.output_dir}/recalls", exist_ok=True)
        os.makedirs(f"{self.output_dir}/complaints", exist_ok=True)
        os.makedirs(f"{self.output_dir}/ratings", exist_ok=True)
        os.makedirs(f"{self.output_dir}/investigations", exist_ok=True)
        os.makedirs(f"{self.output_dir}/processed", exist_ok=True)
        print(f"✓ Created directories in: {self.output_dir}")
    
    def upload_to_s3(self, local_file, s3_key):
        """Upload file to S3"""
        if not self.s3_client or not self.s3_bucket:
            return False
        
        try:
            print(f"    ☁️  Uploading to S3: s3://{self.s3_bucket}/{s3_key}")
            self.s3_client.upload_file(local_file, self.s3_bucket, s3_key)
            print(f"    ✓ Uploaded successfully")
            return True
        except Exception as e:
            print(f"    ❌ S3 upload failed: {e}")
            return False
    
    def download_and_extract_zip(self, url, extract_to):
        """Download and extract a ZIP file"""
        print(f"  Downloading from: {url}")
        
        try:
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            
            print(f"  Download complete: {len(response.content) / 1024 / 1024:.1f} MB")
            print(f"  Extracting...")
            
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                z.extractall(extract_to)
                extracted_files = z.namelist()
                print(f"  ✓ Extracted {len(extracted_files)} file(s): {extracted_files}")
                return extracted_files
                
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return []
    
    def download_file_direct(self, url, save_path):
        """Download a file directly (non-ZIP)"""
        print(f"  Downloading from: {url}")
        
        try:
            response = requests.get(url, timeout=120)
            response.raise_for_status()
            
            # Create directory if needed
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            
            # Save file
            with open(save_path, 'wb') as f:
                f.write(response.content)
            
            file_size_mb = len(response.content) / 1024 / 1024
            print(f"  ✓ Downloaded: {file_size_mb:.1f} MB")
            return save_path
            
        except Exception as e:
            print(f"  ❌ Error: {e}")
            return None
    
    def download_recalls(self, year_ranges=None):
        """Download recall data files"""
        if year_ranges is None:
            year_ranges = ["2020-2024", "2025-2025"]
        
        print("\n" + "="*60)
        print("Downloading NHTSA Recall Data")
        print("="*60)
        
        recall_files = {
            "2020-2024": "https://static.nhtsa.gov/odi/ffdd/rcl/RCL_FROM_2020_2024.zip",
            "2025-2025": "https://static.nhtsa.gov/odi/ffdd/rcl/RCL_FROM_2025_2025.zip"
        }
        
        downloaded_files = []
        
        for year_range in year_ranges:
            if year_range not in recall_files:
                print(f"⚠️  Unknown year range: {year_range}")
                continue
            
            print(f"\n📥 Downloading recalls for {year_range}...")
            url = recall_files[year_range]
            extract_path = f"{self.output_dir}/recalls/{year_range}"
            
            files = self.download_and_extract_zip(url, extract_path)
            downloaded_files.extend([(year_range, f) for f in files])
        
        return downloaded_files
    
    def download_complaints(self):
        """Download all complaints data"""
        print("\n" + "="*60)
        print("Downloading NHTSA Complaints Data")
        print("="*60)
        
        url = "https://static.nhtsa.gov/odi/ffdd/cmpl/COMPLAINTS_RECEIVED_2020-2024.zip"
        
        print(f"\n📥 Downloading complaints received 2020-2024...")
        print("  This file contains complaints RECEIVED between 2020-2024")
        
        extract_path = f"{self.output_dir}/complaints/all_years"
        files = self.download_and_extract_zip(url, extract_path)
        
        return files
    
    def download_ratings(self):
        """Download safety ratings data - FIXED URL"""
        print("\n" + "="*60)
        print("Downloading NHTSA Safety Ratings Data")
        print("="*60)
        
        url = "https://static.nhtsa.gov/nhtsa/downloads/Safercar/Safercar_data.csv"
        
        print(f"\n📥 Downloading safety ratings data...")
        print("  Note: This is a direct CSV file, not a ZIP")
        
        save_path = f"{self.output_dir}/ratings/Safercar_data.csv"
        file = self.download_file_direct(url, save_path)
        
        return [file] if file else []
    
    def download_investigations(self):
        """Download investigations data - CORRECTED URL (ZIP file)"""
        print("\n" + "="*60)
        print("Downloading NHTSA Investigations Data")
        print("="*60)
    
        url = "https://static.nhtsa.gov/odi/ffdd/inv/FLAT_INV.zip"
    
        print(f"\n📥 Downloading investigations data...")
        print("  This contains the full investigations database")
    
        extract_path = f"{self.output_dir}/investigations"
        files = self.download_and_extract_zip(url, extract_path)
    
        return files
    
    def load_recall_data(self, year_range="2020-2024"):
        """Load recall data (CSV format)"""
        print(f"\n📊 Loading recall data for {year_range}...")
        
        recall_dir = f"{self.output_dir}/recalls/{year_range}"
        
        if not os.path.exists(recall_dir):
            print(f"❌ Directory not found: {recall_dir}")
            return None
        
        files = os.listdir(recall_dir)
        data_files = [f for f in files if f.endswith('.csv') or f.endswith('.txt')]
        
        if not data_files:
            print(f"  ❌ No data files found in {recall_dir}")
            return None
        
        data_file = os.path.join(recall_dir, data_files[0])
        print(f"  Loading: {data_files[0]}")
        
        try:
            df = pd.read_csv(
                data_file,
                encoding='latin-1',
                low_memory=False
            )
            
            print(f"  ✓ Loaded {len(df):,} recall records")
            print(f"  Columns ({len(df.columns)}): {list(df.columns[:10])}...")
            return df
            
        except Exception as e:
            print(f"  ❌ Error loading data: {e}")
            return None
    
    def load_complaint_data(self, sample_size=None):
        """Load complaint data from 2020-2024 received file"""
        print(f"\n📊 Loading complaint data...")
        
        complaint_dir = f"{self.output_dir}/complaints/all_years"
        
        # Find the TXT file (name might vary)
        if not os.path.exists(complaint_dir):
            print(f"❌ Directory not found: {complaint_dir}")
            return None
        
        files = os.listdir(complaint_dir)
        data_files = [f for f in files if f.endswith('.txt')]
        
        if not data_files:
            print(f"  ❌ No TXT files found in {complaint_dir}")
            return None
        
        data_file = os.path.join(complaint_dir, data_files[0])
        file_size_mb = os.path.getsize(data_file) / 1024 / 1024
        print(f"  File: {data_files[0]} ({file_size_mb:.1f} MB)")
        
        if sample_size:
            print(f"  Loading first {sample_size:,} rows (sample mode)...")
        else:
            print(f"  Loading ALL data...")
        
        try:
            df = pd.read_csv(
                data_file,
                sep='\t',
                encoding='latin-1',
                header=None,
                names=self.complaint_columns,
                nrows=sample_size,
                on_bad_lines='skip',
                low_memory=False
            )
            
            print(f"  ✓ Loaded {len(df):,} complaint records")
            
            # Show year distribution
            if 'YEARTXT' in df.columns:
                df['YEARTXT_clean'] = pd.to_numeric(df['YEARTXT'], errors='coerce')
                year_dist = df['YEARTXT_clean'].value_counts().sort_index()
                print(f"\n  Vehicle Year distribution:")
                print(f"    Min year: {year_dist.index.min()}")
                print(f"    Max year: {year_dist.index.max()}")
                in_range = len(df[(df['YEARTXT_clean'] >= 2015) & (df['YEARTXT_clean'] <= 2025)])
                print(f"    Records in 2015-2025: {in_range:,}")
            
            return df
            
        except Exception as e:
            print(f"  ❌ Error loading data: {e}")
            return None
    
    def load_ratings_data(self):
        """Load safety ratings data - UPDATED for direct CSV"""
        print(f"\n📊 Loading safety ratings data...")
        
        data_file = f"{self.output_dir}/ratings/Safercar_data.csv"
        
        if not os.path.exists(data_file):
            print(f"❌ File not found: {data_file}")
            return None
        
        print(f"  Loading: Safercar_data.csv")
        
        try:
            df = pd.read_csv(
                data_file,
                encoding='latin-1',
                low_memory=False
            )
            
            print(f"  ✓ Loaded {len(df):,} ratings records")
            print(f"  Columns ({len(df.columns)}): {list(df.columns[:10])}...")
            
            return df
            
        except Exception as e:
            print(f"  ❌ Error loading data: {e}")
            return None
    
    def load_investigations_data(self):
        """Load investigations data from FLAT_INV.txt"""
        print(f"\n📊 Loading investigations data...")
    
        inv_dir = f"{self.output_dir}/investigations"
    
        if not os.path.exists(inv_dir):
            print(f"❌ Directory not found: {inv_dir}")
            return None
    
        # Look for FLAT_INV.txt (the actual data file)
        files = os.listdir(inv_dir)
        print(f"  Files in directory: {files}")
    
        # Find the data file (should be FLAT_INV.txt)
        data_files = [f for f in files if f.startswith('FLAT_') and f.endswith('.txt')]
    
        if not data_files:
            print(f"  ❌ No FLAT_INV.txt file found")
            return None
    
        data_file = os.path.join(inv_dir, data_files[0])
        file_size_mb = os.path.getsize(data_file) / 1024 / 1024
        print(f"  Loading: {data_files[0]} ({file_size_mb:.1f} MB)")
    
        try:
            df = pd.read_csv(
                data_file,
                sep='\t',
                encoding='latin-1',
                header=None,
                names=self.investigation_columns,
                low_memory=False,
                on_bad_lines='skip'
            )
        
            print(f"  ✓ Loaded {len(df):,} investigation records")
        
            # Show sample of MAKE values
            if 'MAKE' in df.columns and len(df) > 0:
                unique_makes = df['MAKE'].value_counts().head(20)
                print(f"\n  Top 20 manufacturers in investigations:")
                for make, count in unique_makes.items():
                    print(f"    {make:30s}: {count} records")
        
            # Convert and show date range
            if 'ODATE' in df.columns:
                df['ODATE_clean'] = pd.to_datetime(df['ODATE'], format='%Y%m%d', errors='coerce')
                if df['ODATE_clean'].notna().any():
                    print(f"\n  Investigation date range:")
                    print(f"    Earliest opened: {df['ODATE_clean'].min().year}")
                    print(f"    Latest opened: {df['ODATE_clean'].max().year}")
        
            if 'CDATE' in df.columns:
                df['CDATE_clean'] = pd.to_datetime(df['CDATE'], format='%Y%m%d', errors='coerce')
        
            return df
        
        except Exception as e:
            print(f"  ❌ Error loading data: {e}")
            return None
    
    def filter_by_manufacturers(self, df, manufacturers, make_column=None):
        """Filter data by list of manufacturers - IMPROVED with fuzzy matching"""
        print(f"\n🔍 Filtering for {len(manufacturers)} manufacturers...")
        
        if make_column is None:
            possible_columns = ['MAKETXT', 'MAKE', 'MFR_NAME', 'MANUFACTURER']
            make_column = None
            
            for col in possible_columns:
                if col in df.columns:
                    make_column = col
                    print(f"  Using column: {make_column}")
                    break
            
            if make_column is None:
                print(f"  ❌ Could not find make column")
                print(f"  Available columns: {list(df.columns)}")
                return df
        
        # Clean manufacturer column
        df[make_column] = df[make_column].fillna('').astype(str).str.strip()
        
        # Build expanded manufacturer list with common variations
        expanded_manufacturers = []
        for make in manufacturers:
            expanded_manufacturers.append(make.upper())
            # Add common variations
            if make.upper() == "GENERAL MOTORS":
                expanded_manufacturers.extend(["GMC", "GM", "GENERAL MOTORS CORPORATION"])
            elif make.upper() == "MERCEDES-BENZ":
                expanded_manufacturers.extend(["MERCEDES BENZ", "DAIMLER", "DAIMLER AG"])
            elif make.upper() == "VOLKSWAGEN":
                expanded_manufacturers.extend(["VW", "VOLKSWAGEN GROUP"])
        
        # Case-insensitive matching with expanded list
        mask = df[make_column].str.upper().isin(expanded_manufacturers)
        filtered_df = df[mask].copy()
        
        print(f"  ✓ Filtered: {len(filtered_df):,} records (from {len(df):,} total)")
        
        # Show breakdown by manufacturer
        if len(filtered_df) > 0:
            print("\n  Breakdown by manufacturer:")
            for make in manufacturers:
                count = len(filtered_df[filtered_df[make_column].str.upper().str.contains(make.upper(), na=False)])
                if count > 0:
                    print(f"    {make:20s}: {count:,} records")
        
        return filtered_df
    
    def filter_by_years(self, df, start_year, end_year, year_column=None):
        """Filter data by year range - IMPROVED"""
        print(f"\n📅 Filtering for years {start_year}-{end_year}...")
        
        if year_column is None:
            possible_columns = ['YEARTXT', 'YEAR', 'MODEL_YR', 'MODELYEAR', 'MODEL YEAR']
            year_column = None
            
            for col in possible_columns:
                if col in df.columns:
                    year_column = col
                    print(f"  Using column: {year_column}")
                    break
            
            if year_column is None:
                print(f"  ⚠️  Could not find year column")
                print(f"  Available columns: {list(df.columns[:20])}")
                return df
        
        # Convert to numeric
        df[f'{year_column}_clean'] = pd.to_numeric(df[year_column], errors='coerce')
        
        # Show year distribution BEFORE filtering
        year_counts = df[f'{year_column}_clean'].value_counts().sort_index()
        if len(year_counts) > 0:
            print(f"\n  Year distribution in data:")
            print(f"    Min year: {year_counts.index.min()}")
            print(f"    Max year: {year_counts.index.max()}")
            print(f"    Records in {start_year}-{end_year}: ", end="")
        
        # Filter by year range
        filtered_df = df[
            (df[f'{year_column}_clean'] >= start_year) & 
            (df[f'{year_column}_clean'] <= end_year)
        ].copy()
        
        print(f"{len(filtered_df):,}")
        
        if len(filtered_df) == 0:
            print(f"\n  ⚠️  WARNING: No records found in year range {start_year}-{end_year}")
        
        return filtered_df
    
    def save_to_csv(self, df, filename, s3_folder="nhtsa/raw"):
        """Save DataFrame to CSV locally and upload to S3"""
        if len(df) == 0:
            print(f"  ⚠️  Skipping save: DataFrame is empty")
            return None
        
        # Save locally
        filepath = f"{self.output_dir}/processed/{filename}"
        df.to_csv(filepath, index=False)
        
        size_mb = os.path.getsize(filepath) / 1024 / 1024
        print(f"  ✓ Saved locally: {filename}")
        print(f"    Size: {size_mb:.1f} MB, Rows: {len(df):,}")
        
        # Upload to S3
        s3_key = f"{s3_folder}/{filename}"
        self.upload_to_s3(filepath, s3_key)
        
        return filepath


def main():
    """Main execution"""
    
    TOP_15_MANUFACTURERS = [
        "Tesla", "Ford", "Toyota", "General Motors", "Honda",
        "Chevrolet", "Nissan", "BMW", "Mercedes-Benz",
        "Volkswagen", "Hyundai", "Kia", "Subaru", "Mazda", "Jeep"
    ]
    
    print("\n" + "="*60)
    print("NHTSA Complete Bulk Data Downloader with S3 Upload")
    print("="*60)
    print(f"Target Period: 2020-2025")
    print(f"Target Manufacturers: {len(TOP_15_MANUFACTURERS)}")
    print(f"Data Sources: Recalls, Complaints, Ratings, Investigations")
    print("="*60)
    
    downloader = NHTSABulkDownloader()
    
    # ========== STEP 1: Download All Data ==========
    print("\n" + "="*60)
    print("PHASE 1: DOWNLOADING ALL DATA")
    print("="*60)
    
    print("\n📦 STEP 1.1: Download Recall Data")
    recall_files = downloader.download_recalls(year_ranges=["2020-2024", "2025-2025"])
    
    print("\n📦 STEP 1.2: Download Complaint Data")
    complaint_files = downloader.download_complaints()
    
    print("\n📦 STEP 1.3: Download Safety Ratings Data")
    ratings_files = downloader.download_ratings()
    
    print("\n📦 STEP 1.4: Download Investigations Data")
    investigation_files = downloader.download_investigations()
    
    # ========== STEP 2: Process All Data ==========
    print("\n" + "="*60)
    print("PHASE 2: PROCESSING ALL DATA")
    print("="*60)
    
    # Process Recalls (2020-2024)
    print("\n📊 STEP 2.1: Process Recall Data (2020-2024)")
    recall_df_2020 = downloader.load_recall_data("2020-2024")
    
    if recall_df_2020 is not None and len(recall_df_2020) > 0:
        filtered_recalls_2020 = downloader.filter_by_manufacturers(
            recall_df_2020, 
            TOP_15_MANUFACTURERS
        )
        downloader.save_to_csv(filtered_recalls_2020, "recalls_2020_2024_top15.csv", s3_folder="nhtsa/raw/recalls")
    
    # Process Recalls (2025)
    print("\n📊 STEP 2.2: Process Recall Data (2025)")
    recall_df_2025 = downloader.load_recall_data("2025-2025")
    
    if recall_df_2025 is not None and len(recall_df_2025) > 0:
        filtered_recalls_2025 = downloader.filter_by_manufacturers(
            recall_df_2025, 
            TOP_15_MANUFACTURERS
        )
        downloader.save_to_csv(filtered_recalls_2025, "recalls_2025_top15.csv", s3_folder="nhtsa/raw/recalls")
    
    # Process Complaints
    print("\n📊 STEP 2.3: Process Complaint Data")
    complaint_df = downloader.load_complaint_data(sample_size=None)  # Load all
    
    if complaint_df is not None and len(complaint_df) > 0:
        filtered_complaints = downloader.filter_by_manufacturers(
            complaint_df,
            TOP_15_MANUFACTURERS
        )
        
        if len(filtered_complaints) > 0:
            # Filter by vehicle year (2015-2025 to include older vehicles with recent complaints)
            filtered_complaints = downloader.filter_by_years(
                filtered_complaints,
                2015,
                2025
            )
            
            downloader.save_to_csv(
                filtered_complaints,
                "complaints_2020_2024_top15.csv",
                s3_folder="nhtsa/raw/complaints"
            )
    
    # Process Safety Ratings
    print("\n📊 STEP 2.4: Process Safety Ratings Data")
    ratings_df = downloader.load_ratings_data()
    
    if ratings_df is not None and len(ratings_df) > 0:
        filtered_ratings = downloader.filter_by_manufacturers(
            ratings_df,
            TOP_15_MANUFACTURERS
        )
        
        if len(filtered_ratings) > 0:
            filtered_ratings = downloader.filter_by_years(
                filtered_ratings,
                2020,
                2025
            )
            
            downloader.save_to_csv(
                filtered_ratings,
                "ratings_2020_2025_top15.csv",
                s3_folder="nhtsa/raw/ratings"
            )
    
    # Process Investigations
    print("\n📊 STEP 2.5: Process Investigations Data")
    investigations_df = downloader.load_investigations_data()
    
    if investigations_df is not None and len(investigations_df) > 0:
        # Don't filter by year first - see what's in the data
        filtered_investigations = downloader.filter_by_manufacturers(
            investigations_df,
            TOP_15_MANUFACTURERS
        )
        
        if len(filtered_investigations) > 0:
            # Now filter by year
            filtered_investigations = downloader.filter_by_years(
                filtered_investigations,
                2020,
                2025
            )
            
            downloader.save_to_csv(
                filtered_investigations,
                "investigations_2020_2025_top15.csv",
                s3_folder="nhtsa/raw/investigations"
            )
        else:
            print("\n  💡 No investigations found for top 15 manufacturers")
            print("  This might be because:")
            print("  - Investigation file only has limited records")
            print("  - Manufacturer names don't match exactly")
            print("  - Investigations are rare events")
    
    # ========== SUMMARY ==========
    print("\n" + "="*60)
    print("PROCESSING COMPLETE!")
    print("="*60)
    print("\n✅ Downloaded and processed:")
    print("   1. Recalls (2020-2024 + 2025)")
    print("   2. Complaints (2020-2024 received)")
    print("   3. Safety Ratings (2020-2025)")
    print("   4. Investigations (2020-2025)")
    print(f"\n📁 Local output: {downloader.output_dir}/processed/")
    if downloader.s3_bucket:
        print(f"☁️  S3 bucket: s3://{downloader.s3_bucket}/nhtsa/raw/")
    print("\n💡 Data ready for Spark ETL processing!")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()