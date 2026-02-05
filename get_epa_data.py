import requests
import pandas as pd
from datetime import datetime
import time
import xml.etree.ElementTree as ET
import os
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Your 15 manufacturers
MANUFACTURERS = [
    "Tesla", "Ford", "Toyota", "GMC",
    "Honda", "Chevrolet", "Nissan", "BMW", 
    "Mercedes-Benz", "Volkswagen", "Hyundai", 
    "Kia", "Subaru", "Mazda", "Jeep"
]

BASE_URL = "https://www.fueleconomy.gov/ws/rest/vehicle"

# Years from 2020 to current
YEARS = [2020, 2021, 2022, 2023, 2024]

def upload_to_s3(local_file, s3_key):
    """Upload file to S3"""
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    
    if not s3_bucket:
        print(f"    ⚠️  No S3_BUCKET_NAME in .env, skipping upload")
        return False
    
    try:
        s3_client = boto3.client('s3')
        print(f"    ☁️  Uploading to S3: s3://{s3_bucket}/{s3_key}")
        s3_client.upload_file(local_file, s3_bucket, s3_key)
        print(f"    ✓ Uploaded successfully")
        return True
    except Exception as e:
        print(f"    ❌ S3 upload failed: {e}")
        return False

def parse_xml_menu_items(xml_text):
    """
    Parse XML response and extract menu items
    Returns list of item values
    """
    try:
        root = ET.fromstring(xml_text)
        items = []
        
        # Find all menuItem elements
        for item in root.findall('.//menuItem'):
            value = item.find('value')
            if value is not None and value.text:
                items.append(value.text)
        
        return items
    except Exception as e:
        return []

def get_all_models_for_make_year(make, year):
    """
    Get all model names for a given make and year
    """
    url = f"{BASE_URL}/menu/model"
    params = {'year': year, 'make': make}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        # Parse XML response
        models = parse_xml_menu_items(response.text)
        return models
        
    except Exception as e:
        print(f"    ⚠ Error getting models for {make} {year}: {e}")
        return []

def get_vehicle_ids_for_make_model_year(make, model, year):
    """
    Get all vehicle variant IDs for a specific make/model/year
    """
    url = f"{BASE_URL}/menu/options"
    params = {'year': year, 'make': make, 'model': model}
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        
        # Parse XML response
        vehicle_ids = parse_xml_menu_items(response.text)
        return vehicle_ids
        
    except Exception as e:
        return []

def parse_vehicle_xml(xml_text):
    """
    Parse vehicle details XML and extract all fields
    """
    try:
        root = ET.fromstring(xml_text)
        
        # Helper function to safely get text from XML element
        def get_text(tag_name, default=None):
            element = root.find(tag_name)
            if element is not None and element.text:
                try:
                    # Try to convert to number if possible
                    return float(element.text) if '.' in element.text else int(element.text)
                except ValueError:
                    return element.text
            return default
        
        vehicle_data = {
            # Identification
            'epa_id': get_text('id'),
            'year': get_text('year'),
            'make': get_text('make'),
            'model': get_text('model'),
            'vehicle_class': get_text('VClass', 'Unknown'),
            
            # Fuel/Energy Type
            'fuel_type': get_text('fuelType', 'Unknown'),
            'fuel_type1': get_text('fuelType1'),
            'fuel_type2': get_text('fuelType2'),
            
            # Electric Vehicle Specs
            'electric_range': get_text('range', 0),
            'mpge_city': get_text('city08U', 0),
            'mpge_highway': get_text('highway08U', 0),
            'mpge_combined': get_text('comb08U', 0),
            'charge_time_240v_hours': get_text('charge240'),
            
            # Gas Vehicle MPG
            'mpg_city': get_text('city08', 0),
            'mpg_highway': get_text('highway08', 0),
            'mpg_combined': get_text('comb08', 0),
            
            # Alternative Fuel MPG (for hybrids)
            'mpg_city_a': get_text('cityA08', 0),
            'mpg_highway_a': get_text('highwayA08', 0),
            'mpg_combined_a': get_text('combA08', 0),
            
            # Cost Analysis
            'annual_fuel_cost': get_text('fuelCost08', 0),
            'annual_fuel_cost_a': get_text('fuelCostA08', 0),
            'five_year_savings': get_text('youSaveSpend', 0),
            
            # Emissions
            'co2_emissions_gpm': get_text('co2', 0),
            'co2_tailpipe_gpm': get_text('co2TailpipeGpm', 0),
            'greenhouse_gas_score': get_text('ghgScore', 0),
            
            # Engine/Drivetrain
            'cylinders': get_text('cylinders', 0),
            'displacement_liters': get_text('displ', 0),
            'engine_descriptor': get_text('engId', ''),
            'transmission': get_text('trany', 'Unknown'),
            'drive_type': get_text('drive', 'Unknown'),
            
            # EPA Ratings
            'epa_fuel_economy_score': get_text('feScore', 0),
            
            # Metadata
            'data_source': 'EPA Fuel Economy API',
            'fetched_at': datetime.now().isoformat()
        }
        
        return vehicle_data
        
    except Exception as e:
        return None

def get_vehicle_details(vehicle_id):
    """
    Get detailed specs for a specific vehicle ID
    """
    url = f"{BASE_URL}/{vehicle_id}"
    
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        # Parse XML response
        return parse_vehicle_xml(response.text)
        
    except Exception as e:
        return None

def fetch_epa_data_for_manufacturers(manufacturers, years):
    """
    Fetch EPA fuel economy data for all specified manufacturers
    """
    all_data = []
    total_vehicles = 0
    
    print(f"\n{'='*60}")
    print(f"Fetching EPA Data for {len(manufacturers)} Manufacturers")
    print(f"Date Range: January 1, 2020 - {datetime.now().strftime('%B %d, %Y')}")
    print(f"Years: {years}")
    print(f"Format: XML (parsed automatically)")
    print(f"{'='*60}\n")
    
    for idx, make in enumerate(manufacturers, 1):
        print(f"📊 [{idx}/{len(manufacturers)}] Processing: {make}")
        make_vehicle_count = 0
        
        for year in years:
            print(f"  └─ Year {year}...", end=" ", flush=True)
            
            # Get all models for this make/year
            models = get_all_models_for_make_year(make, year)
            
            if not models:
                print(f"No models found")
                continue
            
            print(f"Found {len(models)} model(s)", end=" ", flush=True)
            
            year_vehicle_count = 0
            
            # For each model, get all variants
            for model in models:
                vehicle_ids = get_vehicle_ids_for_make_model_year(make, model, year)
                
                if not vehicle_ids:
                    continue
                
                # Get details for each variant
                for vid in vehicle_ids:
                    details = get_vehicle_details(vid)
                    
                    if details:
                        all_data.append(details)
                        make_vehicle_count += 1
                        year_vehicle_count += 1
                        total_vehicles += 1
                    
                    # Be respectful to the API
                    time.sleep(0.2)
            
            print(f"→ {year_vehicle_count} variants")
        
        print(f"  ✅ {make} COMPLETE: {make_vehicle_count} total vehicles\n")
    
    print(f"{'='*60}")
    print(f"✅ COLLECTION COMPLETE: {total_vehicles} total vehicles")
    print(f"{'='*60}\n")
    
    return pd.DataFrame(all_data)

def main():
    """
    Main execution function
    """
    print("\n" + "="*60)
    print("EPA FUEL ECONOMY DATA COLLECTION WITH S3 UPLOAD")
    print("AutoInsight Project - 15 Manufacturers")
    print("="*60)
    print(f"📅 Date Range: January 1, 2020 - {datetime.now().strftime('%B %d, %Y')}")
    print(f"🔓 Authentication: NONE REQUIRED (Public API)")
    print(f"📄 Format: XML (automatically parsed)")
    print("="*60)
    
    start_time = time.time()
    
    # Fetch the data
    df = fetch_epa_data_for_manufacturers(MANUFACTURERS, YEARS)
    
    if len(df) == 0:
        print("❌ No data collected!")
        return
    
    # Create output directory
    os.makedirs('data', exist_ok=True)
    
    # Save to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = f"data/epa_fuel_economy_2020_to_present_{timestamp}.csv"
    df.to_csv(csv_filename, index=False)
    print(f"💾 Saved locally: {csv_filename}")
    
    # Upload CSV to S3
    s3_csv_key = f"epa/raw/epa_fuel_economy_2020_to_present_{timestamp}.csv"
    upload_to_s3(csv_filename, s3_csv_key)
    
    # Save to JSON
    json_filename = f"data/epa_fuel_economy_2020_to_present_{timestamp}.json"
    df.to_json(json_filename, orient='records', indent=2)
    print(f"💾 Saved locally: {json_filename}")
    
    # Upload JSON to S3
    s3_json_key = f"epa/raw/epa_fuel_economy_2020_to_present_{timestamp}.json"
    upload_to_s3(json_filename, s3_json_key)
    
    elapsed_time = time.time() - start_time
    
    # Print summary
    print(f"\n{'='*60}")
    print("📊 DATA SUMMARY")
    print(f"{'='*60}")
    print(f"Total Records: {len(df):,}")
    print(f"Manufacturers: {df['make'].nunique()}")
    print(f"Unique Models: {df['model'].nunique()}")
    print(f"Years Covered: {sorted(df['year'].unique())}")
    print(f"Collection Time: {elapsed_time/60:.1f} minutes")
    
    print(f"\n{'='*60}")
    print("📈 RECORDS BY MANUFACTURER")
    print(f"{'='*60}")
    manufacturer_counts = df['make'].value_counts().sort_values(ascending=False)
    for make, count in manufacturer_counts.items():
        percentage = (count / len(df)) * 100
        print(f"  {make:<20} {count:>5} vehicles ({percentage:>5.1f}%)")
    
    print(f"\n{'='*60}")
    print("⚡ ELECTRIC VEHICLES")
    print(f"{'='*60}")
    ev_df = df[df['fuel_type'].str.contains('Electric', case=False, na=False)]
    print(f"Total EVs: {len(ev_df):,} ({len(ev_df)/len(df)*100:.1f}% of dataset)")
    if len(ev_df) > 0:
        print(f"\n🏆 Top 5 EVs by Range:")
        top_evs = ev_df.nlargest(5, 'electric_range')[['year', 'make', 'model', 'electric_range']]
        print(top_evs.to_string(index=False))
    
    print(f"\n{'='*60}")
    print("🔋 FUEL TYPE DISTRIBUTION")
    print(f"{'='*60}")
    fuel_type_counts = df['fuel_type'].value_counts()
    for fuel_type, count in fuel_type_counts.items():
        percentage = (count / len(df)) * 100
        print(f"  {fuel_type:<30} {count:>5} ({percentage:>5.1f}%)")
    
    print(f"\n{'='*60}")
    print("💰 AVERAGE ANNUAL FUEL COSTS")
    print(f"{'='*60}")
    avg_costs = df[df['annual_fuel_cost'] > 0].groupby('fuel_type')['annual_fuel_cost'].mean().sort_values()
    for fuel_type, avg_cost in avg_costs.items():
        print(f"  {fuel_type:<30} ${avg_cost:>6,.0f}/year")
    
    print(f"\n{'='*60}")
    print("✅ DATA COLLECTION COMPLETE")
    print(f"{'='*60}")
    
    # S3 Summary
    s3_bucket = os.getenv('S3_BUCKET_NAME')
    if s3_bucket:
        print(f"\n☁️  Files uploaded to S3:")
        print(f"    📁 s3://{s3_bucket}/{s3_csv_key}")
        print(f"    📁 s3://{s3_bucket}/{s3_json_key}")
    else:
        print(f"\n💡 Files saved locally in data/ folder")
        print(f"   Add S3_BUCKET_NAME to .env for automatic S3 upload")
    
    print(f"\n{'='*60}\n")
    
    # Display sample data
    print("📋 Sample Data (first 5 rows):")
    sample_cols = ['year', 'make', 'model', 'fuel_type', 'mpge_combined', 'mpg_combined', 'annual_fuel_cost']
    print(df[sample_cols].head(5).to_string(index=False))
    
    return df

if __name__ == "__main__":
    df = main()