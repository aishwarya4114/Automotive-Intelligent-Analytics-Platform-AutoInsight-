import snowflake.connector
import json
import pandas as pd
from pathlib import Path
import sys

# Add src directory to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))

from utils.config import Config

class SchemaExtractor:
    """Extract schema from AUTOMOTIVE_AI database for RAG"""
    
    def __init__(self):
        print(" Connecting to AUTOMOTIVE_AI database...")
        self.conn = snowflake.connector.connect(**Config.SNOWFLAKE_CONFIG)
        print(" Connected!\n")
    
    def list_all_tables(self):
        """Get all tables across NHTSA, EPA, FRED schemas"""
        all_tables = []
        
        for schema in Config.SCHEMAS:
            query = f"""
            SELECT 
                '{schema}' as schema_name,
                table_name, 
                table_type, 
                row_count,
                created as created_date
            FROM AUTOMOTIVE_AI.INFORMATION_SCHEMA.TABLES
            WHERE table_schema = '{schema}'
            ORDER BY table_name
            """
            df = pd.read_sql(query, self.conn)
            all_tables.append(df)
        
        return pd.concat(all_tables, ignore_index=True)
    
    def extract_full_schema(self) -> dict:
        """Extract ALL tables from NHTSA, EPA, FRED schemas"""
        
        schema_data = {}
        
        for schema in Config.SCHEMAS:
            print(f" Extracting {schema} schema...")
            
            query = f"""
            SELECT 
                t.table_name,
                t.table_type,
                t.row_count,
                c.column_name,
                c.ordinal_position,
                c.data_type,
                c.is_nullable,
                c.column_default,
                c.comment as column_comment
            FROM AUTOMOTIVE_AI.INFORMATION_SCHEMA.TABLES t
            JOIN AUTOMOTIVE_AI.INFORMATION_SCHEMA.COLUMNS c 
                ON t.table_name = c.table_name 
                AND t.table_schema = c.table_schema
            WHERE t.table_schema = '{schema}'
            ORDER BY t.table_name, c.ordinal_position
            """
            
            df = pd.read_sql(query, self.conn)
            
            # Process each table
            for table in df['TABLE_NAME'].unique():
                table_data = df[df['TABLE_NAME'] == table]
                full_table_name = f"{schema}.{table}"
                
                schema_data[full_table_name] = {
                    "schema": schema,
                    "table_name": table,
                    "full_name": full_table_name,
                    "table_type": table_data['TABLE_TYPE'].iloc[0],
                    "row_count": int(table_data['ROW_COUNT'].iloc[0]) if pd.notna(table_data['ROW_COUNT'].iloc[0]) else 0,
                    "columns": []
                }
                
                for _, row in table_data.iterrows():
                    schema_data[full_table_name]["columns"].append({
                        "name": row['COLUMN_NAME'],
                        "position": int(row['ORDINAL_POSITION']),
                        "type": row['DATA_TYPE'],
                        "nullable": row['IS_NULLABLE'] == 'YES',
                        "default": row['COLUMN_DEFAULT'],
                        "comment": row['COLUMN_COMMENT']
                    })
                
                print(f"    {full_table_name}: {len(schema_data[full_table_name]['columns'])} columns, {schema_data[full_table_name]['row_count']:,} rows")
        
        return schema_data
    
    def generate_schema_documentation(self, schema: dict) -> list:
        """Generate detailed documentation for RAG"""
        
        docs = []
        
        for full_table_name, table_info in schema.items():
            # Create comprehensive documentation
            doc_text = f"""
Schema: {table_info['schema']}
Table: {table_info['table_name']}
Full Name: AUTOMOTIVE_AI.{full_table_name}
Type: {table_info['table_type']}
Row Count: {table_info['row_count']:,}

Columns ({len(table_info['columns'])}):
"""
            # List all columns
            for col in table_info['columns']:
                doc_text += f"  {col['position']:2d}. {col['name']:40s} {col['type']:15s}"
                if not col['nullable']:
                    doc_text += " [NOT NULL]"
                if col['name'].endswith('_id') or col['name'].endswith('_ID'):
                    doc_text += " [KEY]"
                doc_text += "\n"
            
            # Add context
            context = self._get_table_context(table_info['table_name'])
            doc_text += f"\n{context}\n"
            
            # Add sample queries
            sample_queries = self._get_sample_queries(full_table_name)
            doc_text += f"\nSample Queries:\n{sample_queries}\n"
            
            docs.append({
                "table_name": full_table_name,
                "content": doc_text,
                "metadata": {
                    "source": "snowflake_schema",
                    "database": "AUTOMOTIVE_AI",
                    "schema": table_info['schema'],
                    "table": table_info['table_name'],
                    "full_name": full_table_name,
                    "column_count": len(table_info['columns']),
                    "row_count": table_info['row_count']
                }
            })
        
        return docs
    
    def _get_table_context(self, table_name: str) -> str:
        """Add context for each table"""
        
        contexts = {
            'FACT_INVESTIGATIONS': """
Context: NHTSA safety defect investigations (2015-2025). Contains formal investigations into potential vehicle defects.
Use for: Investigation trends, component issues, manufacturer analysis, investigation-to-recall conversion.
Key columns: investigation_id, make, model, year, component_name, investigation_status, has_recall
Date range: 2015-2025 (10 years of data)
""",
            'FACT_RATINGS': """
Context: NHTSA 5-star crash test safety ratings (2015-2025). Contains comprehensive vehicle safety ratings.
Use for: Safety comparisons, crash test analysis, safety feature adoption, manufacturer safety rankings.
Key columns: rating_id, make, model, year, overall_stars, rollover_stars, has_advanced_safety, advanced_safety_count
Date range: 2015-2025 (10 years of data)
""",
            'FACT_COMPLAINTS': """
Context: Consumer complaints from NHTSA database (2015-2025). Contains customer-reported vehicle issues WITH SENTIMENT ANALYSIS.
Use for: Complaint trends, component failure analysis, sentiment analysis, severity patterns, injury/death analysis.

 SENTIMENT ANALYSIS COLUMNS (Snowflake Cortex enriched):
- sentiment_score: Float (-1.0 to 1.0) - negative is bad, positive is good
- sentiment_category: String (VERY_NEGATIVE, NEGATIVE, NEUTRAL, POSITIVE)
- sentiment_confidence: Float (0.0 to 1.0) - how confident the sentiment prediction is
- safety_keyword_count: Integer - count of safety-related keywords (brake, airbag, crash, etc.)
- negation_count: Integer - count of negation words (not, never, didn't)

Key columns: complaint_id, make, model, year, component_description, crash_flag, fire_flag, injured, deaths, severity_level
Sentiment columns: sentiment_score, sentiment_category, safety_keyword_count, negation_count
Date range: 2015-2025 (10 years with 210K+ complaints)

SAMPLE SENTIMENT QUERIES:
- Find very negative complaints: WHERE sentiment_category = 'VERY_NEGATIVE'
- Find safety-critical complaints: WHERE safety_keyword_count >= 3 AND sentiment_score < -0.5
- Manufacturer sentiment: AVG(sentiment_score) by make
""",
            'FACT_RECALLS': """
Context: NHTSA vehicle recall campaigns (2015-2025). Contains official manufacturer recalls.
Use for: Recall frequency, manufacturer recall patterns, model-specific recalls, recall trends.
Key columns: recall_id, make, model, year, recall_summary, document_type
Date range: 2015-2025 (10 years of data)
""",
            'FACT_FUEL_ECONOMY': """
Context: EPA fuel economy ratings and emissions (2015-2025). Contains MPG and emissions data.
Use for: Fuel efficiency comparisons, emissions analysis, electric vs gas, manufacturer efficiency rankings.
Key columns: fuel_economy_id, make, model, year, mpg_city, mpg_highway, mpg_combined, fuel_type, is_electric, is_hybrid
Date range: 2015-2025 (10 years of data)
""",
            'FACT_ECONOMIC_INDICATORS': """
Context: Monthly US vehicle sales from Federal Reserve (2015-2025). Contains macroeconomic automotive indicators.
Use for: Sales trends, economic correlation, seasonal patterns, market analysis.
Key columns: date, total_vehicle_sales, light_truck_sales, domestic_auto_sales
Date range: 2015-2025 (131 monthly observations)
"""
        }
        
        return contexts.get(table_name, f"AutoInsight data table (2015-2025)")
    
    def _get_sample_queries(self, full_table_name: str) -> str:
        """Sample SQL queries for each table"""
        
        samples = {
            'NHTSA.FACT_INVESTIGATIONS': """
- SELECT make, COUNT(*) FROM AUTOMOTIVE_AI.NHTSA.FACT_INVESTIGATIONS WHERE investigation_status = 'Open' GROUP BY make ORDER BY COUNT(*) DESC;
- SELECT component_name, COUNT(*) FROM AUTOMOTIVE_AI.NHTSA.FACT_INVESTIGATIONS WHERE has_recall = TRUE GROUP BY component_name ORDER BY COUNT(*) DESC;
- SELECT YEAR(open_date) as year, COUNT(*) FROM AUTOMOTIVE_AI.NHTSA.FACT_INVESTIGATIONS GROUP BY year ORDER BY year;
""",
            'NHTSA.FACT_RATINGS': """
- SELECT make, AVG(overall_stars) FROM AUTOMOTIVE_AI.NHTSA.FACT_RATINGS GROUP BY make ORDER BY AVG(overall_stars) DESC;
- SELECT * FROM AUTOMOTIVE_AI.NHTSA.FACT_RATINGS WHERE overall_stars >= 5.0 AND has_advanced_safety = TRUE;
- SELECT year, AVG(overall_stars) FROM AUTOMOTIVE_AI.NHTSA.FACT_RATINGS GROUP BY year ORDER BY year;
""",
            'NHTSA.FACT_COMPLAINTS': """
-- Basic queries
- SELECT make, COUNT(*) FROM AUTOMOTIVE_AI.NHTSA.FACT_COMPLAINTS WHERE crash_flag = TRUE GROUP BY make ORDER BY COUNT(*) DESC;
- SELECT severity_level, COUNT(*) FROM AUTOMOTIVE_AI.NHTSA.FACT_COMPLAINTS GROUP BY severity_level;

--  SENTIMENT ANALYSIS QUERIES
- SELECT make, ROUND(AVG(sentiment_score), 3) as avg_sentiment FROM AUTOMOTIVE_AI.NHTSA.FACT_COMPLAINTS GROUP BY make ORDER BY avg_sentiment;
- SELECT * FROM AUTOMOTIVE_AI.NHTSA.FACT_COMPLAINTS WHERE sentiment_category = 'VERY_NEGATIVE' AND safety_keyword_count >= 3 LIMIT 10;
- SELECT sentiment_category, COUNT(*) FROM AUTOMOTIVE_AI.NHTSA.FACT_COMPLAINTS GROUP BY sentiment_category;
- SELECT make, COUNT(*) as very_neg_count FROM AUTOMOTIVE_AI.NHTSA.FACT_COMPLAINTS WHERE sentiment_category = 'VERY_NEGATIVE' GROUP BY make ORDER BY very_neg_count DESC;
""",
            'NHTSA.FACT_RECALLS': """
- SELECT make, COUNT(*) FROM AUTOMOTIVE_AI.NHTSA.FACT_RECALLS GROUP BY make ORDER BY COUNT(*) DESC LIMIT 10;
- SELECT * FROM AUTOMOTIVE_AI.NHTSA.FACT_RECALLS WHERE make = 'Tesla' ORDER BY year DESC;
- SELECT year, COUNT(*) FROM AUTOMOTIVE_AI.NHTSA.FACT_RECALLS GROUP BY year ORDER BY year;
""",
            'EPA.FACT_FUEL_ECONOMY': """
- SELECT make, AVG(mpg_combined) FROM AUTOMOTIVE_AI.EPA.FACT_FUEL_ECONOMY GROUP BY make ORDER BY AVG(mpg_combined) DESC LIMIT 10;
- SELECT * FROM AUTOMOTIVE_AI.EPA.FACT_FUEL_ECONOMY WHERE is_electric = TRUE ORDER BY electric_range DESC;
- SELECT year, AVG(mpg_combined) FROM AUTOMOTIVE_AI.EPA.FACT_FUEL_ECONOMY WHERE is_electric = FALSE GROUP BY year ORDER BY year;
""",
            'FRED.FACT_ECONOMIC_INDICATORS': """
- SELECT * FROM AUTOMOTIVE_AI.FRED.FACT_ECONOMIC_INDICATORS ORDER BY date DESC LIMIT 12;
- SELECT YEAR(date), AVG(total_vehicle_sales) FROM AUTOMOTIVE_AI.FRED.FACT_ECONOMIC_INDICATORS GROUP BY YEAR(date);
- SELECT date, total_vehicle_sales FROM AUTOMOTIVE_AI.FRED.FACT_ECONOMIC_INDICATORS WHERE YEAR(date) >= 2020 ORDER BY date;
"""
        }
        
        return samples.get(full_table_name, "")
    
    def save_schema(self, schema: dict, filename: str = "autoinsight_schema.json"):
        """Save schema to rag/schemas/"""
        filepath = Config.SCHEMA_DIR / filename
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(schema, f, indent=2)
        print(f"\n Schema saved to {filepath}")
        return filepath


if __name__ == "__main__":
    print("="*80)
    print(" AutoInsight Schema Extractor for AUTOMOTIVE_AI (2015-2025)")
    print("="*80)
    
    extractor = SchemaExtractor()
    
    # Step 1: List tables
    print("\n YOUR SNOWFLAKE TABLES:\n")
    tables = extractor.list_all_tables()
    print(tables.to_string(index=False))
    print(f"\n Total Tables: {len(tables)}")
    print(f" Total Rows: {tables['ROW_COUNT'].sum():,.0f}")
    
    # Step 2: Extract schema
    print("\n" + "="*80)
    print(" EXTRACTING DETAILED SCHEMA...")
    print("="*80 + "\n")
    schema = extractor.extract_full_schema()
    
    # Step 3: Save schema
    schema_file = extractor.save_schema(schema)
    
    # Step 4: Generate docs
    print("\n" + "="*80)
    print(" GENERATING SCHEMA DOCUMENTATION FOR RAG...")
    print("="*80)
    docs = extractor.generate_schema_documentation(schema)
    print(f" Generated {len(docs)} schema documents")
    
    # Step 5: Show sample
    print("\n" + "="*80)
    print(" SAMPLE SCHEMA DOCUMENT:")
    print("="*80)
    if docs:
        # Show FACT_COMPLAINTS doc (with sentiment!)
        complaints_doc = [d for d in docs if 'COMPLAINTS' in d['table_name']]
        if complaints_doc:
            print(complaints_doc[0]['content'][:1500])
            print("...\n")
    
    print("="*80)
    print(" Schema extraction complete!")
    print("="*80)
    print(f"\n Files created:")
    print(f"   - {schema_file}")
    print(f"\n Key Updates:")
    print(f"   - Date range: 2015-2025 (10 years)")
    print(f"   - Sentiment analysis columns documented")
    print(f"   - {len(docs)} tables ready for RAG")