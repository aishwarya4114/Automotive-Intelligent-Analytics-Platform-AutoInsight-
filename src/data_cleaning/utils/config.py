from dotenv import load_dotenv
import os
from pathlib import Path

env_path = Path(__file__).parent.parent.parent / '.env'
load_dotenv(env_path)

class Config:
    """Configuration for AutoInsight Agents"""
    
    # Snowflake
    SNOWFLAKE_CONFIG = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": "AUTOMOTIVE_AI",
        "schema": "NHTSA",
        "insecure_mode": True,  # ← ADD THIS
        "ocsp_fail_open": True   # ← ADD THIS TOO (backup)
    }
    
    # Anthropic
    ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
    
    # Pinecone (NEW)
    PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
    PINECONE_ENVIRONMENT = os.getenv("PINECONE_ENVIRONMENT", "us-east-1-aws")
    PINECONE_INDEX_NAME = "autoinsight-schema"
    
    # Models
    CLAUDE_SONNET = "claude-sonnet-4-20250514"
    CLAUDE_HAIKU = "claude-haiku-4-5-20251001"
    
    # Schemas
    SCHEMAS = ["NHTSA", "EPA", "FRED"]
    
    # Project paths
    PROJECT_ROOT = Path(__file__).parent.parent.parent
    RAG_DIR = PROJECT_ROOT / "rag"
    SCHEMA_DIR = RAG_DIR / "schemas"
    
    @classmethod
    def validate(cls):
        """Validate environment variables"""
        required = [
            "SNOWFLAKE_USER",
            "SNOWFLAKE_PASSWORD", 
            "SNOWFLAKE_ACCOUNT",
            "SNOWFLAKE_WAREHOUSE",
            "ANTHROPIC_API_KEY",
            "PINECONE_API_KEY"  # Added
        ]
        
        missing = [var for var in required if not os.getenv(var)]
        
        if missing:
            raise ValueError(f"Missing: {', '.join(missing)}")
        
        return True

if __name__ == "__main__":
    try:
        Config.validate()
        print(" Configuration valid!")
        print(f" Database: {Config.SNOWFLAKE_CONFIG['database']}")
        print(f" Claude Model: {Config.CLAUDE_SONNET}")
        print(f" Pinecone Index: {Config.PINECONE_INDEX_NAME}")
    except ValueError as e:
        print(f" Configuration error: {e}")