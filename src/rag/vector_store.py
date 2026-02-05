from pinecone import Pinecone, ServerlessSpec
from sentence_transformers import SentenceTransformer
import json
from pathlib import Path
import sys
import time

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / 'src'))
from data_cleaning.utils.config import Config

class PineconeVectorStore:
    """Pinecone-based vector store for schema RAG"""
    
    def __init__(self):
        print(" Initializing Pinecone...")
        
        # Initialize Pinecone
        self.pc = Pinecone(api_key=Config.PINECONE_API_KEY)
        
        # Initialize embedding model (local, free)
        print(" Loading embedding model...")
        self.embedding_model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
        self.embedding_dim = 384  # Dimension for all-MiniLM-L6-v2
        
        self.index_name = Config.PINECONE_INDEX_NAME
        self.index = None
        
        print(" Initialized!\n")
    
    def create_index(self):
        """Create Pinecone index if it doesn't exist"""
        
        # Check if index exists
        existing_indexes = [index.name for index in self.pc.list_indexes()]
        
        if self.index_name in existing_indexes:
            print(f" Index '{self.index_name}' already exists")
            self.index = self.pc.Index(self.index_name)
        else:
            print(f" Creating new index '{self.index_name}'...")
            
            self.pc.create_index(
                name=self.index_name,
                dimension=self.embedding_dim,
                metric='cosine',
                spec=ServerlessSpec(
                    cloud='aws',
                    region=Config.PINECONE_ENVIRONMENT
                )
            )
            
            # Wait for index to be ready
            print(" Waiting for index to be ready...")
            while not self.pc.describe_index(self.index_name).status['ready']:
                time.sleep(1)
            
            self.index = self.pc.Index(self.index_name)
            print(" Index created!\n")
    
    def load_schema_docs(self) -> list:
        """Load schema documentation from JSON"""
        schema_file = Config.SCHEMA_DIR / "autoinsight_schema.json"
        
        if not schema_file.exists():
            raise FileNotFoundError(
                f"Schema file not found: {schema_file}\n"
                "Run: python rag/schema_extractor.py first!"
            )
        
        with open(schema_file, 'r') as f:
            schema = json.load(f)
        
        # Convert to documents
        docs = []
        for table_name, table_info in schema.items():
            # Create rich text description
            doc_text = f"""
Table: {table_info['table_name']}
Schema: {table_info['schema']}
Full Name: AUTOMOTIVE_AI.{table_name}
Row Count: {table_info['row_count']:,}

Columns:
"""
            for col in table_info['columns']:
                doc_text += f"- {col['name']} ({col['type']})\n"
            
            docs.append({
                "id": table_name.replace('.', '_').lower(),
                "text": doc_text,
                "metadata": {
                    "table_name": table_name,
                    "schema": table_info['schema'],
                    "table": table_info['table_name'],
                    "row_count": table_info['row_count'],
                    "column_count": len(table_info['columns'])
                }
            })
        
        return docs
    
    def create_embeddings(self, docs: list):
        """Create embeddings and upload to Pinecone"""
        
        print(f" Creating embeddings for {len(docs)} documents...")
        
        vectors = []
        for doc in docs:
            # Create embedding
            embedding = self.embedding_model.encode(doc['text']).tolist()
            
            vectors.append({
                "id": doc['id'],
                "values": embedding,
                "metadata": {
                    **doc['metadata'],
                    "text": doc['text'][:1000]  # Store first 1000 chars in metadata
                }
            })
        
        # Upload to Pinecone
        print(f"  Uploading {len(vectors)} vectors to Pinecone...")
        self.index.upsert(vectors=vectors)
        
        print(" Upload complete!\n")
        
        # Verify
        stats = self.index.describe_index_stats()
        print(f" Index stats:")
        print(f"   Total vectors: {stats['total_vector_count']}")
        print(f"   Dimension: {stats['dimension']}")
    
    def search_schema(self, query: str, top_k: int = 3):
        """Search for relevant tables"""
        
        # Create query embedding
        query_embedding = self.embedding_model.encode(query).tolist()
        
        # Search Pinecone
        results = self.index.query(
            vector=query_embedding,
            top_k=top_k,
            include_metadata=True
        )
        
        return results['matches']
    
    def get_schema_for_agent(self, query: str, top_k: int = 3) -> str:
        """
        Get schema information formatted for Claude agents
        This is what agents will call!
        """
        results = self.search_schema(query, top_k)
        
        if not results:
            return "No relevant tables found."
        
        schema_text = "RELEVANT SNOWFLAKE SCHEMA:\n\n"
        
        for i, match in enumerate(results, 1):
            metadata = match['metadata']
            score = match['score']
            
            schema_text += f"{i}. Table: {metadata['table_name']}\n"
            schema_text += f"   Relevance: {score:.2%}\n"
            schema_text += f"   {metadata.get('text', '')}\n"
            schema_text += "-" * 80 + "\n\n"
        
        return schema_text


def main():
    """Setup Pinecone vector store"""
    
    print("="*80)
    print(" AutoInsight Pinecone Vector Store Setup")
    print("="*80)
    print()
    
    # Initialize
    vs = PineconeVectorStore()
    
    # Create index
    vs.create_index()
    
    # Load schema docs
    print(" Loading schema documentation...")
    docs = vs.load_schema_docs()
    print(f" Loaded {len(docs)} schema documents\n")
    
    # Create embeddings and upload
    vs.create_embeddings(docs)
    
    # Test search
    print("="*80)
    print(" Testing Vector Search")
    print("="*80)
    print()
    
    test_queries = [
        "recalls and safety issues",
        "fuel economy and emissions",
        "economic indicators and sales"
    ]
    
    for query in test_queries:
        print(f" Query: '{query}'")
        results = vs.search_schema(query, top_k=2)
        
        for match in results:
            print(f"    {match['metadata']['table_name']} (score: {match['score']:.2%})")
        print()
    
    print("="*80)
    print(" Pinecone Vector Store Setup Complete!")
    print("="*80)
    print()
    print(" Index Name:", Config.PINECONE_INDEX_NAME)
    print(" Total Vectors:", len(docs))
    print(" Next: Build agents that use this vector store!")


if __name__ == "__main__":
    main()