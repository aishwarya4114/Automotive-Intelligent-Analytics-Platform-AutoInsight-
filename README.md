# AutoInsight - Automotive Intelligent Platform #
```
FastAPI: [http://18.191.175.145:8001/docs](http://18.191.175.145:8001/docs)
Streamlit: http://18.191.175.145:8501/
Airflow: http://34.173.63.145:8080
```
---

## Overview

**AutoInsight** automates automotive business intelligence by:

1. **Ingesting** 84GB of automotive data from NHTSA (safety), EPA (fuel economy), and FRED (economic indicators)
2. **Processing** through distributed Apache Spark ETL pipelines with sentiment analysis
3. **Storing** 224,590+ cleaned records in Snowflake data warehouse
4. **Querying** via 5-agent LLM system with RAG-powered schema retrieval
5. **Visualizing** with automatic chart generation and professional analytics
6. **Securing** with RBAC, SQL injection prevention, and HITL approval workflows
7. **Orchestrating** with Apache Airflow (daily refresh + full ingestion)

### Key Finding

- **95.3% SQL Accuracy** through multi-agent validation and RAG schema retrieval
- **87% Token Reduction** (15K → 2K tokens) via Pinecone semantic search
- **88% Hallucination Reduction** (40% → 4.8%) using RAG + schema grounding
- **5-Agent Architecture** with automatic visualization generation
- **210,517 Complaints** processed with standalone sentiment analysis (5-10ms each)
---

##  Architecture
```
┌──────────────────────────────────────────────────────────────────────────┐
│                        Data Sources (84GB)                                │
│  NHTSA (83GB) | EPA (0.5GB) | FRED (0.025GB) | AutoNews.com (Scraper)   │
└────────────────────────────┬─────────────────────────────────────────────┘
                             │
                             ▼
                    ┌────────────────┐
                    │ Apache Airflow │ (3 DAGs: Data, Cleaning, News)
                    │  GCP VM Deploy │
                    └────────┬───────┘
                             │
                             ▼
                    ┌────────────────┐
                    │    AWS S3      │ (Raw → Processed → Curated)
                    │  84GB Storage  │
                    └────────┬───────┘
                             │
                             ▼
                    ┌────────────────┐
                    │ Apache Spark   │ (3.5.0 on EMR/Local)
                    │  ETL + Clean   │ Sentiment Analysis
                    └────────┬───────┘
                             │
                             ▼
                    ┌────────────────┐
                    │   Snowflake    │ (AUTOMOTIVE_AI Database)
                    │ 224,590 Records│ 6 Fact Tables + Security
                    └────────┬───────┘
                             │
        ┌────────────────────┴────────────────────┐
        │                                         │
        ▼                                         ▼
┌───────────────┐                      ┌──────────────────┐
│ RAG Layer     │                      │ MCP News Agent   │
│ Pinecone +    │                      │ 5 Tools         │
│ SentenceTrans │                      │ Claude Dashboard │
└───────┬───────┘                      └────────┬─────────┘
        │                                       │
        ▼                                       │
┌───────────────────────────────────────────────┴─────────┐
│            5-Agent LLM System (LangGraph)                │
│  Agent 1: Query Planner (Haiku)                         │
│  Agent 2: SQL Generator (Sonnet) + RBAC                 │
│  Agent 3: Validator (Haiku) + Cost Est + HITL           │
│  Agent 4: Executor/Analyzer (Sonnet) + Filtering        │
│  Agent 5: Visualization (Plotly) - No LLM               │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
                  ┌────────────────┐
                  │  Streamlit UI  │ (2 Tabs: Query + News)
                  │  Professional  │
                  │  Visualizations│
                  └────────────────┘
```

---

## Features

### Core Capabilities
- **Multi-Source Data Integration** - NHTSA (complaints, recalls, ratings, investigations), EPA (fuel economy), FRED (economic indicators)
- **Distributed ETL** - Apache Spark 3.5.0 with 5-20 autoscaling nodes
- **5-Agent LLM System** - Query planning, SQL generation, validation, execution, visualization
- **RAG Schema Retrieval** - Pinecone + SentenceTransformer (384-dim embeddings)
- **Sentiment Analysis** - Standalone rule-based system (30+ safety keywords, negation handling)
- **RBAC Security** - 3-tier access control (Analyst, Manager, Executive)
- **HITL Approval** - Human-in-the-loop for high-cost/high-risk queries
- **MCP News Agent** - 5 tools for automotive news/events with Claude-powered dashboards
- **Apache Airflow** - Automated orchestration on GCP
- **Cloud Storage** - AWS S3 + Snowflake data warehouse
- **Professional Visualizations** - Automatic chart selection (bar, scatter, box plots)
- **Docker Support** - Containerized deployment with Docker Compose

### Dashboard Sections
**Tab 1: Query Agent (Natural Language SQL)**
- Natural language to SQL conversion
- RBAC column-level filtering
- Cost estimation and approval workflows
- Business insights generation
- Automatic chart rendering

**Tab 2: News & Events Portal (MCP-Powered)**
- Breaking automotive news (last 24 hours)
- Manufacturer-specific news search
- Upcoming events calendar
- Claude-powered article dashboards
- Event strategic analysis

### Data Processing
- **6 Fact Tables:** Complaints (210,517), Fuel Economy (8,527), Ratings (3,091), Recalls (2,146), Investigations (178), Economic Indicators (131)
- **Data Quality:** Manufacturer standardization, date validation, numeric range checking, deduplication, severity classification
- **Sentiment Analysis:** 5-10ms per complaint, 5 categories (VERY_NEGATIVE to VERY_POSITIVE)
- **Vehicle ID:** SHA-256 hashing for cross-table joins

---

---

## Installation

### Prerequisites
- Python 3.11+
- Docker & Docker Compose
- AWS Account (S3 access)
- Snowflake Account
- Anthropic API Key (Claude)
- Pinecone API Key
- GCP Account (for Airflow deployment)
  
### Local Development

#### 1. Clone Repository
```bash
git clone https://github.com/YOUR_USERNAME/autoinsight.git
cd autoinsight
```

#### 2. Environment Setup
```bash
# Create .env file
cat > .env << EOF
# Anthropic (Claude)
ANTHROPIC_API_KEY=your_anthropic_api_key

# AWS S3
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=autoinsightfinal

# Snowflake
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_WAREHOUSE=your_WH
SNOWFLAKE_DATABASE=your_db

# Pinecone
PINECONE_API_KEY=your_pinecone_api_key
PINECONE_ENVIRONMENT=us-east-1
PINECONE_INDEX_NAME=your_pinecone_index

# FRED API
FRED_API_KEY=your_fred_api_key

# Spark Configuration
SPARK_ENV=local
SPARK_DRIVER_MEMORY=4g
SPARK_EXECUTOR_MEMORY=4g

# Airflow
AIRFLOW_UID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
EOF
```

#### 3. Install Dependencies (Non-Docker)
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install requirements
pip install -r docker/requirements.txt
```

#### 4. Setup Pinecone Vector Store
```bash
# Initialize schema embeddings
python src/rag/schema_extractor.py
python src/rag/vector_store.py
```

#### 5. Run Streamlit Locally
```bash
# Terminal 1: Start Streamlit
streamlit run src/streamlit_app.py --server.port 8501

# Terminal 2: Start MCP Server (optional)
cd src/mcp_server
uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

#### 6. Access Services
- **Streamlit UI**: http://localhost:8501
- **MCP API Docs**: http://localhost:8001/docs
- **Airflow** : http://localhost:8080

### GCP Cloud Deployment (Apache Airflow)

#### Step 1: Enable Required APIs

1. Go to **APIs & Services → Library**
2. Enable:
   - Compute Engine API
   - Cloud SQL Admin API
   - Cloud Storage API

#### Step 2: Create VM Instance

```bash
# Create VM
gcloud compute instances create airflow-server \
  --machine-type=e2-highmem-4 \
  --zone=us-central1-a \
  --boot-disk-size=100GB \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --tags=http-server,https-server

# Configure firewall
gcloud compute firewall-rules create allow-airflow-services \
  --allow=tcp:8081,tcp:5434 \
  --target-tags=http-server
```

**VM Specifications:**
- **Recommended:** e2-highmem-4 (4 vCPU, 32GB RAM) - Best for Spark workloads
- **Minimum:** e2-standard-4 (4 vCPU, 16GB RAM) - May require memory tuning

#### Step 3: SSH into VM

```bash
gcloud compute ssh airflow-server --zone=us-central1-a
```

#### Step 4: Install Docker & Dependencies

```bash
# Update system
sudo apt-get update && sudo apt-get upgrade -y

# Install Docker
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.24.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verify
docker --version
docker-compose --version

# Re-login for docker group
exit
# SSH back in
```

#### Step 5: Upload Application Files to Cloud Storage

```bash
# On your local machine
# Create bucket
gsutil mb gs://aurelia-airflow-deploy-[unique-id]

# Upload application files (zipped)
cd /path/to/autoinsight
zip -r autoinsight-app.zip . -x "*.git*" -x "*__pycache__*" -x "*.pyc"
gsutil cp autoinsight-app.zip gs://aurelia-airflow-deploy-[unique-id]/

# Make file public temporarily
gsutil acl ch -u AllUsers:R gs://aurelia-airflow-deploy-[unique-id]/autoinsight-app.zip

# Get public URL
echo "https://storage.googleapis.com/aurelia-airflow-deploy-[unique-id]/autoinsight-app.zip"
```

#### Step 6: Download and Extract on VM

```bash
# On GCP VM
cd ~

# Download application
wget "YOUR_PUBLIC_URL" -O autoinsight-app.zip

# Install unzip
sudo apt-get install -y unzip

# Extract
unzip autoinsight-app.zip

# Verify
ls -la
```

#### Step 7: Configure Environment Variables

```bash
cd ~/autoinsight
nano .env

# Add all your credentials (same as local setup)
```

#### Step 8: Deploy with Docker Compose

```bash
# Build images
docker-compose build

# Start services
docker-compose up -d

# Check status
docker ps

# View logs
docker-compose logs -f airflow-webserver
```

#### Step 9: Access Airflow UI

```bash
# Get external IP
curl -s http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip -H "Metadata-Flavor: Google"

# Access at: http://EXTERNAL_IP:8081
# Default credentials: admin/admin
```

#### Step 10: Trigger DAGs

1. Login to Airflow UI
2. Enable DAGs:
   - `autoinsight_data_pipeline` - Daily data collection
   - `autoinsight_cleaning_pipeline_v2` - Spark ETL
   - `autoinsight_daily_scraper` - News scraping
3. Manually trigger initial runs

#### Step 11 AWS EC2 Deployment (FastAPI + Streamlit)
### Services Deployed

FastAPI (MCP Server): http://18.191.175.145:8001/docs
Streamlit UI: http://18.191.175.145:8501/

#### Deployment Steps

```bash
# SSH into EC2 instance
ssh -i your-key.pem ubuntu@your-ec2-ip

# Clone repository
git clone https://github.com/YOUR_USERNAME/autoinsight.git
cd autoinsight

# Configure environment variables
nano .env  # Add Snowflake, Anthropic, Pinecone credentials

# Deploy with Docker Compose
sudo docker-compose up --build -d

# Verify services
sudo docker ps
```
### EC2 Configuration

- **Instance Type**: t3.medium (2 vCPU, 4GB RAM)
- **Security Groups**: Open ports 8001 (FastAPI), 8501 (Streamlit)
---
## System Requirements

### Local Development
- **CPU:** 4+ cores
- **RAM:** 16GB minimum (32GB recommended)
- **Storage:** 50GB available space
- **OS:** macOS, Linux, Windows with WSL2

### GCP Production
- **VM Type:** e2-highmem-4 (4 vCPU, 32GB RAM) recommended
- **Boot Disk:** 100GB
- **Storage:** AWS S3 for data (84GB+)
- **Database:** Snowflake (AUTOMOTIVE_AI)
- **Network:** Open ports 8081 (Airflow), 8501 (Streamlit), 8001 (MCP)

---
## Data Sources

- **NHTSA ODI API** - Complaints, Recalls, Ratings, Investigations
  - Website: https://api.nhtsa.gov/
  - Coverage: 2015-2025
  - Size: 83GB (210K+ complaints)

- **EPA Fuel Economy API** - Vehicle specifications
  - Website: https://www.fueleconomy.gov/feg/ws/
  - Coverage: 2015-2025
  - Size: 0.5GB (8,527 vehicles)

- **FRED API** - Economic indicators
  - Website: https://fred.stlouisfed.org/docs/api/
  - Coverage: 2015-present
  - Size: 0.025GB (131 observations)

- **AutoNews.com** - News & Events (dynamic scraping)
  - Website: https://www.autonews.com
  - Method: Playwright + NLP extraction

---
## License

This project is for academic purposes (Northeastern University DAMG7245).## Technology Stack

### Data Infrastructure
- **Apache Spark 3.5.0** - Distributed ETL
- **Apache Airflow 2.7.0** - Orchestration
- **AWS S3** - Data lake storage
- **Snowflake** - Data warehouse
- **PostgreSQL** - Airflow metadata

### AI/ML
- **Claude Sonnet 4** - SQL generation, analysis
- **Claude Haiku** - Query planning, validation
- **Pinecone** - Vector database (serverless)
- **SentenceTransformer** - Embeddings (all-MiniLM-L6-v2)
- **LangGraph** - Multi-agent orchestration
- **LangChain** - MCP tool integration

### Backend
- **FastAPI** - MCP server
- **Pydantic** - Data validation
- **Python 3.11** - Core language

### Frontend
- **Streamlit** - Interactive UI
- **Plotly** - Visualizations

### DevOps
- **Docker & Docker Compose** - Containerization
- **GCP Compute Engine** - Cloud hosting
- **Git** - Version control

---
