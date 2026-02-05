FROM python:3.11-slim
 
WORKDIR /app
 
# Install system dependencies

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    ca-certificates \   
    && rm -rf /var/lib/apt/lists/*
 
# Copy requirements and install Python dependencies

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy application code

COPY server.py .

COPY database.py .

COPY models.py .

COPY tools/ tools/
 
# Expose port (CHANGED TO 8001)

EXPOSE 8001
 
# Health check (CHANGED TO 8001)

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \

  CMD python -c "import requests; requests.get('http://localhost:8001/')"
 
# Run server (CHANGED TO 8001)

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8001", "--reload"]