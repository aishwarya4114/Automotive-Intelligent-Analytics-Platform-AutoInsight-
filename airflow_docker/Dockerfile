# Start from the official Airflow image
FROM apache/airflow:2.7.3-python3.11

# ============================================================================
# SYSTEM DEPENDENCIES (AS ROOT)
# ============================================================================
USER root

# Install Java (for PySpark) + Playwright dependencies + general utilities
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        # Java for PySpark
        openjdk-11-jdk-headless \
        # Build tools
        build-essential \
        libpq-dev \
        curl \
        wget \
        procps \
        # Playwright/Chromium dependencies
        libnss3 \
        libfontconfig1 \
        libgconf-2-4 \
        libasound2 \
        libatk-bridge2.0-0 \
        libgtk-3-0 \
        libcups2 \
        libxcomposite1 \
        libxrandr2 \
        libgbm-dev \
        libxkbcommon0 \
        libatk1.0-0 \
        libgdk-pixbuf2.0-0 \
        libxtst6 \
        libdrm-dev \
        libexpat1 \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ============================================================================
# DOWNLOAD & INSTALL SPARK (Lightweight - just what we need)
# ============================================================================

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Download and install Spark
RUN cd /opt && \
    wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Download Hadoop AWS JARs for S3 support
RUN cd $SPARK_HOME/jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -q https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Set Spark configuration
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python
ENV SPARK_NO_DAEMONIZE=true

# ============================================================================
# PYTHON PACKAGE INSTALLATION (AS AIRFLOW USER)
# ============================================================================
USER airflow

# Copy requirements file
COPY requirements.txt /tmp/requirements.txt

# Install Python packages
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt

# Install Playwright browser binary (chromium)
RUN playwright install chromium

# Set working directory
WORKDIR /opt/airflow