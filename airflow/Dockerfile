FROM apache/airflow:2.7.0

USER root

# Install additional system packages
RUN apt-get update && apt-get install -y \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    boto3==1.29.0 \
    minio==7.1.17 \
    pyspark==3.4.1 \
    requests==2.31.0 \
    pandas==2.1.0 \
    pyarrow==13.0.0

# Copy requirements file
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
