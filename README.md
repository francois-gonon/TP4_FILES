# Data Pipeline with Medallion Architecture

This project implements a complete data pipeline using Apache Spark, Apache Airflow, and MinIO with a medallion architecture (Bronze, Silver, Gold layers).

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│    Airflow      │───▶│     MinIO       │
│ (S3: epf-big-   │    │   (Orchestrator)│    │ (Object Storage)│
│  data-processing│    │                 │    │                 │
│  /tp3-4/sources)│    └─────────────────┘    └─────────────────┘
└─────────────────┘                                     │
                                                        │
                    ┌─────────────────┐                │
                    │     Spark       │◀───────────────┘
                    │  (Processing)   │
                    └─────────────────┘
```

### Medallion Architecture Layers

1. **Landing Zone**: Raw data ingestion from external sources
2. **Bronze Layer**: Raw data with metadata and basic structure
3. **Silver Layer**: Cleaned, validated, and enriched data
4. **Gold Layer**: Analytics-ready, aggregated business data

## 🚀 Quick Start

### Prerequisites

- Docker Desktop installed and running
- PowerShell (Windows) or Bash (Linux/Mac)
- At least 8GB RAM available for Docker

### 1. Start the Pipeline

```powershell
# On Windows
.\start_pipeline.ps1

# On Linux/Mac
chmod +x start_pipeline.sh && ./start_pipeline.sh
```

### 2. Access the Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin/admin |
| MinIO Console | http://localhost:9001 | minioadmin/minioadmin123 |
| Spark Master UI | http://localhost:8081 | - |
| PostgreSQL | localhost:5432 | airflow/airflow |

### 3. Run the Pipeline

1. Open Airflow UI (http://localhost:8080)
2. Login with `admin/admin`
3. Enable the `medallion_data_pipeline` DAG
4. Trigger the DAG manually or wait for scheduled execution

## 📁 Project Structure

```
TP4_FILES/
├── docker-compose.yml          # Main orchestration file
├── start_pipeline.ps1          # Startup script (Windows)
├── airflow/
│   ├── Dockerfile             # Airflow container configuration
│   └── requirements.txt       # Python dependencies
├── dags/
│   ├── medallion_pipeline.py  # Main data pipeline DAG
│   └── setup_connections.py   # Connection setup DAG
├── spark/
│   └── apps/
│       └── data_transformation.py  # Spark transformation jobs
├── scripts/
│   ├── setup_connections.ps1  # Connection setup (Windows)
│   └── setup_connections.sh   # Connection setup (Linux/Mac)
├── logs/                      # Airflow logs
└── plugins/                   # Airflow plugins
```

## 🔄 Data Flow

1. **Ingestion**: Data is pulled from `s3://epf-big-data-processing/tp3-4/sources`
2. **Landing**: Raw data lands in MinIO `landing` bucket
3. **Bronze**: Data is moved to `bronze` bucket with timestamps
4. **Silver**: Spark processes data for cleaning and validation → `silver` bucket
5. **Gold**: Analytics transformations create business-ready data → `gold` bucket

## 🛠️ Services Configuration

### MinIO (Object Storage)
- **Endpoint**: http://localhost:9000 (API) / http://localhost:9001 (Console)
- **Buckets**: landing, bronze, silver, gold
- **Access**: minioadmin / minioadmin123

### Airflow (Orchestration)
- **Web UI**: http://localhost:8080
- **Components**: Webserver, Scheduler, Worker, Init
- **Executor**: CeleryExecutor with Redis
- **Database**: PostgreSQL

### Spark (Processing)
- **Master UI**: http://localhost:8081
- **Workers**: 2 workers (2GB RAM, 2 cores each)
- **Architecture**: Master-Worker cluster

### PostgreSQL (Metadata)
- **Port**: 5432
- **Database**: airflow
- **User**: airflow / airflow

## 📊 Monitoring and Management

### Airflow DAGs

1. **medallion_data_pipeline**: Main data processing pipeline
   - Runs every 6 hours
   - Tasks: ingest → bronze → spark → silver → gold → quality_check

2. **setup_connections**: One-time setup for service connections
   - Manual trigger only
   - Creates connections for MinIO, Spark, PostgreSQL

### Data Quality Checks

The pipeline includes automated data quality checks:
- Bucket content validation
- Data count verification
- Processing status monitoring

## 🔧 Customization

### Adding New Data Sources

1. Modify `dags/medallion_pipeline.py`
2. Update the `ingest_from_s3()` function
3. Add source configuration in environment variables

### Custom Transformations

1. Edit `spark/apps/data_transformation.py`
2. Add new transformation functions
3. Update Spark job submission in Airflow DAG

### Scaling

- **Spark Workers**: Modify `docker-compose.yml` to add more workers
- **Airflow Workers**: Increase worker replicas in compose file
- **Resources**: Adjust memory and CPU limits per service

## 🚨 Troubleshooting

### Common Issues

1. **Services not starting**
   ```bash
   docker-compose logs [service-name]
   ```

2. **Airflow connection issues**
   ```bash
   docker exec airflow-webserver airflow connections list
   ```

3. **MinIO bucket access**
   - Check MinIO console at http://localhost:9001
   - Verify bucket permissions and policies

4. **Spark job failures**
   - Check Spark Master UI at http://localhost:8081
   - Review application logs in Spark History Server

### Log Locations

- **Airflow**: `./logs/`
- **Docker Services**: `docker-compose logs [service]`
- **Spark**: Available through Spark UI

## 🛡️ Security Notes

This setup is for development/testing purposes. For production:

1. Change default passwords
2. Configure proper SSL/TLS
3. Set up proper network security
4. Configure authentication and authorization
5. Use secrets management

## 📝 Pipeline Monitoring

### Key Metrics to Monitor

1. **Data Volume**: Track data size across medallion layers
2. **Processing Time**: Monitor job execution duration
3. **Success Rate**: Track DAG success/failure rates
4. **Resource Usage**: Monitor CPU, memory, and storage

### Alerts and Notifications

Configure Airflow to send notifications on:
- Pipeline failures
- Data quality issues
- SLA breaches
- Resource threshold violations

## 🔄 Backup and Recovery

### Data Backup
- MinIO data: Backup `/data` volumes
- PostgreSQL: Regular database dumps
- Airflow: Backup DAGs and configurations

### Disaster Recovery
- All configurations are in code (Infrastructure as Code)
- Data can be restored from MinIO backups
- Pipeline can be redeployed using docker-compose

## 📈 Performance Optimization

### Spark Optimization
- Adjust worker memory and cores based on workload
- Tune Spark SQL configurations
- Optimize data partitioning strategies

### Airflow Optimization
- Configure appropriate concurrency limits
- Optimize DAG scheduling intervals
- Use connection pooling

### MinIO Optimization
- Configure appropriate bucket policies
- Use lifecycle management for old data
- Implement data compression strategies

## 🤝 Contributing

1. Fork the repository
2. Create feature branch
3. Make changes
4. Test thoroughly
5. Submit pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
