# Data Pipeline Startup Script
# Medallion Architecture with Spark, Airflow, and MinIO

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "   Data Pipeline Startup             " -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""

# Function to check if Docker is running
function Test-DockerRunning {
    try {
        docker version | Out-Null
        return $true
    }
    catch {
        return $false
    }
}

# Function to wait for service to be ready
function Wait-ForService {
    param(
        [string]$ServiceName,
        [string]$Url,
        [int]$TimeoutSeconds = 180
    )
    
    Write-Host "Waiting for $ServiceName to be ready..." -ForegroundColor Yellow
    $timeout = (Get-Date).AddSeconds($TimeoutSeconds)
    
    do {
        try {
            $response = Invoke-WebRequest -Uri $Url -TimeoutSec 10 -UseBasicParsing
            if ($response.StatusCode -eq 200) {
                Write-Host "$ServiceName is ready!" -ForegroundColor Green
                return $true
            }
        }
        catch {
            Start-Sleep -Seconds 5
        }
    } while ((Get-Date) -lt $timeout)
    
    Write-Host "$ServiceName failed to start within $TimeoutSeconds seconds" -ForegroundColor Red
    return $false
}

# Check if Docker is running
Write-Host "Checking Docker status..." -ForegroundColor Blue
if (-not (Test-DockerRunning)) {
    Write-Host "Docker is not running. Please start Docker Desktop and try again." -ForegroundColor Red
    exit 1
}
Write-Host "Docker is running!" -ForegroundColor Green

# Create necessary directories
Write-Host "Creating necessary directories..." -ForegroundColor Blue
$directories = @(
    "logs",
    "plugins",
    "spark/data"
)

foreach ($dir in $directories) {
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
        Write-Host "Created directory: $dir" -ForegroundColor Green
    }
}

# Stop any existing containers
Write-Host "Stopping any existing containers..." -ForegroundColor Blue
docker-compose down 2>$null

# Start the data pipeline services
Write-Host "Starting data pipeline..." -ForegroundColor Blue
docker-compose up -d

if ($LASTEXITCODE -ne 0) {
    Write-Host "Failed to start services. Check the logs above." -ForegroundColor Red
    exit 1
}

# Wait for services to be ready
Write-Host ""
Write-Host "Waiting for services to initialize..." -ForegroundColor Blue

# Wait for MinIO
if (Wait-ForService -ServiceName "MinIO" -Url "http://localhost:9001") {
    Write-Host "MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)" -ForegroundColor Cyan
}

# Wait for Airflow (this may take longer)
Write-Host "Waiting for Airflow to initialize (this may take a few minutes)..." -ForegroundColor Yellow
if (Wait-ForService -ServiceName "Airflow" -Url "http://localhost:8080" -TimeoutSeconds 300) {
    Write-Host "Airflow UI: http://localhost:8080 (admin/admin)" -ForegroundColor Cyan
} else {
    Write-Host "Airflow is taking longer than expected. You can check manually at http://localhost:8080" -ForegroundColor Yellow
}

# Wait for Spark
if (Wait-ForService -ServiceName "Spark" -Url "http://localhost:8081") {
    Write-Host "Spark UI: http://localhost:8081" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "=====================================" -ForegroundColor Green
Write-Host "   Pipeline Ready!                  " -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Green
Write-Host ""
Write-Host "Access Points:" -ForegroundColor Yellow
Write-Host "• Airflow UI:    http://localhost:8080 (admin/admin)" -ForegroundColor White
Write-Host "• MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)" -ForegroundColor White
Write-Host "• Spark UI:      http://localhost:8081" -ForegroundColor White
Write-Host ""
Write-Host "Medallion Architecture:" -ForegroundColor Yellow
Write-Host "• landing: Raw data ingestion" -ForegroundColor White
Write-Host "• bronze:  Raw data with metadata" -ForegroundColor White
Write-Host "• silver:  Cleaned and processed data" -ForegroundColor White
Write-Host "• gold:    Analytics-ready data" -ForegroundColor White
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "1. Open Airflow UI at http://localhost:8080" -ForegroundColor White
Write-Host "2. Enable the 'medallion_spark_pipeline' DAG" -ForegroundColor White
Write-Host "3. Trigger the pipeline manually" -ForegroundColor White
Write-Host "4. Monitor Spark jobs in Spark UI at http://localhost:8082" -ForegroundColor White
Write-Host "5. Check results in MinIO Console" -ForegroundColor White
Write-Host ""
Write-Host "🚀 ENHANCED SPARK ARCHITECTURE:" -ForegroundColor Yellow
Write-Host "• Distributed processing with Apache Spark" -ForegroundColor White
Write-Host "• Production-grade error handling and monitoring" -ForegroundColor White
Write-Host "• Comprehensive data quality validation" -ForegroundColor White
Write-Host "• Advanced analytics and business insights" -ForegroundColor White
Write-Host ""
Write-Host "To stop: docker-compose down" -ForegroundColor Cyan
Write-Host ""
Write-Host "Checking service status..." -ForegroundColor Blue
docker-compose ps
