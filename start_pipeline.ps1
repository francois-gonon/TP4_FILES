# Data Pipeline Startup Script
# This script sets up and starts the complete data pipeline with Spark, Airflow, and MinIO

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "   Data Pipeline Setup & Startup    " -ForegroundColor Cyan
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
        [int]$TimeoutSeconds = 300
    )
    
    Write-Host "Waiting for $ServiceName to be ready..." -ForegroundColor Yellow
    $timeout = (Get-Date).AddSeconds($TimeoutSeconds)
    
    do {
        try {
            $response = Invoke-WebRequest -Uri $Url -TimeoutSec 5 -UseBasicParsing
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

# Set permissions for Airflow (if on Linux subsystem)
Write-Host "Setting up Airflow directories..." -ForegroundColor Blue
if (Test-Path "logs") {
    # Ensure logs directory is writable
}

# Stop any existing containers
Write-Host "Stopping any existing containers..." -ForegroundColor Blue
docker-compose down 2>$null

# Build and start the services
Write-Host "Building and starting services..." -ForegroundColor Blue
docker-compose up -d --build

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

# Wait for Airflow
if (Wait-ForService -ServiceName "Airflow" -Url "http://localhost:8080") {
    Write-Host "Airflow UI: http://localhost:8080 (admin/admin)" -ForegroundColor Cyan
}

# Wait for Spark
if (Wait-ForService -ServiceName "Spark" -Url "http://localhost:8081") {
    Write-Host "Spark UI: http://localhost:8081" -ForegroundColor Cyan
}

# Initialize Airflow database and create admin user
Write-Host ""
Write-Host "Initializing Airflow..." -ForegroundColor Blue
Start-Sleep -Seconds 10

docker exec airflow-init airflow db init 2>$null
docker exec airflow-init airflow users create `
    --username admin `
    --firstname Admin `
    --lastname User `
    --role Admin `
    --email admin@example.com `
    --password admin 2>$null

# Setup connections
Write-Host ""
Write-Host "Setting up Airflow connections..." -ForegroundColor Blue
Start-Sleep -Seconds 5

# Run the connection setup script
& .\scripts\setup_connections.ps1

Write-Host ""
Write-Host "=====================================" -ForegroundColor Green
Write-Host "   Pipeline Setup Complete!         " -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Green
Write-Host ""
Write-Host "Access Points:" -ForegroundColor Yellow
Write-Host "• Airflow UI:    http://localhost:8080 (admin/admin)" -ForegroundColor White
Write-Host "• MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)" -ForegroundColor White
Write-Host "• Spark UI:      http://localhost:8081" -ForegroundColor White
Write-Host "• PostgreSQL:    localhost:5432 (airflow/airflow)" -ForegroundColor White
Write-Host ""
Write-Host "Medallion Architecture Buckets:" -ForegroundColor Yellow
Write-Host "• landing: Raw data ingestion" -ForegroundColor White
Write-Host "• bronze:  Raw data with metadata" -ForegroundColor White
Write-Host "• silver:  Cleaned and validated data" -ForegroundColor White
Write-Host "• gold:    Analytics-ready data" -ForegroundColor White
Write-Host ""
Write-Host "Next Steps:" -ForegroundColor Yellow
Write-Host "1. Open Airflow UI and enable the 'medallion_data_pipeline' DAG" -ForegroundColor White
Write-Host "2. Trigger the pipeline to start data processing" -ForegroundColor White
Write-Host "3. Monitor progress in the Airflow UI" -ForegroundColor White
Write-Host "4. Check data in MinIO buckets" -ForegroundColor White
Write-Host ""
Write-Host "To stop the pipeline: docker-compose down" -ForegroundColor Cyan
