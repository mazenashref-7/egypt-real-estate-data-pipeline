$ErrorActionPreference = "Stop"

$projectDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
Push-Location $projectDirectory

try {
    $runningServices = docker compose ps --status running --services
    if ($LASTEXITCODE -ne 0) {
        throw "Could not read Docker Compose service status."
    }
    if ($runningServices -notcontains "spark" -or $runningServices -notcontains "postgres") {
        throw "Start the spark and postgres services with 'docker compose up -d postgres spark'."
    }

    $sparkArguments = @(
        "--master", "local[4]",
        "--driver-memory", "4g",
        "--conf", "spark.jars.ivy=/tmp/ivy",
        "--packages", "org.postgresql:postgresql:42.7.3"
    )
    $cleaners = @(
        "clean_egypt_locations.py",
        "clean_inflation_monthly.py",
        "clean_usd_egp_monthly.py",
        "clean_egypt_real_estate.py",
        "clean_propertyfinder.py",
        "clean_listings_stream.py"
    )

    foreach ($cleaner in $cleaners) {
        Write-Host "Running $cleaner"
        docker compose exec -T spark /opt/spark/bin/spark-submit @sparkArguments "/app/silver_cleaners/$cleaner"
        if ($LASTEXITCODE -ne 0) {
            throw "Silver cleaner failed: $cleaner"
        }
    }

    Write-Host "Building Silver star schema"
    docker compose exec -T spark /opt/spark/bin/spark-submit @sparkArguments /app/build_silver_star_schema.py
    if ($LASTEXITCODE -ne 0) {
        throw "Silver star-schema build failed."
    }

    Write-Host "Applying Silver key constraints"
    Get-Content -Raw ".\constraints.sql" |
        docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U postgres -d realestate -f -
    if ($LASTEXITCODE -ne 0) {
        throw "Applying Silver constraints failed."
    }

    Write-Host "Profiling Bronze and Silver tables"
    docker compose exec -T spark /opt/spark/bin/spark-submit @sparkArguments /app/Profile_All_tables.py
    if ($LASTEXITCODE -ne 0) {
        throw "Silver profiling failed."
    }

    Write-Host "Silver pipeline completed. Report: .\profiling_report.txt"
}
finally {
    Pop-Location
}
