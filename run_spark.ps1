# Launcher for Spark Streaming App
# Sets all required environment variables before starting Python

# --- Java ---
$env:JAVA_HOME = "C:\Program Files\Java\jdk-20"
$env:PATH = "$env:JAVA_HOME\bin;" + $env:PATH
Write-Host "JAVA_HOME: $env:JAVA_HOME"

# --- Hadoop / winutils ---
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;" + $env:PATH
Write-Host "HADOOP_HOME: $env:HADOOP_HOME"

# Verify winutils exists
if (-not (Test-Path "C:\hadoop\bin\winutils.exe")) {
    Write-Host "ERROR: winutils.exe not found at C:\hadoop\bin\winutils.exe"
    Write-Host "Run setup_winutils.ps1 first"
    exit 1
}

Write-Host "Starting Spark Streaming App..."
Write-Host ""

Set-Location -Path "$PSScriptRoot\spark"
& python streaming_app.py
