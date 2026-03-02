# Download winutils.exe for Hadoop 3.3.x on Windows
# This is required for PySpark to run on Windows

$HADOOP_HOME = "C:\hadoop"
$HADOOP_BIN = "$HADOOP_HOME\bin"

# Create directories
Write-Host "Creating Hadoop directories..."
New-Item -ItemType Directory -Force -Path $HADOOP_BIN | Out-Null

# Download winutils.exe
$winutilsUrl = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe"
$hadoopDllUrl = "https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll"

Write-Host "Downloading winutils.exe..."
try {
    Invoke-WebRequest -Uri $winutilsUrl -OutFile "$HADOOP_BIN\winutils.exe" -UseBasicParsing
    Write-Host "✓ winutils.exe downloaded"
} catch {
    Write-Host "✗ Failed to download winutils.exe: $_"
    exit 1
}

Write-Host "Downloading hadoop.dll..."
try {
    Invoke-WebRequest -Uri $hadoopDllUrl -OutFile "$HADOOP_BIN\hadoop.dll" -UseBasicParsing
    Write-Host "✓ hadoop.dll downloaded"
} catch {
    Write-Host "✗ Failed to download hadoop.dll: $_"
    exit 1
}

# Set HADOOP_HOME system environment variable
Write-Host "Setting HADOOP_HOME environment variable..."
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", $HADOOP_HOME, "User")
$env:HADOOP_HOME = $HADOOP_HOME
Write-Host "✓ HADOOP_HOME set to $HADOOP_HOME"

# Create temp dir used by Spark
New-Item -ItemType Directory -Force -Path "C:\tmp\hive" | Out-Null
# Grant permissions (winutils needs this)
& "$HADOOP_BIN\winutils.exe" chmod 777 C:\tmp\hive 2>$null

Write-Host ""
Write-Host "✓ Setup complete! Now run: python streaming_app.py"
Write-Host ""
Write-Host "NOTE: Open a NEW terminal so HADOOP_HOME takes effect, or run:"
Write-Host '  $env:HADOOP_HOME = "C:\hadoop"'
