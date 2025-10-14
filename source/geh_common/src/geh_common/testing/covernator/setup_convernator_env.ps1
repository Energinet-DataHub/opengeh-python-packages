Write-Host "==============================================="
Write-Host "Setting up local environment for Covernator"
Write-Host "==============================================="

# --- Paths ---
$PythonDir  = "C:\Python312\"
$JavaDir    = "c:\Users\Energinet\AppData\Local\Programs\Eclipse Adoptium\jdk-25.0.0.36-hotspot\bin\"
$SparkDir   = "$PythonDir\Lib\site-packages\pyspark"
$HadoopDir  = "C:\hadoop"
$RepoSrcDir = "C:\repo\opengeh-python-packages\source\geh_common\src"
$PythonExe  = Join-Path $PythonDir "python.exe"

function Ensure-Dir {
    param([string]$Path)
    if (Test-Path $Path) {
        Write-Host "Found: $Path"
    } else {
        Write-Warning "Directory not found: $Path"
    }
}

Ensure-Dir $PythonDir
Ensure-Dir $JavaDir
Ensure-Dir $SparkDir
Ensure-Dir $HadoopDir
Ensure-Dir $RepoSrcDir

function Set-EnvVar {
    param([string]$Name,[string]$Value)
    try {
        setx $Name $Value | Out-Null
        Write-Host "Set $Name = $Value"
    } catch {
        Write-Warning "Failed to set $Name : $_"
    }
}

# --- Environment variables ---
Set-EnvVar "JAVA_HOME"      $JavaDir
Set-EnvVar "SPARK_HOME"     $SparkDir
Set-EnvVar "HADOOP_HOME"    $HadoopDir
Set-EnvVar "PYSPARK_PYTHON" $PythonExe
Set-EnvVar "PYTHONPATH"     $RepoSrcDir

# --- PATH update ---
$AddPaths = @(
    "$PythonDir",
    "$PythonDir\Scripts",
    "$HadoopDir\bin",
    "$SparkDir\bin",
    "$JavaDir\bin"
)
$OldPath = [Environment]::GetEnvironmentVariable("PATH","Machine")
$NewPath = ($AddPaths + $OldPath.Split(";") | Select-Object -Unique) -join ";"
setx PATH $NewPath | Out-Null
Write-Host "PATH updated with Python, Spark, Hadoop, and Java"

# --- Python packages ---
Write-Host ""
Write-Host "Installing pytest and plugins..."
try {
    & $PythonExe -m pip install --upgrade pip setuptools wheel | Out-Null
    & $PythonExe -m pip install pytest pytest-cov pytest-xdist | Out-Null
    Write-Host "pytest and plugins installed or already present."
} catch {
    Write-Warning "Could not install pytest packages: $_"
}

Write-Host ""
Write-Host "==============================================="
Write-Host "Environment variables have been configured."
Write-Host "Restart PowerShell for them to take effect."
Write-Host "Then run: .\verify_covernator_env.ps1"
Write-Host "==============================================="
