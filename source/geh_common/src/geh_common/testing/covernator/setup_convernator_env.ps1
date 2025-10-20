Write-Host "==============================================="
Write-Host "Setting up local environment for Covernator"
Write-Host "==============================================="

# --- Path configuration ---
$PythonDir  = "C:\Python312"
# Work machine Java
$JavaDir    = "C:\Users\ClausPetersen\AppData\Local\Programs\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
# Home machine alternative (comment/uncomment as needed)
# $JavaDir = "C:\Users\Energinet\AppData\Local\Programs\Eclipse Adoptium\jdk-25.0.0.36-hotspot"

$SparkDir   = "$PythonDir\Lib\site-packages\pyspark"
$HadoopDir  = "C:\hadoop"
$RepoSrcDir = "C:\repo\opengeh-python-packages\source\geh_common\src"
$PythonExe  = Join-Path $PythonDir "python.exe"

# --- Helper: ensure directory exists ---
function Ensure-Dir {
    param([string]$Path)
    if (Test-Path $Path) {
        Write-Host "✅ Found: $Path"
    } else {
        Write-Warning "❌ Directory not found: $Path"
    }
}

Ensure-Dir $PythonDir
Ensure-Dir $JavaDir
Ensure-Dir $SparkDir
Ensure-Dir $HadoopDir
Ensure-Dir $RepoSrcDir

# --- Helper: set environment variable persistently ---
function Set-EnvVar {
    param([string]$Name,[string]$Value)
    try {
        setx $Name $Value | Out-Null
        Write-Host "Set $Name = $Value"
        # Also set in current session
        $env:$Name = $Value
    } catch {
        Write-Warning "⚠️ Failed to set $Name : $_"
    }
}

# --- Environment variables ---
Set-EnvVar "JAVA_HOME"      $JavaDir
Set-EnvVar "SPARK_HOME"     $SparkDir
Set-EnvVar "HADOOP_HOME"    $HadoopDir
Set-EnvVar "PYSPARK_PYTHON" $PythonExe
Set-EnvVar "PYTHONPATH"     $RepoSrcDir

# --- PATH update ---
Write-Host ""
Write-Host "Updating PATH..."
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
$env:PATH = $NewPath
Write-Host "PATH updated with Python, Spark, Hadoop, and Java binaries."

# --- Python sanity check ---
Write-Host ""
Write-Host "Checking Python environment..."
if (Test-Path $PythonExe) {
    & $PythonExe --version
} else {
    Write-Warning "⚠️ Python executable not found at $PythonExe"
}

# --- Install / update test dependencies ---
Write-Host ""
Write-Host "Installing pytest and plugins..."
try {
    & $PythonExe -m pip install --upgrade pip setuptools wheel | Out-Null
    & $PythonExe -m pip install pytest pytest-cov pytest-xdist | Out-Null
    Write-Host "✅ pytest and plugins installed or already present."
} catch {
    Write-Warning "⚠️ Could not install pytest packages: $_"
}

# --- Verify PYTHONPATH fix works ---
Write-Host ""
Write-Host "Verifying geh_common import..."
try {
    & $PythonExe -c "import geh_common; print('✅ geh_common import OK')"
} catch {
    Write-Warning "❌ geh_common import failed — check PYTHONPATH or folder structure."
}

Write-Host ""
Write-Host "==============================================="
Write-Host "✅ Environment variables have been configured."
Write-Host "Restart PowerShell for changes to take effect."
Write-Host ""
Write-Host "Then run: .\verify_covernator_env.ps1"
Write-Host "==============================================="
