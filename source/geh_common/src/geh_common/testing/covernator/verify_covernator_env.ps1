Write-Host "==============================="
Write-Host "Verifying Covernator environment"
Write-Host "==============================="
$exitCode = 0

function Test-CommandVersion {
    param(
        [string]$Command,
        [string]$Args = "--version",
        [string]$Pattern = ".*",
        [int]$TimeoutSeconds = 5
    )

    try {
        $job = Start-Job -ScriptBlock { param($c,$a) & $c $a 2>&1 } -ArgumentList $Command,$Args
        if (Wait-Job $job -Timeout $TimeoutSeconds) {
            $output = Receive-Job $job
            Remove-Job $job | Out-Null
            if ($output -match $Pattern) {
                Write-Host "OK: $Command $($Matches[0])"
                return $true
            } else {
                Write-Warning "$Command returned unexpected output: $output"
                return $false
            }
        } else {
            Stop-Job $job | Out-Null
            Remove-Job $job | Out-Null
            Write-Warning "$Command did not respond within ${TimeoutSeconds}s"
            return $false
        }
    }
    catch {
        Write-Warning "$Command failed or not found: $_"
        return $false
    }
}

# ---------- Python ----------
Write-Host "`nChecking Python..."
try {
    $pythonVersion = & python --version 2>&1
    if ($pythonVersion -match "Python\s+\d+\.\d+") {
        Write-Host "OK: $pythonVersion"
    } else {
        Write-Warning "Warning: python returned unexpected output: $pythonVersion"
    }
} catch {
    Write-Warning "❌ Python not found: $_"
}



# ---------- pytest ----------
Write-Host "`nChecking pytest..."
try {
    & pytest --version *> $null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "OK: pytest installed"
    }
    else {
        Write-Warning "pytest not working"
        $exitCode++
    }
}
catch {
    Write-Warning "pytest not installed"
    $exitCode++
}

# ---------- Java ----------
Write-Host "`nChecking Java (expecting 17)..."
try {
    $javaOutput = & java -version 2>&1 | Out-String
    if ($javaOutput -match "17\." -or $javaOutput -match "openjdk") {
        Write-Host "Java detected:" $javaOutput.Split("`n")[0]
    } else {
        Write-Warning "Java found, but version may not be 17"
        Write-Host $javaOutput
    }
} catch {
    Write-Warning "❌ Java not found or not callable: $_"
}


# ---------- Environment variables ----------
Write-Host "`nChecking environment variables..."
$vars = "JAVA_HOME","SPARK_HOME","HADOOP_HOME","PYSPARK_PYTHON","PYTHONPATH"
foreach ($v in $vars) {
    $val = (Get-Item "Env:$v" -ErrorAction SilentlyContinue).Value
    if ($val) {
        Write-Host "$v = $val"
    }
    else {
        Write-Warning "$v not set"
        $exitCode++
    }
}

# ---------- Spark ----------
Write-Host "`nChecking Spark..."
if ($env:SPARK_HOME) {
    $spark = Join-Path $env:SPARK_HOME "bin\spark-submit.cmd"
    if (Test-Path $spark) {
        try {
            & $spark --version 2>&1 | Select-String "version"
            Write-Host "OK: Spark command works"
        }
        catch {
            Write-Warning "Spark present but not starting"
        }
    }
    else {
        Write-Warning "spark-submit.cmd not found"
        $exitCode++
    }
}
else {
    Write-Warning "SPARK_HOME not set"
    $exitCode++
}

# ---------- Hadoop / winutils ----------
Write-Host "`nChecking Hadoop..."
if ($env:HADOOP_HOME) {
    $win = Join-Path $env:HADOOP_HOME "bin\winutils.exe"
    if (Test-Path $win) {
        Write-Host "OK: winutils found"
    }
    else {
        Write-Warning "winutils.exe missing"
        $exitCode++
    }
}
else {
    Write-Warning "HADOOP_HOME not set"
    $exitCode++
}

# ---------- PYTHONPATH quick validation ----------
Write-Host "`nChecking PYTHONPATH for geh_common..."
if ($env:PYTHONPATH -and (Test-Path (Join-Path $env:PYTHONPATH "geh_common"))) {
    Write-Host "OK: PYTHONPATH points to geh_common"
}
else {
    Write-Warning "PYTHONPATH may be incorrect"
}

# ---------- Summary ----------
Write-Host "`n==============================="
if ($exitCode -eq 0) {
    Write-Host "All checks passed"
}
else {
    Write-Warning "$exitCode problem(s) detected"
}
exit $exitCode
