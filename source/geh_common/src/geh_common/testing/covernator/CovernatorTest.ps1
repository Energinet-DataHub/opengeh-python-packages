Clear-Host

# Force DEBUG logging and set Python search path
$env:PYTHONLOGLEVEL = "DEBUG"
$env:PYTHONPATH = "C:\repo\opengeh-python-packages\source\geh_common\src;$env:PYTHONPATH"

# Choose the repo you want to test
#$subsystemFolder = "geh_settlement_report"
$subsystemFolder = "geh_wholesale"
#$subsystemFolder = "geh_calculated_measurements"

#$repoRoot = "c:\repo\geh-settlement-report\"
$repoRoot = "C:\repo\opengeh-wholesale"
#$repoRoot = "C:\repo\opengeh-measurements"

$projectFolder = Join-Path $repoRoot "source\$subsystemFolder"

# Resolve Desktop output path
$desktop = [Environment]::GetFolderPath("Desktop")

# Run the script
python -c "import logging, runpy, sys; logging.basicConfig(level=logging.DEBUG, format='%(message)s'); sys.argv = sys.argv[1:]; runpy.run_path(sys.argv[0], run_name='__main__')" `
  "C:\repo\opengeh-python-packages\source\geh_common\src\geh_common\covernator_streamlit\server.py" `
  -g `
  -o "$desktop\covernator_out" `
  -p "$projectFolder"
