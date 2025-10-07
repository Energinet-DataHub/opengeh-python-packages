Clear-Host

# Force DEBUG logging and set Python search path
$env:PYTHONLOGLEVEL="DEBUG"
$env:PYTHONPATH="C:\repo\opengeh-python-packages\source\geh_common\src;$env:PYTHONPATH"

# Resolve Desktop folder dynamically
$desktop = [Environment]::GetFolderPath("Desktop")

# Run the script with logging forced to DEBUG
python -c "import logging, runpy, sys; logging.basicConfig(level=logging.DEBUG, format='%(message)s'); sys.argv = sys.argv[1:]; runpy.run_path(sys.argv[0], run_name='__main__')" `
  C:\repo\opengeh-python-packages\source\geh_common\src\geh_common\covernator_streamlit\server.py `
  -g `
  -o "$desktop\covernator_out" `
  -p C:\repo\opengeh-wholesale\source\
#  -p C:\repo\opengeh-measurements\source\  