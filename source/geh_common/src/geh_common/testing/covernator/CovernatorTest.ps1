$env:PYTHONLOGLEVEL="DEBUG"

$env:PYTHONPATH="C:\repo\opengeh-python-packages\source\geh_common\src;$env:PYTHONPATH"

$desktop = [Environment]::GetFolderPath("Desktop")

python -c "import logging, runpy, sys; logging.basicConfig(level=logging.DEBUG, format='%(message)s'); sys.argv = sys.argv[1:]; runpy.run_path(sys.argv[0], run_name='__main__')" `
  C:\repo\opengeh-python-packages\source\geh_common\src\geh_common\covernator_streamlit\server.py `
  -g `
  -o "$desktop\covernator_out" `
  -p C:\repo\opengeh-measurements\source\geh_calculated_measurements\tests

