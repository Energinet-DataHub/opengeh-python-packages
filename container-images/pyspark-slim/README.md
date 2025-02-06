# PySpark configuration

The version of pyspark is configured in build-config.yml

Please check the download link in build-config.yml for available versions.

## Image tag

Image tag is produced by combining text `pyspark-slim` with variable `spark_version`
and variable `image_postfix` from `build-config.yml`.

Example: `pyspark-slim-3.5.1-2`