from setuptools import setup, find_packages


setup(
    name="opengeh-spark-sql-migrations",
    version="0.1.1",
    description="opengeh spark sql migrations package",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "ConfigArgParse==1.5.3",
        "pyspark==3.3.0",
        "azure-identity==1.11.0",
        "azure-storage-file-datalake==12.9.1",
        "azure-storage-blob==12.14.1",
        "databricks-cli==0.17.3",
        "delta-spark==2.3.0",
        "dependency_injector==4.41.0",
        "azure-monitor-opentelemetry==1.0.0",
    ],
)
