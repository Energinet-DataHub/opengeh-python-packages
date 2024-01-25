from setuptools import setup, find_packages


setup(
    name="opengeh-telemetry",
    version="0.1.1",
    description="opengeh telemetry package",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "ConfigArgParse==1.5.3",
        "pyspark==3.3.2",
        "azure-identity==1.13.0",
        "azure-storage-file-datalake==12.11.0",
        "azure-storage-blob==12.14.1",
        "databricks-cli==0.17.7",
        "delta-spark==2.3.0",
        "dependency_injector==4.41.0",
        "azure-monitor-opentelemetry==1.0.0",
    ],
)
