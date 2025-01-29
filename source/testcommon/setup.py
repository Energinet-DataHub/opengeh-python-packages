from setuptools import setup, find_packages


setup(
    name="opengeh-testcommon",
    version="0.1.1",
    description="Shared testing utilities for OpenGEH Python packages",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.5.0",
        "PyYAML",
        "delta-spark",
        "databricks-sdk",
        'opengeh-pyspark @ git+https://github.com/Energinet-DataHub/opengeh-python-packages.git@pyspark_functions#subdirectory=source/pyspark_functions'
    ],
)
