from setuptools import setup, find_packages


setup(
    name="opengeh-testcommon",
    version="0.0.2",
    description="Shared testing utilities for OpenGEH Python packages",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.5.0",
        "pytest",
        "PyYAML",
        "delta-spark",
        # The pandas, numpy and PyArray dependencies are required for using
        # `assertDataFrameEqual` and `assertSchemaEqual` function from PySpark.
        "pandas>=1.0.5,<2.0.0",
        "numpy==1.26.4",
        "PyArrow>=4.0.0",
    ],
)
