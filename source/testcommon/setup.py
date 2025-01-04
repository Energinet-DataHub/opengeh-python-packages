from setuptools import setup, find_packages


setup(
    name="opengeh-testcommon",
    version="0.0.4",
    description="Shared testing utilities for OpenGEH Python packages",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.5.0",
        "PyYAML",
        "delta-spark",
    ],
)
