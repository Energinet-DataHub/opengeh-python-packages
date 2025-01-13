from setuptools import setup, find_packages


setup(
    name="opengeh-pyspark",
    version="2.2.1",
    description="Shared pyspark functions for OpenGEH",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.5.0"
    ],
)