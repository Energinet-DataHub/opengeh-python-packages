from setuptools import setup, find_packages


setup(
    name="opengeh-pyspark",
    version="0.0.2",
    description="Shared pyspark functions for OpenGEH",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.5.0",
        "opengeh-testcommon @ git+https://git@github.com/Energinet-DataHub/opengeh-python-packages@3.3.0#subdirectory=source/testcommon",
    ],
)
