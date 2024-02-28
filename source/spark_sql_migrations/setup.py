from setuptools import setup, find_packages


setup(
    name="opengeh-spark-sql-migrations",
    version="0.1.2",
    description="opengeh spark sql migrations package",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "ConfigArgParse==1.5.3",
        "pyspark==3.5.1",
        "delta-spark==3.1.0",
        "dependency_injector==4.41.0",
    ],
)
