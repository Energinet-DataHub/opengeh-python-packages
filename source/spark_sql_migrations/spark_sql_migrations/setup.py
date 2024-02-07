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
        "pyspark==3.3.2",
        "delta-spark==2.3.0",
        "dependency_injector==4.41.0",
    ],
)
