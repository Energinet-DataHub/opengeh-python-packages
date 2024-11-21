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
        "pyspark>=3.5.3,<4.0",
        "delta-spark>=3.1,<4.0",
        "dependency_injector>=4.43.0,<5.0",
    ],
)
