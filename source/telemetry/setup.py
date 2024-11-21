from setuptools import setup, find_packages


setup(
    name="opengeh-telemetry",
    version="2.1.1",
    description="Shared telemetry logging and tracing for OpenGEH",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "azure-monitor-opentelemetry==1.6.0",
        "azure-core==1.30.0",
    ],
)
