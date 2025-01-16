from setuptools import setup, find_packages


setup(
    name="opengeh-telemetry",
    version="2.2.1",
    description="Shared telemetry logging and tracing for OpenGEH",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "azure-monitor-opentelemetry>=1.6.0,<2.0.0",
        "azure-core>=1.30.0,<2.0.0",
        "azure-identity>=1.12.0,<2.0.0",
        "azure-keyvault-secrets>=4.7.0,<5.0.0",
        "azure-monitor-query>=1.2.0,<2.0.0",
        "pydantic==2.10.4",
        "pydantic-settings==2.7.1",
    ],
)
