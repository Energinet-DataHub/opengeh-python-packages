from setuptools import setup, find_packages


setup(
    name="common-python-packages",
    version="0.1",
    description="Common Python packages",
    long_description="",
    long_description_content_type="text/markdown",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "ConfigArgParse==1.5.3",
    ],
)
