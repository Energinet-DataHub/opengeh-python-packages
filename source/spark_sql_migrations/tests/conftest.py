"""
By having a conftest.py in this directory, we can define fixtures to be
shared among all tests in the test suite.
"""

import os


def pytest_runtest_setup():
    """
    This function is called before each test function is executed.
    """
    os.environ["DATALAKE_STORAGE_ACCOUNT"] = "storage_account"
    os.environ["DATALAKE_SHARED_STORAGE_ACCOUNT"] = "shared_storage_account"
