import pytest


@pytest.fixture(scope="session")
def clear_azure_env():
    import os

    yield
    for key in os.environ.keys():
        if key.startswith("AZURE_"):
            os.environ.pop(key)
