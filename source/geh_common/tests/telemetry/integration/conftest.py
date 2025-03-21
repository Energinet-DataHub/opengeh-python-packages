import logging
import os
from pathlib import Path

import pytest
import yaml

from tests.telemetry.integration.integration_test_configuration import (
    IntegrationTestConfiguration,
)

from ...constants import TESTS_ROOT_DIR


@pytest.fixture(scope="session")
def telemetry_tests_path() -> str:
    """
    Returns the tests folder path for telemetry.
    Please note that this only works if the constant TESTS_ROOT_DIR in constants module is maintained.
    """
    telemetry_tests_path = TESTS_ROOT_DIR / "telemetry"
    return f"{telemetry_tests_path}"


@pytest.fixture(scope="session")
def integration_test_configuration(
    telemetry_tests_path: str,
) -> IntegrationTestConfiguration:
    """
    Load settings for integration tests either from a local YAML settings file or from environment variables.
    Proceeds even if certain Azure-related keys are not present in the settings file.
    """

    settings_file_path = Path(telemetry_tests_path) / "integration" / "integrationtest.local.settings.yml"

    def _load_settings_from_file(file_path: Path) -> dict:
        if file_path.exists():
            with file_path.open() as stream:
                return yaml.safe_load(stream)
        else:
            return {}

    def _load_settings_from_env() -> dict:
        return {
            key: os.getenv(key)
            for key in [
                "AZURE_KEYVAULT_URL",
                "AZURE_CLIENT_ID",
                "AZURE_CLIENT_SECRET",
                "AZURE_TENANT_ID",
                "AZURE_SUBSCRIPTION_ID",
            ]
            if os.getenv(key) is not None
        }

    settings = _load_settings_from_file(settings_file_path) or _load_settings_from_env()

    # Set environment variables from loaded settings
    for key, value in settings.items():
        if value is not None:
            os.environ[key] = value

    if "AZURE_KEYVAULT_URL" in settings:
        return IntegrationTestConfiguration(azure_keyvault_url=settings["AZURE_KEYVAULT_URL"])

    logging.error(
        f"Integration test configuration could not be loaded from {settings_file_path} or environment variables."
    )
    raise Exception(
        "Failed to load integration test settings. Ensure that the Azure Key Vault URL is provided in the settings file or as an environment variable."
    )
