from dataclasses import dataclass
import os
from pydantic import Field, BaseModel, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict
import argparse
from uuid import UUID


# To test Environment variables
os.environ['CLOUD_ROLE_NAME'] = 'dbr-electrical-heating'
os.environ['APPLICATIONINSIGHTS_CONNECTION_STRING'] = 'test_string'
os.environ['SUBSYSTEM'] = 'electrical-heating'
# os.environ['LOGGING_EXTRAS'] = '{"default": "value1"}'

# ---------------------------------------------------
# From CLI

class CLIArgs(BaseModel):
    orchestration_instance_id: UUID = Field(..., description="The UUID of the orchestration instance")

    @classmethod
    def parse_from_cli(cls):
        # Initialize argparse
        parser = argparse.ArgumentParser(description="Process the orchestration instance UUID.")
        parser.add_argument('--orchestration_instance_id', type=str, required=True, help='Your name')

        # Parse the arguments
        args = parser.parse_args()

        try:
            # Use Pydantic to validate and parse the UUID input
            return cls(orchestration_instance_id=args.orchestration_instance_id)

        except ValidationError as e:
            print(f"Validation failed:\n{e.json()}")



# ---------------------------------------------------
# From Environment
class ENVArgs(BaseSettings):
    cloud_role_name: str = Field(validation_alias="CLOUD_ROLE_NAME")
    applicationinsights_connection_string: str = Field(validation_alias="APPLICATIONINSIGHTS_CONNECTION_STRING")
    subsystem: str = Field(validation_alias="SUBSYSTEM")

class LoggingSettings(BaseSettings):

    """
    LoggingSettings class .....
    """
    cloud_role_name: str
    applicationinsights_connection_string: str
    subsystem: str
    logging_extras: dict

    @classmethod
    def load(cls):
        # Load CLI args
        cli_args = CLIArgs.parse_from_cli()

        # Load environment args
        env_args = ENVArgs()

        # Combine and return LoggingSettings
        return cls(
            cloud_role_name=env_args.cloud_role_name,
            applicationinsights_connection_string=env_args.applicationinsights_connection_string,
            subsystem=env_args.subsystem,
            logging_extras = {"OrchestrationInstanceId": cli_args.orchestration_instance_id}
        )

logging_settings = LoggingSettings.load()
print(logging_settings)
print(logging_settings.model_dump_json(indent=4))






# class LoggingSettings(BaseSettings):
#     cloud_role_name: str = Field(validation_alias="CLOUD_ROLE_NAME")
#     applicationinsights_connection_string: str = Field(validation_alias="APPLICATIONINSIGHTS_CONNECTION_STRING")
#     subsystem: str = Field(validation_alias="SUBSYSTEM")
#     logging_extras: dict = {"OrchestrationInstanceId": cli_args.orchestration_instance_id}
#     # logging_extras: dict = Field(logging_extras)
#
#     class Config:
#         env_file = ".env"  # Optional: Load values from a .env file if present


# settings = ENVArgs()
#
#
# print(settings.cloud_role_name)
