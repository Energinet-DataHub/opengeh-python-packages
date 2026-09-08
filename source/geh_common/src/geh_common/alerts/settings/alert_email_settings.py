from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class AlertEmailSettings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False)

    sendgrid_api_key: SecretStr = Field(init=False)
    alert_email_from: str = Field(init=False)
    alert_email_to: str = Field(init=False)
