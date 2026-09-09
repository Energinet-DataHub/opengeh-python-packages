from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class EmailSenderSettings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False)

    sendgrid_api_key: SecretStr = Field(init=False)
    email_from: str = Field(init=False, validation_alias="ALERT_EMAIL_FROM")
    email_to: str = Field(init=False, validation_alias="ALERT_EMAIL_TO")
