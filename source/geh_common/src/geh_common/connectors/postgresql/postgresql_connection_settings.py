from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class PostgreSqlConnectionSettings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False)

    postgresql_host: str = Field(init=False)
    postgresql_port: int = Field(default=5432, init=False)
    database_name: str = Field(init=False)
    username: str = Field(init=False)
    tenant_id: str = Field(init=False)
    spn_app_id: str = Field(init=False)
    spn_app_secret: str = Field(init=False, repr=False)
