from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SqlConnectionSettings(BaseSettings):
    model_config = SettingsConfigDict(case_sensitive=False)

    sql_server: str = Field(init=False)
    database_name: str = Field(init=False)
    tenant_id: str = Field(init=False)
    spn_app_id: str = Field(init=False)
    spn_app_secret: str = Field(init=False, repr=False)
