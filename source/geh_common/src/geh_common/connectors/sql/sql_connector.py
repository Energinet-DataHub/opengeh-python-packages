import time

from azure.identity import ClientSecretCredential
from pyspark.sql import DataFrame, SparkSession

from .sql_connection_settings import SqlConnectionSettings

_TOKEN_CACHE: dict[tuple[str, str], tuple[str, int]] = {}


def _get_cached_access_token(settings: SqlConnectionSettings) -> str:
    key = (settings.tenant_id, settings.spn_app_id)
    cached = _TOKEN_CACHE.get(key)

    if cached is None or time.time() > (cached[1] - 60):
        credential = ClientSecretCredential(settings.tenant_id, settings.spn_app_id, settings.spn_app_secret)
        access_token = credential.get_token("https://database.windows.net/.default")
        _TOKEN_CACHE[key] = (access_token.token, int(access_token.expires_on))

    return _TOKEN_CACHE[key][0]


def _get_connection() -> tuple[str, dict[str, str]]:
    settings = SqlConnectionSettings()
    connection_properties = {
        "accessToken": _get_cached_access_token(settings),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
    jdbc_url = (
        f"jdbc:sqlserver://{settings.sql_server}.database.windows.net:1433;"
        f"database={settings.database_name};encrypt=true;trustServerCertificate=false;"
        "hostNameInCertificate=*.database.windows.net;loginTimeout=30"
    )
    return jdbc_url, connection_properties


def read_table(spark: SparkSession, schema_name: str, table_name: str) -> DataFrame:
    jdbc_url, connection_properties = _get_connection()
    return spark.read.jdbc(
        url=jdbc_url,
        table=f"[{schema_name}].[{table_name}]",
        properties=connection_properties,
    )


def append_to_table(dataframe: DataFrame, schema_name: str, table_name: str) -> None:
    jdbc_url, connection_properties = _get_connection()
    dataframe.write.mode("append").jdbc(
        url=jdbc_url,
        table=f"[{schema_name}].[{table_name}]",
        properties=connection_properties,
    )
