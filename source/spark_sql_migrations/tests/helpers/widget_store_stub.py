from package.schemas.schema_constants.schema_migration_schema_constants import (
    SchemaMigrationSchema,
)


class WidgetStoreStub:
    widgets: dict = {
        "db_folder": "",
        "table_prefix": "",
        "schema_migration_container": "schema-migration",
        "bronze_container": "bronze",
        "silver_container": "silver",
        "gold_container": "gold",
        "eloverblik_container": "eloverblik",
        "wholesale_container": "wholesale",
        "bronze_charges_table": "charges",
        "bronze_charge_links_table": "charge_links",
        "bronze_time_series_table": "time_series",
        "bronze_time_series_quarantined_table": "time_series_quarantined",
        "bronze_metering_point_table": "metering_points",
        "bronze_metering_point_quarantined_table": "metering_points_quarantined",
        "silver_time_series_table": "time_series",
        "silver_time_series_masterdata_table": "time_series_masterdata",
        "silver_metering_point_connection_register_table": "metering_points_connection_register",
        "silver_metering_point_connection_register_quarantined_table": "metering_points_connection_register_quarantined",
        "silver_metering_point_btd_info_table": "metering_points_business_transaction_dossiers_info",
        "silver_metering_point_btd_steps_table": "metering_points_business_transaction_dossiers_steps",
        "silver_time_series_quarantined_table": "time_series_quarantined",
        "gold_metering_point_table": "metering_points",
        "gold_time_series_table": "time_series_points",
        "eloverblik_time_series_table": "eloverblik_time_series_points",
        "wholesale_time_series_table": "time_series_points",
        "wholesale_metering_points_table": "metering_point_periods",
        "wholesale_charge_masterdata_table": "charge_masterdata_periods",
        "wholesale_charge_price_table": "charge_price_points",
        "wholesale_charge_link_table": "charge_link_periods",
        "gold_performance_measurement_table": "performance_measurement",
        "datalake_storage_account": "datalake_storage_account",
        "datalake_shared_storage_account": "datalake_shared_storage_account",
        "schema_migration_schema": "schema_migration",
        "schema_migration_path": "tables",
        # DEPRECATED - See https://github.com/Energinet-DataHub/opengeh-migration/tree/main/docs/deprecated_code/README.md for more information
        "silver_time_series_wholesale_quarantined_table": "time_series_wholesale_quarantined",
    }

    @staticmethod
    def db_folder() -> str:
        return WidgetStoreStub.widgets["db_folder"]

    @staticmethod
    def table_prefix() -> str:
        return WidgetStoreStub.widgets["table_prefix"]

    @staticmethod
    def schema_migration_container() -> str:
        return WidgetStoreStub.widgets["schema_migration_container"]

    @staticmethod
    def bronze_container() -> str:
        return WidgetStoreStub.widgets["bronze_container"]

    @staticmethod
    def silver_container() -> str:
        return WidgetStoreStub.widgets["silver_container"]

    @staticmethod
    def gold_container() -> str:
        return WidgetStoreStub.widgets["gold_container"]

    @staticmethod
    def eloverblik_container() -> str:
        return WidgetStoreStub.widgets["eloverblik_container"]

    @staticmethod
    def wholesale_container() -> str:
        return WidgetStoreStub.widgets["wholesale_container"]

    @staticmethod
    def bronze_schema() -> str:
        db_folder = WidgetStoreStub.db_folder()
        return db_folder if db_folder != "" else WidgetStoreStub.bronze_container()

    @staticmethod
    def silver_schema() -> str:
        db_folder = WidgetStoreStub.db_folder()
        return db_folder if db_folder != "" else WidgetStoreStub.silver_container()

    @staticmethod
    def gold_schema() -> str:
        db_folder = WidgetStoreStub.db_folder()
        return db_folder if db_folder != "" else WidgetStoreStub.gold_container()

    @staticmethod
    def wholesale_schema() -> str:
        db_folder = WidgetStoreStub.db_folder()
        return db_folder if db_folder != "" else WidgetStoreStub.wholesale_container()

    @staticmethod
    def eloverblik_schema() -> str:
        db_folder = WidgetStoreStub.db_folder()
        return db_folder if db_folder != "" else WidgetStoreStub.eloverblik_container()

    @staticmethod
    def bronze_charges_table() -> str:
        return WidgetStoreStub.widgets["bronze_charges_table"]

    @staticmethod
    def bronze_charge_links_table() -> str:
        return WidgetStoreStub.widgets["bronze_charge_links_table"]

    @staticmethod
    def bronze_time_series_table() -> str:
        return WidgetStoreStub.widgets["bronze_time_series_table"]

    @staticmethod
    def bronze_time_series_quarantined_table() -> str:
        return WidgetStoreStub.widgets["bronze_time_series_quarantined_table"]

    @staticmethod
    def bronze_metering_point_table() -> str:
        return WidgetStoreStub.widgets["bronze_metering_point_table"]

    @staticmethod
    def bronze_metering_point_quarantined_table() -> str:
        return WidgetStoreStub.widgets["bronze_metering_point_quarantined_table"]

    @staticmethod
    def silver_time_series_table() -> str:
        return WidgetStoreStub.widgets["silver_time_series_table"]

    @staticmethod
    def silver_time_series_masterdata_table() -> str:
        return WidgetStoreStub.widgets["silver_time_series_masterdata_table"]

    @staticmethod
    def silver_metering_point_connection_register_table() -> str:
        return WidgetStoreStub.widgets["silver_metering_point_connection_register_table"]

    @staticmethod
    def silver_metering_point_connection_register_quarantined_table() -> str:
        return WidgetStoreStub.widgets[
            "silver_metering_point_connection_register_quarantined_table"
        ]

    @staticmethod
    def silver_metering_point_btd_info_table() -> str:
        return WidgetStoreStub.widgets["silver_metering_point_btd_info_table"]

    @staticmethod
    def silver_metering_point_btd_steps_table() -> str:
        return WidgetStoreStub.widgets["silver_metering_point_btd_steps_table"]

    @staticmethod
    def silver_time_series_quarantined_table() -> str:
        return WidgetStoreStub.widgets["silver_time_series_quarantined_table"]

    @staticmethod
    def silver_time_series_wholesale_quarantined_table() -> str:
        return WidgetStoreStub.widgets["silver_time_series_wholesale_quarantined_table"]

    @staticmethod
    def gold_metering_point_table() -> str:
        return WidgetStoreStub.widgets["gold_metering_point_table"]

    @staticmethod
    def gold_time_series_table() -> str:
        return WidgetStoreStub.widgets["gold_time_series_table"]

    @staticmethod
    def eloverblik_time_series_table() -> str:
        return WidgetStoreStub.widgets["eloverblik_time_series_table"]

    @staticmethod
    def wholesale_time_series_table() -> str:
        return WidgetStoreStub.widgets["wholesale_time_series_table"]

    @staticmethod
    def wholesale_metering_points_table() -> str:
        return WidgetStoreStub.widgets["wholesale_metering_points_table"]

    @staticmethod
    def wholesale_charge_masterdata_table() -> str:
        return WidgetStoreStub.widgets["wholesale_charge_masterdata_table"]

    @staticmethod
    def wholesale_charge_price_table() -> str:
        return WidgetStoreStub.widgets["wholesale_charge_price_table"]

    @staticmethod
    def wholesale_charge_link_table() -> str:
        return WidgetStoreStub.widgets["wholesale_charge_link_table"]

    @staticmethod
    def gold_performance_measurement_table() -> str:
        return WidgetStoreStub.widgets["gold_performance_measurement_table"]

    @staticmethod
    def datalake_storage_account() -> str:
        return WidgetStoreStub.widgets["datalake_storage_account"]

    @staticmethod
    def datalake_shared_storage_account() -> str:
        return WidgetStoreStub.widgets["datalake_shared_storage_account"]

    @staticmethod
    def schema_migration_schema() -> str:
        db_folder = WidgetStoreStub.db_folder()
        return db_folder if db_folder != "" else "schema_migration"

    @staticmethod
    def schema_migration_path() -> str:
        return WidgetStoreStub.widgets["schema_migration_path"]
