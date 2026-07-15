import pyspark.sql.types as T

nullable = True

database_name = "charges_reporting"

view_name = "charge_price_information_periods_v1"

schema = T.StructType(
    [
        T.StructField("code", T.StringType(), not nullable),
        T.StructField("type", T.StringType(), not nullable),
        T.StructField("owner", T.StringType(), not nullable),
        T.StructField("start_date", T.TimestampType(), nullable),
        T.StructField("end_date", T.TimestampType(), nullable),
        T.StructField("resolution", T.StringType(), not nullable),
        T.StructField("tax_indicator", T.BooleanType(), not nullable),
        T.StructField("pricing_category", T.StringType(), not nullable),
        T.StructField("name", T.StringType(), nullable),
        T.StructField("description", T.StringType(), nullable),
        T.StructField("vat_classification", T.StringType(), nullable),
        T.StructField("transparent_invoicing", T.BooleanType(), nullable),
    ]
)
