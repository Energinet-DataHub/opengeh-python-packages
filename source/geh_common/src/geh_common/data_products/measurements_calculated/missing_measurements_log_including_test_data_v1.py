import pyspark.sql.types as t

nullable = True

database_name = "measurements_calculated"

view_name = "missing_measurements_log_including_test_data_v1"

schema = t.StructType(
    [
        #
        # ID of the orchestration that initiated the calculation job
        t.StructField("orchestration_instance_id", t.StringType(), not nullable),
        #
        # GSRN number
        t.StructField("metering_point_id", t.StringType(), not nullable),
        #
        # UTC datetime
        t.StructField("date", t.TimestampType(), not nullable),
        #
        # Flag whether the data is from a test run
        t.StructField("is_test_data", t.BooleanType(), not nullable),
    ]
)
