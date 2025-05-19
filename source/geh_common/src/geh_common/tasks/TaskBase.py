from abc import abstractmethod

from pyspark.sql import SparkSession

from geh_common.databricks import get_dbutils
from geh_common.telemetry import Logger


class TaskBase:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.dbutils = get_dbutils(spark)
        self.log = Logger(__name__)

    @abstractmethod
    def execute(self) -> None:
        pass
