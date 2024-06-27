class Catalog:
    default_hive_metastore = "hive_metastore"
    test_hive_metastore = "spark_catalog"
    hive_metastores = [default_hive_metastore, test_hive_metastore]
