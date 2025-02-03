from pyspark.sql.session import SparkSession
from pyspark.sql import DataFrame

def coreflex_table(saprk: SparkSession, config: dict, table_name: str):
    return spark.read.format("jdbc") \
        .option("url",
                f"jdbc:oracle:thin:@{config['coreflex']['host']}:{config['coreflex']['port']}/{config['coreflex']['service_name']}") \
        .option("dbtable", f"FLEXBO.{table_name}") \
        .option("user", config['coreflex']['username']) \
        .option("password", config['coreflex']['password']) \
        .option("driver", "oracle.jdbc.driver.OracleDriver") \
        .load()