from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("RiskFlowSparkImageVerification")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
if spark.range(1).count() != 1:
    raise RuntimeError("Spark runtime verification failed")
spark.stop()
