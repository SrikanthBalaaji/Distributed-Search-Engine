"""SparkSession creation shared by all jobs."""
from dse import config as C


def get_spark(app_name):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    if not spark.conf.get("spark.sql.catalog.%s" % C.CATALOG, None):
        raise SystemExit(
            "Iceberg catalog '%s' is not configured. Submit jobs with scripts/submit.sh "
            "(it passes the generated spark-defaults.conf)." % C.CATALOG
        )
    return spark


def banner(title, **kv):
    line = "=" * 72
    print("\n" + line)
    print(" " + title)
    for k, v in kv.items():
        print("   %-22s %s" % (k, v))
    print(line, flush=True)
