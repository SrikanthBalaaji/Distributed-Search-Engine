"""
NODE 2 - create the Iceberg namespace + tables on HDFS (idempotent).

    bash vm2_storage/init_tables.sh
Other jobs also call ensure_tables(), so running this first is optional but it is a
good smoke test of HDFS + Spark + Iceberg working together.
"""
from dse import config as C
from dse.sparkutil import banner, get_spark
from dse.tables import current_snapshot_id, ensure_tables


def main():
    spark = get_spark("dse-init-tables")
    ensure_tables(spark)
    banner("NODE 2 :: ICEBERG TABLES", catalog=C.CATALOG, namespace=C.NAMESPACE,
           warehouse=spark.conf.get("spark.sql.catalog.%s.warehouse" % C.CATALOG))
    for name in C.ALL_TABLES:
        t = C.table(name)
        n = spark.table(t).count()
        print("  %-45s rows=%-7d snapshot=%s" % (t, n, current_snapshot_id(spark, name)))
    print("\nschema of pages:")
    spark.table(C.table(C.PAGES)).printSchema()
    spark.stop()


if __name__ == "__main__":
    main()
