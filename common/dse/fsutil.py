"""Hadoop FileSystem helpers that go through the Spark driver's JVM, so they work for
hdfs:// (cluster) and file:// (local test mode) without needing the `hdfs` CLI."""


def _fs_and_path(spark, uri):
    jvm = spark.sparkContext._jvm
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    return path.getFileSystem(conf), path


def exists(spark, uri):
    fs, p = _fs_and_path(spark, uri)
    return bool(fs.exists(p))


def mkdirs(spark, uri):
    fs, p = _fs_and_path(spark, uri)
    return bool(fs.mkdirs(p))


def delete(spark, uri, recursive=True):
    fs, p = _fs_and_path(spark, uri)
    return bool(fs.delete(p, recursive))


def rename(spark, src, dst):
    fs, p = _fs_and_path(spark, src)
    _, q = _fs_and_path(spark, dst)
    return bool(fs.rename(p, q))


def list_names(spark, uri):
    """Names of the direct children of a directory ([] if it doesn't exist)."""
    fs, p = _fs_and_path(spark, uri)
    if not fs.exists(p):
        return []
    return [st.getPath().getName() for st in fs.listStatus(p)]


def list_files(spark, uri):
    """[(relative_path, size_bytes)] for every file below uri (recursive)."""
    fs, p = _fs_and_path(spark, uri)
    base = fs.makeQualified(p).toString().rstrip("/") + "/"
    out = []
    it = fs.listFiles(p, True)
    while it.hasNext():
        st = it.next()
        full = st.getPath().toString()
        out.append((full[len(base):] if full.startswith(base) else st.getPath().getName(), int(st.getLen())))
    return sorted(out)


def write_text(spark, uri, text):
    fs, p = _fs_and_path(spark, uri)
    out = fs.create(p, True)
    try:
        out.write(bytearray(text.encode("utf-8")))
    finally:
        out.close()
