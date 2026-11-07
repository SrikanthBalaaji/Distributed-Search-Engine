"""Minimal WebHDFS client (standard library only).

Node 4 uses it to pull /serving/vN/ from HDFS without running (or even installing)
Hadoop. Node 1 can use it as a fallback uploader when the `hdfs` CLI is missing.

CLI:
    python3 -m dse.webhdfs ls  /serving
    python3 -m dse.webhdfs get /serving/v1/manifest.json ./manifest.json
    python3 -m dse.webhdfs put ./pages.jsonl /dse/raw/phase0/pages.jsonl
"""
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


class WebHDFSError(RuntimeError):
    pass


class WebHDFS:
    def __init__(self, base_url=None, user=None, timeout=60):
        self.base = (base_url or os.environ.get("WEBHDFS_URL") or "http://dse-node2:9870").rstrip("/")
        self.user = user or os.environ.get("HADOOP_USER_NAME") or os.environ.get("HDFS_USER") or "hadoop"
        self.timeout = timeout

    def _url(self, path, op, **params):
        q = {"op": op, "user.name": self.user}
        q.update({k: v for k, v in params.items() if v is not None})
        return "%s/webhdfs/v1%s?%s" % (self.base, urllib.parse.quote(path), urllib.parse.urlencode(q))

    def _json(self, path, op, method="GET", **params):
        req = urllib.request.Request(self._url(path, op, **params), method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                body = r.read()
                return json.loads(body) if body else {}
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise FileNotFoundError(path)
            raise WebHDFSError("%s %s -> HTTP %s %s" % (op, path, e.code, e.read()[:300]))

    # ---- read ops -------------------------------------------------------
    def status(self, path):
        try:
            return self._json(path, "GETFILESTATUS")["FileStatus"]
        except FileNotFoundError:
            return None

    def exists(self, path):
        return self.status(path) is not None

    def list(self, path):
        """[{'pathSuffix','type','length',...}]"""
        return self._json(path, "LISTSTATUS")["FileStatuses"]["FileStatus"]

    def read(self, path):
        """Whole file as bytes (follows the NameNode -> DataNode redirect)."""
        try:
            with urllib.request.urlopen(self._url(path, "OPEN"), timeout=self.timeout) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise FileNotFoundError(path)
            raise WebHDFSError("OPEN %s -> HTTP %s" % (path, e.code))

    def download(self, path, local_path, chunk=1 << 20):
        os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
        with urllib.request.urlopen(self._url(path, "OPEN"), timeout=self.timeout) as r, \
                open(local_path, "wb") as f:
            while True:
                b = r.read(chunk)
                if not b:
                    break
                f.write(b)
        return os.path.getsize(local_path)

    # ---- write ops ------------------------------------------------------
    def mkdirs(self, path):
        return self._json(path, "MKDIRS", method="PUT").get("boolean", False)

    def upload(self, local_path, path, overwrite=True, replication=None):
        """Two-step WebHDFS CREATE: NameNode answers 307 with the DataNode location."""
        url = self._url(path, "CREATE", overwrite=str(overwrite).lower(), replication=replication)
        req = urllib.request.Request(url, method="PUT")
        location = None
        try:
            urllib.request.urlopen(req, timeout=self.timeout)
        except urllib.error.HTTPError as e:
            if e.code in (301, 302, 307):
                location = e.headers.get("Location")
            else:
                raise WebHDFSError("CREATE %s -> HTTP %s %s" % (path, e.code, e.read()[:300]))
        if not location:
            raise WebHDFSError("CREATE %s: NameNode did not redirect" % path)
        with open(local_path, "rb") as f:
            data = f.read()
        req2 = urllib.request.Request(location, data=data, method="PUT",
                                      headers={"Content-Type": "application/octet-stream"})
        with urllib.request.urlopen(req2, timeout=max(self.timeout, 300)) as r:
            if r.status not in (200, 201):
                raise WebHDFSError("CREATE %s data upload -> HTTP %s" % (path, r.status))
        return True


def _main(argv):
    if len(argv) < 2:
        print(__doc__)
        return 1
    c = WebHDFS()
    cmd = argv[1]
    if cmd == "ls":
        for s in c.list(argv[2]):
            print("%-4s %12s  %s" % (s["type"][0], s["length"], s["pathSuffix"]))
    elif cmd == "get":
        print(c.download(argv[2], argv[3]), "bytes")
    elif cmd == "put":
        c.mkdirs(os.path.dirname(argv[3]) or "/")
        c.upload(argv[2], argv[3], replication=os.environ.get("HDFS_REPLICATION"))
        print("uploaded", argv[2], "->", argv[3])
    else:
        print(__doc__)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(_main(sys.argv))
