#!/usr/bin/env bash
# ---------------------------------------------------------------------
# Install the Iceberg Spark runtime jar (same steps as the Apache Iceberg
# hands-on). Run on Nodes 1, 2 and 3 (every node that runs a Spark driver).
#   bash scripts/install_iceberg.sh
# ---------------------------------------------------------------------
set -euo pipefail
ICEBERG_VERSION="${ICEBERG_VERSION:-1.8.1}"
JAR_NAME="iceberg-spark-runtime-3.3_2.12-${ICEBERG_VERSION}.jar"
BASE_URL="https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/${ICEBERG_VERSION}"

sudo mkdir -p /opt/iceberg
sudo chown -R "$USER":"$USER" /opt/iceberg
cd /opt/iceberg

if [ -f "$JAR_NAME" ]; then
  echo "[iceberg] $JAR_NAME already present"
else
  wget -q "${BASE_URL}/${JAR_NAME}"
fi
wget -q -O "${JAR_NAME}.sha1" "${BASE_URL}/${JAR_NAME}.sha1"
EXPECTED_SHA1="$(tr -d '[:space:]' < "${JAR_NAME}.sha1")"
ACTUAL_SHA1="$(sha1sum "${JAR_NAME}" | awk '{print $1}')"
rm -f "${JAR_NAME}.sha1"
if [ "$EXPECTED_SHA1" = "$ACTUAL_SHA1" ]; then
  echo "[iceberg] SHA-1 verification: OK"
else
  echo "[iceberg] SHA-1 verification: FAILED" >&2
  rm -f "$JAR_NAME"; exit 1
fi
ls -lh /opt/iceberg/
