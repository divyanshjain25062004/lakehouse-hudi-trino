#!/usr/bin/env bash
set -euo pipefail

JAR_DIR="$(cd "$(dirname "$0")/.." && pwd)/jars"
mkdir -p "$JAR_DIR"

# JAR versions aligned with Spark 3.3 and Hive 3.1.
POSTGRES_VERSION="42.7.3"
HADOOP_AWS_VERSION="3.3.4"
AWS_SDK_VERSION="1.12.262"
HUDI_VERSION="0.14.0"

download() {
  local url="$1"
  local destination="$2"

  echo "→ $destination"
  curl -fSL "$url" -o "$destination"
}

download \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.jar" \
  "$JAR_DIR/postgresql-${POSTGRES_VERSION}.jar"

download \
  "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" \
  "$JAR_DIR/hadoop-aws-${HADOOP_AWS_VERSION}.jar"

download \
  "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" \
  "$JAR_DIR/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

download \
  "https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.3-bundle_2.12/${HUDI_VERSION}/hudi-spark3.3-bundle_2.12-${HUDI_VERSION}.jar" \
  "$JAR_DIR/hudi-spark3.3-bundle_2.12-${HUDI_VERSION}.jar"

download \
  "https://repo1.maven.org/maven2/org/apache/hudi/hudi-hadoop-mr-bundle/${HUDI_VERSION}/hudi-hadoop-mr-bundle-${HUDI_VERSION}.jar" \
  "$JAR_DIR/hudi-hadoop-mr-bundle-${HUDI_VERSION}.jar"

echo "Downloaded supporting JARs into $JAR_DIR"
