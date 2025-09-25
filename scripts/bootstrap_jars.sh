#!/usr/bin/env bash
set -euo pipefail

JAR_DIR="$(cd "$(dirname "$0")/.." && pwd)/jars"
mkdir -p "$JAR_DIR"

# JAR versions aligned with Spark 3.3 and Hive 3.1.
POSTGRES_VERSION="42.7.3"
HADOOP_AWS_VERSION="3.3.4"
AWS_SDK_VERSION="1.12.262"

curl -L "https://repo1.maven.org/maven2/org/postgresql/postgresql/${POSTGRES_VERSION}/postgresql-${POSTGRES_VERSION}.jar" -o "$JAR_DIR/postgresql-${POSTGRES_VERSION}.jar"
curl -L "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" -o "$JAR_DIR/hadoop-aws-${HADOOP_AWS_VERSION}.jar"
curl -L "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar" -o "$JAR_DIR/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

echo "Downloaded supporting JARs into $JAR_DIR"
