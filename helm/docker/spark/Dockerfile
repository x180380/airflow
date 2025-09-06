

FROM bitnami/spark:3.4.3
USER root
RUN install_packages curl

RUN cd /opt/bitnami/spark/jars \
 && curl -LO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.9.2/iceberg-spark-runtime-3.4_2.12-1.9.2.jar -k \
 && curl -LO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.2/iceberg-aws-bundle-1.9.2.jar -k \
 && curl -LO https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.4_2.12/0.104.3/nessie-spark-extensions-3.4_2.12-0.104.3.jar -k \
 && curl -LO https://repo1.maven.org/maven2/org/opensearch/client/opensearch-spark-30_2.12/1.3.0/opensearch-spark-30_2.12-1.3.0.jar -k \
 && curl -LO https://repo1.maven.org/maven2/com/starrocks/starrocks-spark-connector-3.4_2.12/1.1.3/starrocks-spark-connector-3.4_2.12-1.1.3.jar -k \
 && curl -LO https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/9.2.0/mysql-connector-j-9.2.0.jar -k \
 && curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -k \
 && curl -LO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.780/aws-java-sdk-core-1.12.780.jar -k \
 && curl -LO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/1.12.780/aws-java-sdk-dynamodb-1.12.780.jar -k \
 && curl -LO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.780/aws-java-sdk-s3-1.12.780.jar -k \
 && curl -LO https://repo1.maven.org/maven2/com/starrocks/starrocks-spark-connector-3.4_2.12/1.1.3/starrocks-spark-connector-3.4_2.12-1.1.3.jar -k