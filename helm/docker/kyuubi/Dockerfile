FROM apache/kyuubi:master-snapshot

RUN cd /opt/kyuubi/externals/spark-3.5.5-bin-hadoop3/jars \
 && curl -LO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.2/iceberg-aws-bundle-1.9.2.jar -k \
 && curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -k \
 && curl -LO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.780/aws-java-sdk-core-1.12.780.jar -k \
 && curl -LO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-dynamodb/1.12.780/aws-java-sdk-dynamodb-1.12.780.jar -k \
 && curl -LO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-s3/1.12.780/aws-java-sdk-s3-1.12.780.jar -k
