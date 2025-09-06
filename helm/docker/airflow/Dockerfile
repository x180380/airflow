# Start from the official Airflow image
FROM apache/airflow:3.0.5

RUN pip install apache-airflow-providers-jdbc --use-deprecated=legacy-resolver \
&& pip install apache-airflow-providers-common-sql \
&& pip install jaydebeapi \
&& pip install dag-factory \
&& pip install apache-airflow-providers-apache-hive \
&& pip install 'apache-airflow[amazon]' --use-deprecated=legacy-resolver \
&& pip install apache-airflow-providers-apache-kafka \
&& pip install apache-airflow-providers-common-compat \
&& pip install apache-airflow-providers-common-io \
&& pip install apache-airflow-providers-apache-spark \
&& pip install apache-airflow-providers-elasticsearch \
&& pip install apache-airflow-providers-opensearch \
&& pip install apache-airflow-providers-http \
&& pip install apache-airflow-providers-postgres \
&& pip install apache-airflow-providers-sftp \
&& pip install apache-airflow-providers-mysql \
&& pip install apache-airflow-providers-apache-iceberg \
&& pip install apache-airflow-providers-oracle \
&& pip install huaweicloud-python-sdk-dis \
&& pip install apache-airflow-providers-common-messaging \
&& pip install "pyiceberg[s3fs]"

USER root

RUN mkdir -p /opt/airflow/plugins \
    && curl https://repo1.maven.org/maven2/org/apache/kyuubi/kyuubi-hive-jdbc-shaded/1.10.1/kyuubi-hive-jdbc-shaded-1.10.1.jar -o /opt/airflow/plugins/kyuubi-hive-jdbc-shaded-1.10.1.jar -k \
    && curl https://repo1.maven.org/maven2/com/huawei/dli/huaweicloud-dli-jdbc/2.1.3/huaweicloud-dli-jdbc-2.1.3.jar -o /opt/airflow/plugins/huaweicloud-dli-jdbc-2.1.3.jar -k \
    && curl https://dlcdn.apache.org/kyuubi/kyuubi-1.10.1/apache-kyuubi-1.10.1-bin.tgz -o /tmp/apache-kyuubi-1.10.1-bin.tgz -k \
    && tar xzf /tmp/apache-kyuubi-1.10.1-bin.tgz -C /opt/ \
    && rm -rf  /tmp/apache-kyuubi-1.10.1-bin.tgz
RUN  apt-get update \
     && apt-get install openjdk-17-jdk -y \
     && apt-get clean
COPY plugins/ /opt/airflow/plugins
RUN chown -R airflow /opt/airflow/plugins
COPY dags/ /opt/airflow/dags
RUN chown -R airflow /opt/airflow/dags
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_PATH_IN_EXTRA=true
ENV AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_CLASS_IN_EXTRA=true
ENV PATH="/opt/apache-kyuubi-1.10.1-bin/bin:${PATH}"
