airflow connections add jdbc --conn-type jdbc --conn-host 'jdbc:postgresql://postgres:5432/airflow' --conn-login airflow --conn-password airflow --conn-extra '{"driver_path": "/opt/airflow/plugins/huaweicloud-dli-jdbc-2.0.0.jar", "driver_class": "org.postgresql.Driver"}'
airflow connections add obs --conn-type aws --conn-password 'YHOvX42IlwL5OB5UVHRlrW0ZEpWGh1KikSx6azDF' --conn-login='HGMO3AFFUJCEESUQKDSN' --conn-extra '{"service_config": {"s3": {"endpoint_url": "https://obs.ae-ad-1.vb.g42cloud.com"}}}'
airflow connections add kafka --conn-type kafka --conn-extra '{"bootstrap.servers":"kafka-controller-headless:9094","security.protocol":"SASL_PLAINTEXT","sasl.mechanism":"SCRAM-SHA-256","sasl.username":"user1","sasl.password":"user123","group.id":"airflow"}'
airflow connections add kyuubi --conn-type hiveserver2 --conn-host 'kyuubi-thrift-binary' --conn-schema='nessie/default' --conn-port='10009' 

