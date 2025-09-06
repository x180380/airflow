
CREATE TABLE IF NOT EXISTS park_catalog.default.es_user_event (
    id int,
    data STRING
) USING org.opensearch.spark.sql
OPTIONS(
    'pushdown' 'true',
    'opensearch.resource' 'user-event', 
    'opensearch.mapping.id' 'id',
    'opensearch.nodes'  'https://opensearch-masters:9200',
    'opensearch.net.http.auth.user' 'admin',
    'opensearch.net.http.auth.pass' 'admin',
    'opensearch.net.ssl' 'true',
    'opensearch.net.ssl.cert.allow.self.signed' 'true'
);

insert into spark_catalog.default.es_user_event select * from nessie.default.user_event;