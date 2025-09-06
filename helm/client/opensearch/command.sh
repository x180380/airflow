curl -u admin:admin -k -X GET "https://localhost:9200/_cluster/health?pretty"
curl -u admin:admin -k -X GET "https://localhost:9200/_cat/indices?v"
curl -u admin:admin -k -X PUT "https://localhost:9200/my-index"
curl -u admin:admin -k -X DELETE "https://localhost:9200/my-index"
curl -u admin:admin -k -X POST "https://localhost:9200/my-index/_doc/1" \
-H 'Content-Type: application/json' \
-d '{
  "title": "OpenSearch Example",
  "content": "This is a sample document"
}'
curl -u admin:admin -k -X GET "https://localhost:9200/my-index/_doc/1"
curl -u admin:admin -k -X GET "https://localhost:9200/my-index/_search" \
-H 'Content-Type: application/json' \
-d '{
  "query": {
    "match": {
      "content": "sample"
    }
  }
}'
curl -u admin:admin -k -X GET "https://localhost:9200/my-index/_search?q=*:*&pretty"
curl -u admin:admin -k -X DELETE "https://localhost:9200/my-index/_doc/1"
curl -u admin:admin -k -X POST "https://localhost:9200/my-index/_bulk" \
-H 'Content-Type: application/json' \
-d '
{ "index": { "_id": 1 } }
{ "title": "First document", "content": "Hello OpenSearch" }
{ "index": { "_id": 2 } }
{ "title": "Second document", "content": "OpenSearch is great" }
'
