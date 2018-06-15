from elasticsearch import Elasticsearch
import requests
import mysql.connector
import json
import pymysql
import traceback
import sys
import subprocess
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MultiMatch, Match, Q


#client = Elasticsearch(host="0.0.0.0", timeout = 40, port = 9201)
#client.indices.delete(index='medline2017', ignore=[400, 404])

curl_put = "curl -X PUT \"localhost:9201/_settings\" -H 'Content-Type: application/json' -d"
#cmd = "'{\"index\":{\"blocks\": {\"read_only_allow_delete\": \"false\"}}}'"
#cmd = "'{ \"transient\": {    \"cluster.routing.allocation.disk.watermark.low\": \"100gb\",    \"cluster.routing.allocation.disk.watermark.high\": \"50gb\",    \"cluster.routing.allocation.disk.watermark.flood_stage\": \"20gb\",    \"cluster.info.update.interval\":\"1m\"  }}'"

#cmd = "'{\"index.blocks.read_only_allow_delete\": null}'"

cmd = "curl 0.0.0.0:9201/medline2017/_stats"
proc = subprocess.Popen( cmd, shell = True, universal_newlines=True, stdout=subprocess.PIPE)
out, err = proc.communicate(timeout = 15)
print(out)
print(err) 


