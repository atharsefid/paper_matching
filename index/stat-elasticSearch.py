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


client = Elasticsearch(host="0.0.0.0", timeout = 40, port = 9208)


#client.indices.delete(index='csx', ignore=[400, 404])

#cmd  = "curl -X PUT \"http://0.0.0.0:9202/citations_wos/_settings\" -H \'Content-Type: application/json\' -d\'{\"index\": {\"blocks\": {\"read_only_allow_delete\": \"false\"}}}\'"

#res = client.get(index="wos_citations2017", doc_type='citation', id = 1)
#print(res['_source'])

#s = Search(using=client, index="wos_citations2017").query("match", id='180306000')
#response = s.execute()
#for hit in response:
#    print(hit.meta.score, hit['id'], hit['citedTitle']) 


cmd = "curl 0.0.0.0:9208/wos_papers2017/_stats"
#cmd = "curl -XGET localhost:9204"
proc = subprocess.Popen(cmd, shell = True, universal_newlines=True, stdout=subprocess.PIPE)
out, err = proc.communicate(timeout = 15)
print(out)
print(err) 

