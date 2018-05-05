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


client = Elasticsearch(host="0.0.0.0", timeout = 40, port = 9200)


#client.indices.delete(index='', ignore=[400, 404])
#client.indices.delete(index='csx', ignore=[400, 404])

# to add more filters to your search just add another query 
# "match" indicates approximate match
# secod parameter of query is the field and its value that you want to search on

#s = Search(using=client, index="alaki") \
#    .query("match", id='1')
#    .query("match", year="2016") \
#    .query("match", authors="Lin Zhang" )
#response = s.execute()
#for hit in response:
#    print(hit.meta.score, hit['id']) #, hit['abstract'], hit['keywords'])


#res = client.get(index="wos_papers", doc_type='paper', id=10040)
#print(res['_source'])
# citations_wos : 176M
# wos_citations: 685M 285G
cmd = "curl 0.0.0.0:9202/citations_wos/_stats"
#cmd = "curl -XGET localhost:9201/citations_wos/_stats"
proc = subprocess.Popen(cmd, shell = True, universal_newlines=True, stdout=subprocess.PIPE)
out, err = proc.communicate(timeout = 15)
print(out)
print(err) 

