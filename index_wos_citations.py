import mysql.connector
from collections import deque
import pymysql
import traceback
import sys
import time
import subprocess
from elasticsearch import helpers
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MultiMatch, Match, Q
import json

###################################################################################################################
cmd_citations= "select * from citations where id > 655000000"
wosdb = mysql.connector.connect(user='wos-user', password='#uHbG9LA', host='heisenberg', database='wos_tiny', charset='utf8', use_unicode=True, connection_timeout= 5000)

try:
    WOScursor = wosdb.cursor( dictionary = True)
except Exception as e:
    print("-" * 60)
    print(traceback.format_exc())
    print(sys.exc_info()[0])
    print("-" * 60)
###################################################################################################################
cmd = "curl 0.0.0.0:9201/citations_wos/_count" 
def citations_dictionary():
    WOScursor.execute(cmd_citations)
    counter =1
    start_time= time.time()
    while True:
        five_hundred_rows = WOScursor.fetchmany(50)
        if counter % 100000 ==0:
            print(counter)
        counter += len(five_hundred_rows)
        if five_hundred_rows == []:
            break
        for row in five_hundred_rows:
            yield  row
    wosdb.close()


def es_index_bulk():
    es = Elasticsearch(timeout = 1000, port=9201)
    # NOTE the (...) round brackets. This is for a generator.
    k = ({
            "_index": "citations_wos",
            "_type" : "citation",
            "_source": citation,
         }  for citation in citations_dictionary()  )
    #deque(helpers.parallel_bulk(es, k, thread_count=10), maxlen=0)
    result = helpers.bulk(es, k,request_timeout=10000, stats_only=True, refresh=True)
    print(result)


es_index_bulk()
