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
cmd_citations= "select * from citations where id > 179205500"
wosdb = mysql.connector.connect(user='wos-user', password='#uHbG9LA', host='heisenberg', database='wos_tiny', charset='utf8', use_unicode=True, connection_timeout= 5000)

try:
    WOScursor = wosdb.cursor( dictionary = True)
except Exception as e:
    print("-" * 60)
    print(traceback.format_exc())
    print(sys.exc_info()[0])
    print("-" * 60)
###################################################################################################################
def citations_dictionary():
    WOScursor.execute(cmd_citations)
    counter =1
    start_time= time.time()
    i=0
    while True:
        i +=1        
        five_hundred_rows = WOScursor.fetchmany(100)
        if i %1000==0:  
            print(five_hundred_rows[len(five_hundred_rows)-1]['id'])
        if five_hundred_rows == []:
            break
        for row in five_hundred_rows:
            yield  row
    wosdb.close()


def es_index_bulk():
    es = Elasticsearch(timeout = 100, port=9208)
    # NOTE the (...) round brackets. This is for a generator.
    k = ({
            "_index": "wos_citations2017",
            "_type" : "citation",
            "_source": citation,
         }  for citation in citations_dictionary()  )
    #deque(helpers.parallel_bulk(es, k, thread_count=10), maxlen=0)
    result = helpers.bulk(es, k,request_timeout=100, stats_only=True, refresh=True)
    print(result)


es_index_bulk()
