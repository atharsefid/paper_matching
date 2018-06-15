from elasticsearch import Elasticsearch
import requests
import mysql.connector
import json
import pymysql
import traceback
import sys
import subprocess

###################################################################################################################
cmd_citations= "select * from citations"
wosdb = mysql.connector.connect(user='wos-user', password='#uHbG9LA', host='heisenberg', database='wos_tiny', charset='utf8', use_unicode=True, connection_timeout= 5000)

try:
    WOScursor = wosdb.cursor( dictionary = True)
except Exception as e:
    print("-" * 60) 
    print(traceback.format_exc())
    print(sys.exc_info()[0])
    print("-" * 60) 
###################################################################################################################


es = Elasticsearch(timeout = 1000, port=9208)
i=0
try:
    WOScursor.execute(cmd_citations)
    row = WOScursor.fetchone()
    i=1
    while row is not None:
        es.index(index='citations_wos', doc_type='citation', body=row)
        if i%5000==0:
            print(i)
        i+=1
        row = WOScursor.fetchone()
    WOScursor.close()
    wosdb.close()
except:
    print("-" * 60)
    print(traceback.format_exc())
    print(sys.exc_info()[0])
    print(i)
    print("-" * 60)
