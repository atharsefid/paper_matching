from elasticsearch import Elasticsearch
import requests
import mysql.connector
import json
import pymysql
import traceback
import sys

es = Elasticsearch(timeout = 100, port= 9202)
cmd_paper= "select id, title, abstract, year, venue, venueType, pages, volume, number, publisher, pubAddress from wospapers where rid > 19097000"
cmd_author = "select name, affil, address, email, ord from wosauthors where paperid = '%s';"
db = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01',pool_name = "mypool",
                                  pool_size = 10, database='wos2017_12', charset='utf8', use_unicode=True)

db2 = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01',pool_name = "mypool",
                                  pool_size = 10, database='wos2017_12', charset='utf8', use_unicode=True)
try:
    cursor = db.cursor( dictionary=True)
    authorCursor = db2.cursor( dictionary=True)
except Exception as e:
    print("could not get cursor")
    print("-" * 60)
    print(traceback.format_exc())
    print(sys.exc_info()[0])
    print("-" * 60)

i = 19097000 
try:
    cursor.execute(cmd_paper)
    row = cursor.fetchone()
    while row is not None:
        try:
            authorCursor.execute(cmd_author % (row['id']))
            authors = authorCursor.fetchall()
            row['authors'] = ""
            for author in authors:
                name =''
                affil=''
                email=''
                ord =''
                if author['name'] is not None:
                    name = author['name']

                if author['affil'] is not None:
                    affil = author['affil']

                if author['email'] is not None:
                    email = author['email']

                row['authors'] += '['+name + " " + affil + " " + email + ']' + " ;  "
        except:
            print('could not get authors')
        es.index(index='wos_papers', doc_type='paper', id=i, body=row)
        if i%1000==0:
            print(i)
        i+=1
        try:
            row = cursor.fetchone()
        except:
            print('could not fetch next record')
            print("-" * 60)
            print(traceback.format_exc())
            print(sys.exc_info()[0])
            print(i)
            row = cursor.fetchone()
            print("-" * 60)
    cursor.close()
    db.close()
    authorCursor.close()
    db2.close()
except:
    print("-" * 60)
    print(traceback.format_exc())
    print(sys.exc_info()[0])
    print(i)
    print("-" * 60)
