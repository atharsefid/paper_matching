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

client = Elasticsearch(timeout = 40, port= 9202)

cmd_paper= "select * from papers where rid = %s"
cmd_author = "select fname, mname, lname from authors where paperid = '%s';"
db = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01',pool_name = "mypool",
                                  pool_size = 10, database='citeseerx2', charset='utf8', use_unicode=True)

db2 = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01',pool_name = "mypool",
                                  pool_size = 10, database='citeseerx2', charset='utf8', use_unicode=True)
try:
    cursor = db.cursor( dictionary=True)
    authorCursor = db2.cursor( dictionary=True)
except Exception as e:
    print("could not get cursor")
    print("-" * 60)
    print(traceback.format_exc())
    print(sys.exc_info()[0])
    print("-" * 60)
f = open ("remaining_ground_truth.txt", 'r')
output = open("higheasyMatcher.txt", 'w')
i=1
Nones = 0
for line in f:
    line = line.split()[0]
    if True: 
    #try:
        output.write('-' * 150+ "\n")
        #print(cmd_paper % (line.strip()) )
        cursor.execute(cmd_paper % (line.strip()))
        row = cursor.fetchone()
        if row is None:
            print(line.strip(), 'does not exist at all')
            output.write(line.strip() +"does not exist at all")
            Nones +=1
            continue
        authorCursor.execute(cmd_author % (row['id']))
        authors = authorCursor.fetchall()
        s = Search(using=client, index="wos_papers")
        if row['title'] is None or len(row['title'])<20:
            if len(authors)>0 and row['year'] is not None:
                s.query = Q('bool', should=[Q('match', year=row['year']), Q('match',authors = authors[0]['lname'])])
            else:
                if row['abstract'] is not None:
                    s = s.query("match", abstract=row['abstract'])
                else:
                    print(line.strip()," does not have author or year or abstract")
                    output.write(line.strip()+"does not have author or year or abstract")
                    Nones+=1
        else:
            s = s.query("match", title=row['title'])
        if row['abstract'] == None:
            row['abstract'] = ''


        if row['volume'] == None:
            row['volume'] = ''


        if row['year'] == None:
            row['year'] = ''


        if row['venue'] == None:
            row['venue'] = ''


        if row['doi'] == None:
            row['doi'] = ''


        if row['title'] == None:
            row['title'] = ''
        output.write( 'id:'+ row['id']+'|'+ str(row['rid']) +'|' +  "doi:" + row['doi'] + " | " + "title:"+row['title']+' | \n' \
        "abstract:" + row['abstract'] +' | \n' \
        + ' | year:'+ str(row['year'])+' | volume:'+ str(row['volume'])+ ' | venue:' + str( row['venue']) + ' | \n' )
        output.write("authors: " + str(authors)+ "\n")
        response = s.execute()
        output.write('~' * 100+ "\n")
        for hit in response:
            if hit.meta.score >20:
                if hit['abstract'] == None:
                    hit['abstract'] = ''
                if hit['authors'] == None:
                    hit['authors'] = ''
                if hit['volume'] == None:
                    hit['volume'] = ''
                if hit['year'] == None:
                    hit['year'] = ''
                if hit['venue'] == None:
                    hit['venue'] = ''
                if hit['title'] == None:
                    hit['title'] = ''
                output.write(str(hit.meta.score)+'|'+hit['id'] + " | " + hit['title'] + ' | ' + hit['abstract']\
                  + ' | '+ str(hit['year'])+' | '+ str(hit['volume'])+ ' | '+str( hit['venue']) + ' | ' + hit['authors'] + "\n")

                output.write('*' * 100+ "\n")
        #output.write('-' * 150+ "\n")
        if i%100 == 0:
            print(i)
        i+=1
    '''except:
        print(" could not read all retreived data")
        print("-" * 60)
        print(traceback.format_exc())
        print(sys.exc_info()[0])
        print("-" * 60)
    '''
print('total nones:',Nones)
cursor.close()
db.close()
authorCursor.close()
db2.close()
