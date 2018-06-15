import json
import pymysql
import traceback
import sys 
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MultiMatch, Match, Q
import numpy
from similarityProfile import *
import mysql.connector 
import queue
from sklearn.externals import joblib 
from multiprocessing import Process
import multiprocessing 
import threading 
import time
import fcntl
###################################################################################################################
cmd_paperids = "select id from papers limit 8000000, 2000000"  
cmd_paper = "select * from papers where id='%s'; "
cmd_author = "select fname, mname, lname from authors where paperid = '%s'"
csxdb = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01', database='citeseerx2', charset='utf8', use_unicode=True)
CSXcursor = csxdb.cursor( dictionary = True)
CSXauthorCursor = csxdb.cursor( dictionary = True)


cmd_medauthor = "select fname, mname, lname from medlineauthors where paperid = '%s'"
cmd_medpaper= "select * from medlinepapers where id = '%s'"
meddb = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01', database='medline2017_12', charset='utf8', use_unicode=True)
medcursor =  meddb.cursor(dictionary = True)

###################################################################################################################
t1 = time.time()
#q = queue.Queue()
q = multiprocessing.JoinableQueue()
CSXcursor.execute(cmd_paperids)
IDs = CSXcursor.fetchall()
print('total remaining:',len(IDs))
for id  in IDs:
    q.put(id['id'])


print('put done')
def find_match(queue, clf):
    try:
        client = Elasticsearch(timeout = 200, port = 9201)
        csxdb = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01', database='citeseerx2', charset='utf8', use_unicode=True)
        CSXcursor = csxdb.cursor( dictionary = True)
        CSXauthorCursor = csxdb.cursor( dictionary = True)
        meddb = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01', database='medline2017_12', charset='utf8', use_unicode=True)
        medcursor =  meddb.cursor(dictionary = True)
        s = Search(using=client, index="medline2017")  
    except:
        print("-" * 60)
        print(traceback.format_exc())
        print(sys.exc_info()[0])
        print("-" * 60)
    while(True):
        if queue.empty():
            break
        try:
            csxID = queue.get()
            if csxID is None:
                queue.task_done()
                break
            CSXcursor.execute(cmd_paper % (csxID))
            CSXPaper = CSXcursor.fetchone()
            if int(CSXPaper['rid']) % 1000 ==0:
                print(CSXPaper['rid'])
            CSXauthorCursor.execute(cmd_author % (csxID))
            CSXauthors = CSXauthorCursor.fetchall() 
            s = Search(using=client, index="medline2017")  
            if CSXPaper['title'] is None or len(CSXPaper['title'])<20:
                if len(CSXauthors)>0 and  CSXauthors[0]['lname'] is not None  and CSXPaper['year'] is not None:
                    s.query = Q('bool', should=[Q('match', year=CSXPaper['year']), Q('match',authors = CSXauthors[0]['lname'])])
                else:
                    if CSXPaper['abstract'] is not None:
                        s = s.query("match", abstract=CSXPaper['abstract'])
                    else:
                        queue.task_done()
                        continue
            else:
                s = s.query("match", title=CSXPaper['title'])
            response = s.execute()
            for hit in response:
                medcursor.execute(cmd_medpaper % (hit['id']))
                medpaper = medcursor.fetchone()
                medcursor.execute(cmd_medauthor % (hit['id']))
                medauthors = medcursor.fetchall()
                features = SimilarityProfile.calcFeatureVector(medpaper, medauthors, CSXPaper, CSXauthors)
                label = clf.predict([features])
                if label ==1:
                    with open("match_files9.txt", "a") as g:
                        fcntl.flock(g, fcntl.LOCK_EX)
                        g.write(csxID + '\t' + hit['id']+'\n')
                        fcntl.flock(g, fcntl.LOCK_UN)
                    break
            queue.task_done()
        except:
            queue.task_done()
            print("-" * 60)
            print(csxID)
            print(traceback.format_exc())
            print(sys.exc_info()[0])
            print("-" * 60)
        

clf = joblib.load('HMM.pkl')

processes =[]
threads =[] 
for i in range(16):
    p = Process(target=find_match, args=(q,clf, ))
    processes.append(p)
    p.start()
for p in processes:
    p.join()
q.join()
print('total time:',time.time()-t1)
