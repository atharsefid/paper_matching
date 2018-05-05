import string
import queue
from unidecode import unidecode
import matplotlib
import threading
from simhash import Simhash
from citations_SimilarityProfile import *
from sklearn.externals import joblib
import requests
from xlrd import open_workbook
import mysql.connector
import csv
from sklearn.metrics import precision_recall_curve, auc
import numpy as np
from sklearn.metrics import jaccard_similarity_score
import json
import pymysql
import traceback
import sys
from sklearn.naive_bayes import GaussianNB
import subprocess
from imblearn.pipeline import make_pipeline
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MultiMatch, Match, Q
import numpy
from name_parser import *
from sklearn.ensemble import RandomForestClassifier
from random import randint
from html.parser import HTMLParser

###################################################################################################################
cmd_paperid = "select id from papers where id = '%s'"
cmd_total_paper = "select * from papers where id = '%s'"
cmd_citations = "select * from citations where paperid = '%s'"# this command gives all of the citations of paperid
cmd_citers = "select * from papers where uid ='%s'" # this command gives all of the papers that cited paperid
cmd_cited = "select * from citations where paperid ='%s'"
csxdb = mysql.connector.connect(user='csx-prod', password='csx-prod', host='csxdb02', database='citeseerx', charset='utf8', use_unicode=True)
wosdb = mysql.connector.connect(user='wos-user', password='#uHbG9LA', host='heisenberg', database='wos_tiny', charset='utf8', use_unicode=True)

try:
    CSXcursor = csxdb.cursor( dictionary = True)
    WOScursor = wosdb.cursor( dictionary = True)
    CSXCitationsCursor = csxdb.cursor( dictionary = True)
except Exception as e:
    print("-" * 60)
    print(traceback.format_exc())
    print(sys.exc_info()[0])
    print("-" * 60)
###################################################################################################################

def mystring(item):
    if item is None:
        return ''
    else:
         return str(item)



def find_match(csx_paperID):
    try:
        client = Elasticsearch(host="0.0.0.0", timeout = 200, port = 9201)
        CSXCitationsCursor.execute(cmd_total_paper % (csx_paperID))
        csx_title = CSXCitationsCursor.fetchone()['title']
        print('csx title:', csx_title)
        CSXCitationsCursor.execute(cmd_citations % (csx_paperID))
        CSXcitations = CSXCitationsCursor.fetchall()
        print('number of citations:',len(CSXcitations))
        page = 100
        for csx_citation in CSXcitations:
            start = 0
            if csx_citation['title'] is not None :
                s = Search(using=client, index="citations_wos").query("match", citedTitle=csx_citation['title'])
            else:
                break
            s = s[start:start+page]
            response = s.execute()
            print('*'*50)
            print('csx citation title:', csx_citation['title'])
            for hit in response:
                print('wos citation title:', hit['citedTitle'])
                WOScursor.execute(cmd_citers % (hit['paperid']))
                WOS_paper = WOScursor.fetchall()[0]
                print('wos citer title:',WOS_paper['title'])
                print('-'*10)
    except:
        print("-" * 60)
        print(str(traceback.format_exc()))
        print(str(sys.exc_info()[0]))
        print("-" * 60)
        csxdb.close()
        wosdb.close()



find_match('10.1.1.748.4644')
