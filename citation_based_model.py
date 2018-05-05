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

def get_features(s):
    width = 3
    s = s.lower()
    s = re.sub(r'[^\w]+', '', s)
    return [s[i:i + width] for i in range(max(len(s) - width + 1, 1))]
def mystring(item):
    if item is None:
        return ''
    else:
         return str(item)
def normalize(text):
    if text is None:
        return None
    if text[:5] == 'orcid':
        text = text.split('>')[1]
    h = HTMLParser()
    text = str(h.unescape(text))
    normalizr = Normalizr(language='en')
    normalizations = [('replace_punctuation', {'replacement': ' '}),  
        'lower_case',
        ('remove_stop_words',{'ignore_case':'False'}),
        'remove_extra_whitespaces']
    text = normalizr.normalize(text, normalizations)
    return text

def compare_cosine_citations(wos,csx):
    wos_citations_titles = ' '.join(mystring(w['citedTitle']) for w in wos)
    csx_citations_titles = ' '.join(mystring(c['title']) for c in csx)

    wos_TF = collections.Counter(normalize(wos_citations_titles, normalizations).split())
    csx_TF = collections.Counter(normalize(csx_citations_titles, normalizations).split())
    wos_cordinality = math.sqrt(np.sum(np.square(np.array(list(wos_TF.values())))))
    csx_cordinality = math.sqrt(np.sum(np.square(np.array(list(csx_TF.values())))))
    cosine = 0
    for term in wos_TF.keys():
        if term in csx_TF.keys():
            cosine += wos_TF[term] * csx_TF[term]
    if wos_cordinality==0 or csx_cordinality==0:
        cosine = 0
    else:
        cosine = cosine/(wos_cordinality*csx_cordinality)
    return cosine

def jaccard_similarity(x,y):
    intersection_cardinality = len(set.intersection(*[set(x), set(y)]))
    union_cardinality = len(set.union(*[set(x), set(y)]))
    if union_cardinality !=0:
        return intersection_cardinality/float(union_cardinality)
    else:
        return 0

def compare_jaccard_citations(wos,csx):
    wos_citations_titles = ' '.join(mystring(w['citedTitle']) for w in wos)
    csx_citations_titles = ' '.join(mystring(c['title']) for c in csx)
    wos_bow = normalize(wos_citations_titles).split()
    csx_bow = normalize(csx_citations_titles).split()
    jaccard = jaccard_similarity(wos_bow, csx_bow)
    return jaccard




def find_match(queue, pairs, match_file, evaluate_file, pos_or_neg):
    while(True):
        if queue.empty():
            break
        shared_cit = 0
        prevID = None
        csx_paperID = queue.get()
        if csx_paperID is None:
            break
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
        #brk= False
        try:
            #f = open('./' + pos_or_neg + '_jaccard_or_0.7_0.2_results/' + csx_paperID, 'w')
            f2 = open('./' + pos_or_neg + '_citations_results/' + csx_paperID + '-eval.txt', 'w')
            CSXCitationsCursor.execute(cmd_citations % (csx_paperID))
            CSXcitations = CSXCitationsCursor.fetchall()
            #if CSXcitations == None or len(CSXcitations)==0:
            #    f.write('Paper %s does not have any citaions in citeseer. \n' % csx_paperID)
            #f.write('Number of citations of %s is %s\n' % (csx_paperID,len(CSXcitations) ) )
            #for c in CSXcitations:
            #    f.write(str(c) +'\n')
            counter = 0
            for csx_citation in CSXcitations:
                counter += 1
                #if brk ==True:
                #    break
                #f.write(str(counter) + ', ' + '-'*100 + '\n')
                #f.write('citation:'+csx_citation['raw']+'\n')
                #f.write('search title:'+ mystring(csx_citation['title'])+'\n')
                csx_citation['authors'] = parse_csx_authors(csx_citation['authors'])
                csx_citation['abstract'] = ''
                label = 1
                start = 0
                while (label ==1):
                    #if brk == True:
                    #    break
                    if start >= 10000:
                        break
                    if csx_citation['title'] is None and len(csx_citation['authors'])!=0:
                        if csx_citation['year'] is not None :
                            #f.write('search by author:%s and year:%s \n' %(csx_citation['authors'][0]['lname'],csx_citation['year']))
                            s = Search(using=client, index="citations_wos").query("match", citedAuthor=csx_citation['authors'][0]['lname']).query("match",year=csx_citation['year'])
                        else:
                            #f.write('search by author:'+csx_citation['authors'][0]['lname']+'\n')
                            s = Search(using=client, index="citations_wos").query("match", citedAuthor=csx_citation['authors'][0]['lname'])
                    elif csx_citation['title'] is not None:
                        s = Search(using=client, index="citations_wos").query("match", citedTitle=csx_citation['title'])
                    else:
                        break
                    #print('total hits:', s.count())
                    s = s[start:start+page]
                    response = s.execute()
                    if len(response) ==0:
                        label=0
                    for hit in response:
                        hit['abstract']=''
                        #if.write('hit title: '+ mystring(hit['citedTitle'])+'\n')
                        wos_citation = dict()
                        wos_citation['title']=hit['citedTitle']
                        wos_citation['year']=hit['year']
                        wos_citation['abstract']=hit['abstract']
                        wos_citation['pages']=hit['page']
                        wos_citation['volume']=hit['volume']
                        wos_citation['title']=hit['citedTitle']
                        features = SimilarityProfile.calcFeatureVector(csx_citation, csx_citation['authors'], wos_citation, parse_wos_authors(hit['citedAuthor']))
                        label = clf.predict([features])[0]
                        #if.write('label:'+str(label)+'\n')
                        if label==1:
                            #f.write('-'*50 + '\n')
                            WOScursor.execute(cmd_citers % (hit['paperid']))
                            WOS_paper = WOScursor.fetchall()[0]
                            CSXcursor.execute(cmd_total_paper % (csx_paperID) )
                            CSX_paper = CSXcursor.fetchall()[0]
                            WOScursor.execute(cmd_cited % (WOS_paper['uid']))
                            WOScitations = WOScursor.fetchall()
                            citations_similarity = compare_jaccard_citations(WOScitations,CSXcitations)
                            title1 = '%x' % Simhash(get_features(normalize(mystring(CSX_paper['title'])))).value 
                            title2 = '%x' % Simhash(get_features(normalize(mystring(WOS_paper['title'])))).value 
                            dist = distance.nlevenshtein(title1, title2)
                            #f.write('main paper:'+ CSX_paper['title']+'\n')
                            #f.write('candidate paper:'+WOS_paper['title']+'\n')
                            #f.write('cosine similarity: %s , leven distance : %s \n' % (citations_similarity, dist))
                            evaluate_file.write(CSX_paper['id']+', '+WOS_paper['uid']+ ', ' + str(citations_similarity) +', ' + str(dist) + ', ' + pos_or_neg +'\n')
                            f2.write(CSX_paper['id']+', '+WOS_paper['uid']+ ', ' + str(citations_similarity) +', ' + str(dist) + ', ' + pos_or_neg +'\n')
                            if citations_similarity > 0.7 or dist< 0.2:
                                #f.write('main paper:'+ CSX_paper['title']+'\n')
                                #f.write('candidate paper:'+WOS_paper['title']+'\n')
                                #f.write('total citations of csx:%s and wos:%s \n'%(len(CSXcitations),len(WOScitations)))
                                #f.write('\t'+hit['paperid']+'\n')
                                match_file.write(CSX_paper['id']+': '+WOS_paper['uid'] +'\n')
                                match_file.flush()
                                #f.write('******************match***************************\n')
                                #brk = True
                                #break
                            #f.write('-'*50 + '\n')
                    start= start +len(response)
                #f.flush()
        except:
            print("-" * 60)
            print('csx paper id:', csx_paperID)
            print(str(traceback.format_exc()))
            print(str(sys.exc_info()[0]))
            print("-" * 60)
            #f.close()
            csxdb.close()
            wosdb.close()
        #f.close()



match_file = open('pos_citation_result.txt','w')
evaluate_file = open('evaluate.txt','w')
non_match_file = open('neg_citation_result.txt','w')
pos_q = queue.Queue()
neg_q = queue.Queue()
client = Elasticsearch(host="0.0.0.0", timeout = 200, port = 9201)  
pos_pairs=dict()
groundtruth = open('./groundtruth/groundtruth.txt','r')
cnt = 0
for line in groundtruth:
    cnt += 1
    #if cnt >= 5:
    #    break
    splitted = line.split()
    CSXcursor.execute(cmd_paperid % (splitted[0]))
    CSX_paperIDs = CSXcursor.fetchall()
    if len(CSX_paperIDs)>0:
        if len(splitted)>1:
            pos_pairs[CSX_paperIDs[0]['id']]=splitted[1]
            pos_q.put(CSX_paperIDs[0]['id'])
        else:
            neg_q.put(CSX_paperIDs[0]['id'])
    else:
        print(splitted[0], 'does not exist')

page =100
clf = joblib.load('citations_model.pkl')

threads =[]
for i in range(8):
    t = threading.Thread(target=find_match, args=(pos_q,pos_pairs,match_file, evaluate_file,'pos',))
    threads.append(t)
    t.start()
for t in threads:
    t.join()

print("Positives Finished")



threads =[]
for i in range(8):
    t = threading.Thread(target=find_match, args=(neg_q,pos_pairs,non_match_file, evaluate_file,'neg',))
    threads.append(t)
    t.start()
for t in threads:
    t.join()
print('Totally Finished')




