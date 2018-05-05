import matplotlib
import random
from sklearn.externals import joblib
matplotlib.use('Agg')
import requests
import mysql.connector
import matplotlib.pyplot as plt
import csv
from sklearn.metrics import precision_recall_curve, auc
import json
import pymysql
import traceback
import sys
from sklearn.naive_bayes import GaussianNB
from jaccard_SimilarityProfile import SimilarityProfile
#from citations_SimilarityProfile import SimilarityProfile
import subprocess
from imblearn.pipeline import make_pipeline
from elasticsearch_dsl.connections import connections
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MultiMatch, Match, Q
from sklearn import svm
from sklearn.model_selection import cross_val_score
from sklearn.utils import shuffle
from sklearn.model_selection import KFold
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.model_selection import GridSearchCV
from sklearn import linear_model
from imblearn.over_sampling import ADASYN, SMOTE, RandomOverSampler
import numpy
from sklearn.ensemble import RandomForestClassifier
from random import randint

def combine(neg_features, pos_features, diff):
    poses = [1]* len(pos_features)
    negs = [0] * len(neg_features)
    #print('diff:', diff)
    features = neg_features[:]
    classes = negs
    for i in range(diff+1):
        features += pos_features
        classes +=poses  
    #print("len features:",len(features), " ,len classes:",len(classes))
    return numpy.array(features), numpy.array(classes)



client = Elasticsearch(timeout = 40, port = 9202)

def wos_samples():
    client = Elasticsearch(host="heisenberg.ist.psu.edu", timeout = 1000)
    ###################################################################################################################
    #cmd_IEEEpapers = 'select * authors join papers on authors.paperid = papers.pmid where authors.id = %s'
    cmd_author = "select fname, mname, lname from authors where paperid = '%s';"
    cmd_paper= "select * from papers where id = '%s'"
    cmd_IEEEauthor = "select fname, mname, lname from wosauthors where paperid = '%s';"
    cmd_IEEEpaper= "select * from wospapers where id = '%s'"

    db = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01', database='citeseerx2', charset='utf8', use_unicode=True)

    IEEEdb = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01', database='wos2017_12', charset='utf8', use_unicode=True)
    try:
        CSXcursor = db.cursor( dictionary = True)
        CSXauthorCursor = db.cursor( dictionary = True)
        IEEEcursor =  IEEEdb.cursor(dictionary = True)
    except Exception as e:
        print("-" * 60)
        print(traceback.format_exc())
        print(sys.exc_info()[0])
        print("-" * 60)
    ###################################################################################################################
    # reading fround truth from file
    f =  open('./groundtruth/groundtruth.txt','r')
    groundtruth=dict()
    for  line in f:
        parts = line.split()
        if len(parts)>1:
            groundtruth[parts[0]] = parts[1] 
        else:
            groundtruth[parts[0]] = None
    random.seed(0)
    neg_features = []
    pos_features = []
    cnt =1
    pp = 0
    for csxID in groundtruth.keys():
        if groundtruth[csxID] is not None:
            pp +=1
        #print(csxID)
        cnt += 1
        #if cnt >=30 :
        #    break
        try:
            CSXcursor.execute(cmd_paper % (csxID))
            CSXPaper = CSXcursor.fetchone()
            if CSXPaper is None:
                print(csxID,'is none from db')
                continue
            CSXauthorCursor.execute(cmd_author % (csxID))
            CSXauthors = CSXauthorCursor.fetchall()
            #print('paper',CSXPaper)
            #print('author',CSXauthors)

            s = Search(using=client, index="wos_papers")
            if CSXPaper['title'] is None or len(CSXPaper['title'])<20:
                if len(CSXauthors)>0 and CSXPaper['year'] is not None:
                    s.query = Q('bool', should=[Q('match', year=CSXPaper['year']), Q('match',authors = CSXauthors[0]['lname'])])
                else:
                    if CSXPaper['abstract'] is not None:
                        s = s.query("match", abstract=CSXPaper['abstract'])
                    else:
                        print(csxID," does not have author or year or abstract")
            else:
                s = s.query("match", title=CSXPaper['title'])

            response = s.execute()
            first = True
            for hit in response:
                IEEEcursor.execute(cmd_IEEEpaper % (hit['id']))
                IEEEpaper = IEEEcursor.fetchone()
                IEEEcursor.execute(cmd_IEEEauthor % (hit['id']))
                IEEEauthors = IEEEcursor.fetchall()
                #print('wos paper:',IEEEpaper)
                #print('wos authors:',IEEEauthors)
                features = SimilarityProfile.calcFeatureVector(IEEEpaper, IEEEauthors, CSXPaper, CSXauthors)
                #print(features)
                if(len(features) ==0 ):
                    print (' '* 30,'-'*50, ' '*30)
                    print(' '*50, 'ERROR' )
                    print (' '* 30,'-'*50, ' '*30)
                if (hit['id']==groundtruth[csxID]):
                    pos_features.append(features)
                else:
                    neg_features.append(features)
        except:
            print("-" * 60)
            print(traceback.format_exc())
            print(sys.exc_info()[0])
            print("-" * 60)
    CSXcursor.close()
    db.close()
    CSXauthorCursor.close()
    print('pp',pp)
    return(pos_features,neg_features )

def ieee_samples():
    client = Elasticsearch(timeout = 40)
    ###################################################################################################################
    cmd_author = "select fname, mname, lname from authors where paperid = '%s';"
    cmd_paper= "select * from papers where id = '%s'"
    cmd_IEEEauthor = "select fname, mname, lname from authors where paperid = '%s';"
    cmd_IEEEpaper= "select * from papers where id = '%s'"
    cmd_matches = "select * from master_final where ieeeid != 'Null';" # this db contains csx-ieee ground truth

    db = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01', database='citeseerx2', charset='utf8', use_unicode=True)
    Matchesdb = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01', database='citeseerx2', charset='utf8', use_unicode=True)

    IEEEdb = mysql.connector.connect(user='csx-devel', password='csx-devel', host='csxstaging01', database='citegraph_ieee', charset='utf8', use_unicode=True)
    try:
        matchesCursor =  Matchesdb.cursor( dictionary = True)
        CSXcursor = db.cursor( dictionary = True)
        CSXauthorCursor = db.cursor( dictionary = True)
        IEEEcursor =  IEEEdb.cursor(dictionary = True)
    except Exception as e:
        print("-" * 60)
        print(traceback.format_exc())
        print(sys.exc_info()[0])
        print("-" * 60)
    ###################################################################################################################
    random.seed(0)
    matchesCursor.execute(cmd_matches)
    match = matchesCursor.fetchone()
    neg_features = []
    pos_features = []
    num_matches = 0
    while match is not None:
        num_matches +=1
        #print(match['csxid'])
        #if num_matches > 10:
        #    break
        try:
            CSXcursor.execute(cmd_paper % (match['csxid']))
            CSXPaper = CSXcursor.fetchone()
            if CSXPaper is None:
                match = matchesCursor.fetchone()
                continue
            CSXauthorCursor.execute(cmd_author % (CSXPaper['id']))
            CSXauthors = CSXauthorCursor.fetchall()
            #print(CSXPaper)
            #print(CSXauthors)

            s = Search(using=client, index="ieeeelastic")
            if CSXPaper['title'] is None or len(CSXPaper['title'])<20:
                if len(CSXauthors)>0 and CSXPaper['year'] is not None:
                    s.query = Q('bool', should=[Q('match', year=CSXPaper['year']), Q('match',authors = CSXauthors[0]['lname'])])
                else:
                    if CSXPaper['abstract'] is not None:
                        s = s.query("match", abstract=CSXPaper['abstract'])
                    else:
                        print(line.strip()," does not have author or year or abstract")
            else:
                s = s.query("match", title=CSXPaper['title'])
            response = s.execute()
            first = True
            for hit in response:  
                IEEEcursor.execute(cmd_IEEEpaper % (hit['id']))
                IEEEpaper = IEEEcursor.fetchone()
                IEEEcursor.execute(cmd_IEEEauthor % (hit['id']))
                IEEEauthors = IEEEcursor.fetchall()
                #print(IEEEpaper)
                #print(IEEEauthors)
                features = SimilarityProfile.calcFeatureVector(IEEEpaper, IEEEauthors, CSXPaper, CSXauthors)
                #print(features)
                if(len(features) ==0 ):
                    print (' '* 30,'-'*50, ' '*30)
                    print(' '*50, 'ERROR' )
                    print (' '* 30,'-'*50, ' '*30)
                if (hit['id']==int(match['ieeeid'])):
                    pos_features.append(features)
                else:
                    neg_features.append(features)
        except:
            print("-" * 60) 
            print(traceback.format_exc())
            print(sys.exc_info()[0])
            print("-" * 60) 
        match = matchesCursor.fetchone()
    CSXcursor.close()
    db.close()
    CSXauthorCursor.close()
    return pos_features, neg_features
random.seed(0)
WOS_pos , WOS_neg = wos_samples()
print(len(WOS_pos),len(WOS_neg))
IEEE_pos , IEEE_neg = ieee_samples()
print(len(IEEE_pos),len(IEEE_neg))
#DBLP_pos , DBLP_neg = wos_samples()
pos_features =  WOS_pos + IEEE_pos # + DBLP_pos
neg_features =  WOS_neg + IEEE_neg #+ DBLP_neg
print('total number of positives:',len(pos_features))
print('total number of negatives:',len(neg_features))
pairFeatures, classes = combine(neg_features, pos_features, 0)
pairFeatures, classes = shuffle(pairFeatures, classes , random_state=0)
'''param_grid = {"n_estimators": [20, 30, 40],
              "max_depth": [3, 10 , 100 , None],
              "max_features": [1, 3, 6],
              "min_samples_split": [2,  3, 5, 10],
              "min_samples_leaf": [1, 3, 10],
              "bootstrap": [True, False],
              "criterion": ["gini", "entropy"]}

scores = ['precision', 'recall']

for score in scores:
    print("# Tuning hyper-parameters for %s" % score)
    print()

    clf = GridSearchCV(RandomForestClassifier(), param_grid, cv=5,
                       scoring='%s_macro' % score)
    clf.fit(pairFeatures, classes)

    print("Best parameters set found on development set:")
    print()
    print(clf.best_params_)
    print()
    print("Grid scores on development set:")
    print()
    means = clf.cv_results_['mean_test_score']
    stds = clf.cv_results_['std_test_score']
    for mean, std, params in zip(means, stds, clf.cv_results_['params']):
        print("%0.3f (+/-%0.03f) for %r"
              % (mean, std * 2, params))
'''
clf.best_params_ = {'max_depth': 10, 'n_estimators': 30, 'criterion': 'gini', 'max_features': 3, 'min_samples_leaf': 1, 'min_samples_split': 10, 'bootstrap': False}
clf = RandomForestClassifier(**clf.best_params_)
kf = StratifiedKFold(n_splits=5, shuffle = True)
p =[]
r = []
for train_index, test_index in kf.split(pairFeatures, classes):
    clf.fit(pairFeatures[train_index],classes[train_index])
    prediction = clf.predict(pairFeatures[test_index])
    precision = precision_score(classes[test_index],prediction)
    recall = recall_score(classes[test_index],prediction)
    p.append(precision)
    r.append(recall)
    print('precision:', precision)
    print('recall:',recall)
print('total precision:',sum(p)/len(p))
print('total recall:',sum(r)/len(r))

clf.fit(pairFeatures,classes)
joblib.dump(clf, 'jaccardModel.pkl')
