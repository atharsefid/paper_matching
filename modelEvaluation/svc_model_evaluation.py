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
import numpy
from sklearn.ensemble import RandomForestClassifier
from random import randint
import pickle
from sklearn import preprocessing
import time


with open('lables.pickle', 'rb') as handle:
    classes = pickle.load(handle)
with open('features.pickle', 'rb') as handle:
    pairFeatures = pickle.load(handle)
pairFeatures = preprocessing.scale(pairFeatures)
param_grid = [  {'C': [1, 10, 100, 1000], 'kernel': ['linear'], 'probability':[True]},
             {'C': [1, 10, 100, 1000], 'gamma': [0.001, 0.0001], 'kernel': ['rbf'], 'probability':[True]}]
scores = ['precision', 'recall']
f = open('svm_gridsearchCV.txt','w')
for score in scores:
    f.write("# Tuning hyper-parameters for %s \n" % score)
    f.write('\n')
    clf = GridSearchCV(svm.SVC(), param_grid, cv=10 ,scoring='%s_macro' % score)
    clf.fit(pairFeatures, classes)
    f.write("Best parameters set found on development set: \n")
    f.write('\n')
    f.write(str(clf.best_params_))
    f.write('\n')
    f.write("Grid scores on development set: \n")
    f.write('\n')
    means = clf.cv_results_['mean_test_score']
    stds = clf.cv_results_['std_test_score']
    for mean, std, params in zip(means, stds, clf.cv_results_['params']):
        f.write("%0.3f (+/-%0.03f) for %r \n"
              % (mean, std * 2, params))
f, axes = plt.subplots(1,1, figsize=(10, 10))
clf = svm.SVC(**clf.best_params_)
kf = StratifiedKFold(n_splits=10, shuffle = True)
p =[]
r = []
y_real = []
y_proba = []
i =1
train_time =[]
test_time = []
for train_index, test_index in kf.split(pairFeatures, classes):
    t1 = time.time()
    clf.fit(pairFeatures[train_index],classes[train_index])
    t2 = time.time()
    train_time.append(t2-t1)
    prediction = clf.predict(pairFeatures[test_index])
    test_time.append(time.time()-t2)
    pred_proba = clf.predict_proba(pairFeatures[test_index])
    precision = precision_score(classes[test_index],prediction)
    recall = recall_score(classes[test_index],prediction)
    p.append(precision)
    r.append(recall)
    precision, recall, _ = precision_recall_curve(classes[test_index], pred_proba[:,1])
    lab = 'Fold %d AUC=%.4f' % (i, auc(recall, precision))
    axes.step(recall, precision, label=lab)
    y_real.append(classes[test_index])
    y_proba.append(pred_proba[:,1])
    i+=1

y_real = numpy.concatenate(y_real)
y_proba = numpy.concatenate(y_proba)
precision, recall, _ = precision_recall_curve(y_real, y_proba)
lab = 'Overall AUC=%.4f' % (auc(recall, precision))
axes.step(recall, precision, label=lab, lw=2, color='black')
axes.set_xlabel('Recall', fontsize=15)
axes.set_ylabel('Precision', fontsize=15)
axes.legend(loc='lower left', fontsize=15)
axes.tick_params(labelsize=15)
f.tight_layout()
f.savefig('SVM.png')


print('precision: ',p)
print('recall:',r)
print('total precision:', sum(p)/len(p))
print('total recall:', sum(r)/len(r))
print('average train time:', sum(train_time)/len(train_time))
print('average test time:', sum(test_time)/len(test_time))
clf.fit(pairFeatures,classes)
joblib.dump(clf, 'svm_jaccardModel.pkl')



