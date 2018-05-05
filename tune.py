import string
import queue
from unidecode import unidecode
import traceback
import sys
from collections import defaultdict

from os.path import isfile, join
import os
from os import listdir

pairs=dict()
groundtruth = open('groundtruth/groundtruth.txt','r')
for line in groundtruth:
    splitted = line.split()
    if len(splitted)>1:
        pairs[splitted[0]]=splitted[1]


pos_path = './pos_citations_results/'
pos_files = [f for f in listdir(pos_path) if isfile(join(pos_path, f))]
neg_path = './neg_citations_results/'
neg_files = [f for f in listdir(neg_path) if isfile(join(neg_path, f))]


cosine_sim = [0.35 , 0.4, 0.45, 0.5, 0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85,0.9]
title_distance = [0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45]
#cosine_sim = [ 0.7]
#title_distance = [ 0.2]
tp = defaultdict(int)
fp = defaultdict(int)
for cosine_threshold in cosine_sim:
    print('-'*30)
    for title_threshold in title_distance:
        index = str(cosine_threshold)+str(title_threshold)
        false = 0
        for file in pos_files:
            if file.endswith('.txt'):
                f = open (pos_path+file, 'r')
                matched = False
                for line in f:
                    csx, wos, sim, dist,np = line.split(',')
                    if float(sim) >cosine_threshold or float(dist)< title_threshold:
                        if str(pairs[csx]).strip()==str(wos).strip(): 
                            tp[index] += 1
                        else:
                            #print('FP:', csx, wos, sim, dist)
                            fp[index] += 1
                        matched = True
                        break
                #if matched == False:
                #    #print('FN:' , csx)

        for file in neg_files:
            if file.endswith('.txt'):
                f = open (neg_path + file, 'r')
                for line in f:
                    csx, wos, sim, dist , pn = line.split(',')
                    if float(sim) >cosine_threshold or float(dist)< title_threshold:
                        fp[index]+=1
                        false +=1
                        #print('FP:', csx, wos, sim, dist)
                        break
        precision = tp[index]  / (tp[index]  + fp[index])
        recall = tp[index] /len(pos_files)
        f1 =  2*precision * recall /(precision+ recall)
        print("%.3f , %.3f, precision: %.3f, recall: %.3f, F1: %.3f, TP: %d, FP: %d  " % (cosine_threshold, title_threshold, precision , recall,  f1, tp[index], fp[index]))
        #print(false)
