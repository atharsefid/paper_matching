import pickle
import jellyfish
import codecs
import collections
from scipy import spatial
import math
import numpy as np
import os
from string import punctuation
import distance
import re
from simhash import Simhash
import hashlib
from normalizr  import Normalizr
from unidecode import unidecode 
import string


def normalize(text):
    lower = unidecode(text.lower())  #put it in lowercase
    translator = str.maketrans(string.punctuation, ' '*len(string.punctuation)) #gets rid of puncuation
    norm = lower.translate(translator)
    norm = re.sub(' +', ' ', norm) #get rid of extra spaces
    for r in (("'t ",""),("'s ",""),(' an ',' '),(' and ',' '),(' are ',' '),(' as ',' '),(' at ',' '),(' be ',' '),(' but ',' '),(' by ',' '),(' for ',' '),(' if ',' '),(' in ',' '),(' into ',' '),(' is ',' '),(' it ',' '),(' no ',' '),(' not ',' '),(' of ',' '),(' on ',' '),(' or ' ,' '),(' such ',' '),(' that ',' '),(' the ',' '),(' their ',' '),(' then ',' '),(' there ',' '),(' these ',' '),(' they ',' '),(' this ',' '),(' to ',' '),(' was ',' '),(' will ',' '),(' with ',' ')):
        norm = str(norm).replace(*r)
    return norm


def get_features(s):
    width = 3 
    s = s.lower()
    s = re.sub(r'[^\w]+', '', s)
    return [s[i:i + width] for i in range(max(len(s) - width + 1, 1))]


class SimilarityProfile(object):
    """
    Class to generate feature vector for pairwise classifier
    """
    EPS = 2.2250738585072014e-308
    @staticmethod
    def calcFeatureVector(p1, a1s, p2, a2s):
        featVector = list()
        def calcBinaryAuthorFeats(a1, a2, featVector):
            feature = ''
            # first name
            if (a1['fname'] is None or a2['fname'] is None):
                feature += '1'
            else:
                if a1['fname'][:1]== a2['fname'][:1]:
                    feature = '1' + feature
                else:
                    feature = '0' + feature
            #middle name
            if (a1['mname'] is None or a2['mname'] is None):
                feature = '1' + feature
            else:
                if a1['mname'][:1] == a2['mname'][:1]:
                    feature = '1'+ feature
                else:
                    feature = '0'+ feature
            # last name
            if (a1['lname'] is None or a2['lname'] is None):
                feature = '1' + feature
            else:
                if a1['lname'] == a2['lname']:
                    feature = '1'+ feature
                else:
                    feature = '0'+ feature
            feature = '0b' + feature
            featVector.append(int(feature,2))


        # features related to author name and order
        def calcAuthorFeats(a1, a2, featVector):
            if (a1['fname'] is None or a2['fname'] is None):
                featVector.append(1.0)
            else:  
                """ first name """
                f1 = a1['fname'].lower()
                f2 = a2['fname'].lower()

                f = 0.0
                f_jw = 0.0

                if len(f1) == 0 or len(f2) == 0:
                    f = 1.0
                elif len(f1) > 1 and len(f2) > 1:
                    if f1 == f2:
                        f = 3.0
                    else:
                        f = 0.0
                else:
                    if f1[:1] == f2[:1]:
                        f = 2.0
                    else:
                        f = 0.0
                featVector.append(f)
            #print(featVector)
            if (a1['mname'] is None or a2['mname'] is None):
                featVector.append(1.0)
            else:
                """ middle name """
                m1 = a1['mname'].lower()
                m2 = a2['mname'].lower()

                m = 0.0
                if len(m1) == 1 and len(m2) > 1:
                    m2 = m2[:1]
                elif len(m2) == 1 and len(m1) > 1:
                    m1 = m1[:1]

                if m1 == m2:
                    if len(m1) > 0:
                        m = 3.0
                    else:
                        m = 2.0
                else:
                    if len(m1) > 0 and len(m2) > 0:
                        m = 0.0
                    else:
                        m = 0.0
                featVector.append(m)

            if (a1['lname'] is None or a2['lname'] is None):
                featVector.append(1.0)
            else:
                last1 = a1['lname'].lower()
                last2 = a2['lname'].lower()

                last = 0.0 
                if len(last1) == 0 or len(last2) == 0:
                    last = 1.0
                elif last1 == last2:
                    last = 3.0
                else:
                    last = 0.0
                featVector.append(last)
        def calcYearFeats(year1, year2, featVector):
            if year1 is None or year2 is None:
                featVector.append(-1)
                return
            if year1 < 1800 or year2 < 1800:
                j_year_diff = -1
            elif year1 > 2020 or year2 > 2020:
                j_year_diff = -1
            else:
                j_year_diff = abs(year1 - year2)
                if j_year_diff > 100:
                    j_year_diff = 100
            featVector.append(float(j_year_diff))

        # features related to title
        def xstr(string):
            return string if string is not None else  ''

        def calcBOWcosine(title1, title2, abstract1, abstract2, featVector):
            normalizr = Normalizr(language='en')
            normalizations = ['remove_extra_whitespaces',
            ('replace_punctuation', {'replacement': ' '}),
            'lower_case',
            ('remove_stop_words',{'ignore_case':'False'})]
            TF1 = collections.Counter(normalizr.normalize(xstr(title1)+' '+ xstr(abstract1), normalizations).split())

            TF2 = collections.Counter(normalizr.normalize(xstr(title2)+' '+ xstr(abstract2), normalizations).split())
            cordinality1 = math.sqrt(np.sum(np.square(np.array(list(TF1.values())))))
            cordinality2 = math.sqrt(np.sum(np.square(np.array(list(TF2.values())))))
            cosine = 0
            for term in TF1.keys():
                if term in TF2.keys():
                    cosine += TF1[term] * TF2[term]
            if cordinality1==0 or cordinality2==0:
                cosine = 0
            else:
                cosine = cosine/(cordinality1*cordinality2)
            featVector.append(1-cosine)



        def calcTitleHashFeats(title1, title2, featVector):
            if title1 is None or title2 is None or title1=='' or title2=='':
                featVector.append(0)
                return
            title1 = '%x' % Simhash(get_features(normalize(title1))).value
            title2 = '%x' % Simhash(get_features(normalize(title2))).value
            if not title1 or not title2:
                t2 = 0
            else:
                t2 = distance.levenshtein(title1, title2)
            featVector.append(t2)
        def calcAbstractHashFeats(abstract1, abstract2, featVector):
            if abstract1 is None or abstract2 is None or abstract1 =='' or abstract2=='':
                featVector.append(0)
                return
            abstract1 = '%x' % Simhash(get_features(abstract1)).value
            abstract2 = '%x' % Simhash(get_features(abstract2)).value
            if not abstract1 or not abstract2:
                t2 = 0
            else:
                t2 = distance.levenshtein(abstract1, abstract2)
            featVector.append(t2)
        def authors_jaccord(a1s,a2s, featVector):
            a1_lasts = set()
            a2_lasts =  set()
            for a in a1s:
                a1_lasts.add(a['lname'])
            for a in a2s:
                a2_lasts.add(a['lname'])
            jaccord_value = compute_jaccard_index(a1_lasts, a2_lasts)
            featVector.append(jaccord_value)
        def compute_jaccard_index(set1, set2):
            n = len(set1.intersection(set2))
            return n / float(len(set1) + len(set2) - n + SimilarityProfile.EPS)
        if a1s is None:
            a1s = []
        if a2s is None:
            a2s = []
        if len (a1s) ==0 or len(a2s)==0:
            '''
            featVector.append(0)
            featVector.append(0)
            featVector.append(0)
            featVector.append(0)
            featVector.append(0)
            featVector.append(0)
            '''
            featVector.append(0)
            featVector.append(0)
        else:
            #calcAuthorFeats(a1s[0], a2s[0], featVector)# first author features
            #calcAuthorFeats(a1s[len(a1s)-1], a2s[len(a2s)-1], featVector)# last author features
            calcBinaryAuthorFeats(a1s[0], a2s[0], featVector)
            calcBinaryAuthorFeats(a1s[len(a1s)-1], a2s[len(a2s)-1], featVector)

        calcYearFeats(p1['year'], p2['year'], featVector)
        calcTitleHashFeats(p1['title'], p2['title'], featVector)
        calcAbstractHashFeats(p1['abstract'], p2['abstract'], featVector)
        authors_jaccord(a1s,a2s, featVector)
        calcBOWcosine(p1['title'], p2['title'],p1['abstract'], p2['abstract'], featVector)
        return featVector





