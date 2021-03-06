from sklearn.datasets import load_boston
from sklearn.ensemble import RandomForestRegressor
import numpy as np
from sklearn.externals import joblib
rf = joblib.load( 'RF_Model.pkl')
print ("Features sorted by their score:")
names = [i for i in range(1,12)]
print(list(zip(map(lambda x: round(x, 4), rf.feature_importances_), names)))
print(sorted(zip(map(lambda x: round(x, 4), rf.feature_importances_), names), reverse=True))

