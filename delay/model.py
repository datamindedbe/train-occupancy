import sys

import numpy as np
import pandas as pd
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import RandomForestRegressor
from sklearn.feature_selection import SelectKBest
from sklearn.metrics import confusion_matrix
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import Imputer
from sklearn.preprocessing import OneHotEncoder
from sqlalchemy import create_engine
from sklearn.externals import joblib

from config import CONNECTION_STRING, ALL_FEATURES, CATEGORICAL_FEATURES, TARGET


def build_model_random_forest(df, features, categorical_features, target, split=0.70):
    print "using %d features (%d columns) on %d rows and target %s. Split %f." % (
    len(features), len(df.columns), len(df), target, split)
    df['is_train'] = np.random.uniform(0, 1, len(df)) <= split
    train, test = df[df['is_train'] == True], df[df['is_train'] == False]


    # one_hot_encoding because it doesn't work in pipeline for some reason
    # for f in categorical_features:
    #     dummies = pd.get_dummies(df[f], prefix=f)
    #     for dummy in dummies.columns:
    #         df[dummy] = dummies[dummy]
    #         features.append(dummy)
    #     df = df.drop(f, 1)
    #     features.remove(f)

    clf = Pipeline([
        ("imputer", Imputer(strategy="mean", axis=0)),
        ('feature_selection', SelectKBest(k=5)),
        ("forest", RandomForestClassifier())])
    clf.fit(train[features], train[target])
    score = clf.score(test[features], test[target])
    predicted = clf.predict(test[features])

    cm = confusion_matrix(test[target], predicted)
    print "Random Forest score: %f" % score
    print "confusion_matrix : \n%s" % cm
    return clf


def build_model(all_features, categorical_features, target, connection_string, filename):
    engine = create_engine(connection_string)

    query = """SELECT * FROM connection_features"""
    df = pd.read_sql_query(query, con=engine, index_col=['departurestop', 'departuredate', 'route'])
    df.index.levels[0].name = 'stationfrom'
    df.index.levels[1].name = 'date'
    df.index.levels[2].name = 'vehicle'
    df = df.reset_index()

    model = build_model_random_forest(df, all_features, categorical_features, target)
    joblib.dump(model, filename)




build_model(ALL_FEATURES, CATEGORICAL_FEATURES, TARGET, CONNECTION_STRING, 'model.pkl')


