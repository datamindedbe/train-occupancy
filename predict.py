import sys

import numpy as np
import pandas as pd
from sklearn import preprocessing
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import Imputer
from sqlalchemy import create_engine

ALL_FEATURES = [
    # 'useragent',
    'month',
    'day',
    'dow',
    'hour',
    'quarter',
    # 'vehicle',
    # 'stationfrom', #departurestop
    # 'arrivalstop',
    'traintype',
    'departurename',
    # 'arrivalname',
    'weighted_freqs',
    'absolute_freqs',
    'distance',
    'weighted_positive_freqs',
    'am_weighted_freqs',
    'before_noon',
    'morning_commute',
    'evening_commute'
]

CATEGORICAL_FEATURES = [
    # 'useragent',
    # 'vehicle',
    'departurename',
    # 'arrivalname',
    'traintype'
]

TARGET = 'occupancy'

CONNECTION_STRING = 'postgresql://krispeeters@localhost:5432/trains'


def label_encoder(df, categorical_features):
    for feature in categorical_features:
        try:
            # df[feature] = df[feature].map(tostr)
            df[feature].replace('None', 'UNKNOWN', inplace=True)
            df[feature].fillna('UNKNOWN', inplace=True)
            enc = preprocessing.LabelEncoder()
            df[feature] = enc.fit_transform(df[feature])
        except:
            df.drop(feature, axis=1)
            print "feature %s dropped because it could not be encoded" % feature
            print "Unexpected error:", sys.exc_info()
    return df


def make_predictions_random_forest(df, features, target, split=0.70):
    print "using %d features (%d columns) on %d rows and target %s. Split %f." % (
    len(features), len(df.columns), len(df), target, split)
    # print "unused features: ", '\n\t\t'.join([f for f in df.columns if f not in features])
    # print "columns: ", '\n\t\t'.join(df.columns)
    df['is_train'] = np.random.uniform(0, 1, len(df)) <= split
    train, test = df[df['is_train'] == True], df[df['is_train'] == False]

    clf = Pipeline([
        ("imputer", Imputer(strategy="mean", axis=0)),
        # ('feature_selection', SelectKBest(k=200)),
        ("forest", RandomForestClassifier(
            min_samples_leaf=1, min_samples_split=10, n_estimators=60, max_depth=None, criterion='gini'))])
    clf.fit(train[features], train[target])
    score = clf.score(test[features], test[target])
    predicted = clf.predict(test[features])

    cm = confusion_matrix(test[target], predicted)
    # print classification_report(test[target], predicted)

    return score, cm


# Utility function to report best scores
def report(results, n_top=3):
    for i in range(1, n_top + 1):
        candidates = np.flatnonzero(results['rank_test_score'] == i)
        for candidate in candidates:
            print("Model with rank: {0}".format(i))
            print("Mean validation score: {0:.3f} (std: {1:.3f})".format(
                results['mean_test_score'][candidate],
                results['std_test_score'][candidate]))
            print("Parameters: {0}".format(results['params'][candidate]))
            print("")


def make_predictions(all_features, categorical_features, target, connection_string):
    engine = create_engine(connection_string)

    query = """
WITH frequencies AS (SELECT
                       c.departurestop,
                       count(*) AS freq
                     FROM connection c
                       INNER JOIN station s ON c.departurestop = s.id
                     GROUP BY departurestop, s.name),
    trip AS (SELECT
               d.*,
               sf.name                            AS stationfrom_name,
               st.name                            AS stationto_name,
               f.freq,
               CASE WHEN d.distance <= 0
                 THEN 0
               ELSE
                 f.freq :: FLOAT END              AS absolute_freq,
               CASE WHEN d.distance = 0
                 THEN 0
               ELSE
                 f.freq :: FLOAT / d.distance END AS weighted_freq,
               CASE WHEN d.distance <= 0
                 THEN 0
               ELSE
                 f.freq :: FLOAT / d.distance END AS weighted_positive_freq
             FROM distance d INNER JOIN frequencies f ON d.stationto = f.departurestop
               INNER JOIN station sf ON d.stationfrom = sf.id
               INNER JOIN station st ON d.stationto = st.id
             ORDER BY d.vehicle, d.date, d.distance),
    weighted_freqs AS (SELECT
                         stationfrom,
                         vehicle,
                         date,
                         AVG(distance)               AS distance,
                         SUM(absolute_freq)          AS absolute_freqs,
                         SUM(weighted_freq)          AS weighted_freqs,
                         SUM(weighted_positive_freq) AS weighted_positive_freqs
                       FROM trip
                       GROUP BY stationfrom, vehicle, date)
SELECT
  o.occupancy,
  o.useragent,
  c.*,
  EXTRACT(MONTH FROM c.departuretime)                   AS month,
  EXTRACT(DAY FROM c.departuretime)                     AS day,
  EXTRACT(DOW FROM c.departuretime)                     AS dow,
  EXTRACT(HOUR FROM c.departuretime)                    AS hour,
  EXTRACT(MINUTE FROM c.departuretime) :: INT / 15 * 15 AS quarter,
  d.name                                                AS departurename,
  a.name                                                AS arrivalname,
  regexp_replace(c.route, '[^A-Z]', '', 'g')            AS traintype,
  f.absolute_freqs,
  f.weighted_freqs,
  f.weighted_positive_freqs,
  f.distance,
  CASE WHEN EXTRACT(HOUR FROM c.departuretime) < 12
    THEN f.weighted_freqs
  ELSE
    -f.weighted_freqs END                               AS am_weighted_freqs,
  CASE WHEN EXTRACT(HOUR FROM c.departuretime) < 12
    THEN 1
  ELSE
    0 END                                               AS before_noon,
  CASE WHEN EXTRACT(HOUR FROM c.departuretime) >= 7 AND EXTRACT(HOUR FROM c.departuretime) <= 10
            AND EXTRACT(DOW FROM c.departuretime) > 0 AND EXTRACT(DOW FROM c.departuretime) < 6
    THEN 1
  ELSE
    0 END                                               AS morning_commute,
  CASE WHEN EXTRACT(HOUR FROM c.departuretime) >= 16 AND EXTRACT(HOUR FROM c.departuretime) <= 19
            AND EXTRACT(DOW FROM c.departuretime) > 0 AND EXTRACT(DOW FROM c.departuretime) < 6
    THEN 1
  ELSE
    0 END                                               AS evening_commute
FROM occupancy o
  INNER JOIN connection c ON o.stationfrom = c.departurestop AND o.date = c.departuredate AND o.vehicle = c.route
  INNER JOIN weighted_freqs f ON o.stationfrom = f.stationfrom AND o.vehicle = f.vehicle AND o.date = f.date
  LEFT JOIN station d ON c.departurestop = d.id
  LEFT JOIN station a ON c.arrivalstop = a.id
ORDER BY occupancy, trip, departuretime;
    """
    df = pd.read_sql_query(query, con=engine, index_col=['departurestop', 'departuredate', 'route'])
    df.index.levels[0].name = 'stationfrom'
    df.index.levels[1].name = 'date'
    df.index.levels[2].name = 'vehicle'

    df, stations = pivot_stations(df, engine)
    df = df.reset_index()
    features = all_features
    for f in categorical_features:
        dummies = pd.get_dummies(df[f], prefix=f)
        for dummy in dummies.columns:
            df[dummy] = dummies[dummy]
            features.append(dummy)
        df = df.drop(f, 1)
        features.remove(f)

    print df

    score_rf, cm = make_predictions_random_forest(df, all_features, target)
    print "Random Forest score without distances : %f" % score_rf
    print "confusion_matrix : \n%s" % cm

    score_rf, cm = make_predictions_random_forest(df, all_features + stations, target)
    print "Random Forest score with distances: %f" % score_rf
    print "confusion_matrix : \n%s" % cm


def pivot_stations(df, engine):
    query = """
    SELECT
      d.*,
      s.name AS arrivalname
    FROM distance d INNER JOIN station s ON d.stationto = s.id
    """
    distances = pd.read_sql_query(query, con=engine)
    stations = distances['arrivalname'].unique().tolist()
    dist_pivot = pd.pivot_table(distances, values='distance', index=['stationfrom', 'date', 'vehicle'],
                                columns=['arrivalname'], aggfunc=np.mean)
    df = df.join(dist_pivot, how='outer')
    return df, stations


make_predictions(ALL_FEATURES, CATEGORICAL_FEATURES, TARGET, CONNECTION_STRING)
