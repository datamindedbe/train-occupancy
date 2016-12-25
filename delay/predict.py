from sklearn.externals import joblib
from sqlalchemy import create_engine
import pandas as pd
from config import CONNECTION_STRING
from delay_config import ALL_FEATURES, CATEGORICAL_FEATURES

def predict(all_features, categorical_features, connection_string, filename):
    engine = create_engine(connection_string)
    model = joblib.load(filename)
    print model

    query = """SELECT * FROM connection_features"""
    df = pd.read_sql_query(query, con=engine, index_col=['departurestop', 'departuredate', 'route'])
    df.index.levels[0].name = 'stationfrom'
    df.index.levels[1].name = 'date'
    df.index.levels[2].name = 'vehicle'
    df = df.reset_index()

    predicted = model.predict(df[all_features])
    print predicted


predict(ALL_FEATURES, CATEGORICAL_FEATURES, CONNECTION_STRING, 'model.pkl')