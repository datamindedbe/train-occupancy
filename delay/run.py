from delay.delay_config import *
from config import *
from delay.features import build_features
from delay.model import build_model
from delay.predict import predict

build_features(CONNECTION_STRING)
build_model(ALL_FEATURES, CATEGORICAL_FEATURES, TARGET, CONNECTION_STRING, 'model.pkl')
predict(ALL_FEATURES, CATEGORICAL_FEATURES, CONNECTION_STRING, 'model.pkl')
