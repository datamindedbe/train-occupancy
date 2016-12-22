CONNECTION_STRING = 'postgresql://krispeeters@localhost:5432/trains'
USER = 'krispeeters'
HOST = 'localhost'
PWD = ''
DB = 'trains'
DATA = '../data'

ALL_FEATURES = [
    'month',
    'day',
    'dow',
    'hour',
    'quarter',
    # 'stationfrom', #departurestop
    # 'arrivalstop',
    # 'vehicle',
    # 'traintype',
    # 'departurename',
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
    'vehicle',
    'departurename',
    'arrivalname',
    'traintype'
]

TARGET = 'delay_category'