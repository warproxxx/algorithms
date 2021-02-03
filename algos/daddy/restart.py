import os

if os.path.isfile('data/features.csv'):
    os.remove('data/features.csv')

from arctic import Arctic

store = Arctic('localhost')
library = store['daddy']
library.delete('trades')