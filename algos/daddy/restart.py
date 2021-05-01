import os

if os.path.isfile('data/XBT_features.csv'):
    os.remove('data/XBT_features.csv')

from arctic import Arctic

store = Arctic('localhost')
library = store['daddy']
library.delete('XBT_trades')