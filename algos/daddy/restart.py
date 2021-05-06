import os

if os.path.isfile('data/XBT_features.csv'):
    os.remove('data/XBT_features.csv')

if os.path.isfile('data/features.csv'):
    os.remove('data/features.csv')

from arctic import Arctic

store = Arctic('localhost')
library = store['daddy']

try:
    library.delete('XBT_trades')
except:
    pass

try:
    library.delete('trades')
except:
    pass