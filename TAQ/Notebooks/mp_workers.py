import sys
sys.path.insert(0, "../Helper_Input")
from iex_helper import *

def calc_prob_matrix(path):
    QW = Quote_Wrangler(path)
    exch = 'A B C D I J K M N P S T Q V W X Y Z'.split()
    prob_db = probability_master_func(QW, exch)
    prob_db['Date'] = QW.quotes_df.iloc[0]['Date']
    prob_db['Symbol'] = QW.quotes_df.iloc[0]['SYM_ROOT']
    prob_db.to_csv('probability_results.csv', mode='a', header=False)
    return

