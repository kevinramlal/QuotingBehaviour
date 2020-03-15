# DataFrame Preperation for IEX Class
# DataFrame Preperation for IEX Class
from collections import defaultdict
import pandas as pd
import numpy as np
import sklearn as sk
import sys
import os
import csv
from datetime import datetime,timedelta

'''
There are two main types of files, Stock QUotes File, and Trade File
The Goal of this file if to create a dataframe class that extracts generic
information from the input files.
'''


my_dir = os.path.dirname(__file__)


def dict_create(input_file):
    """Creates dictionary from input file"""
    with open(input_file, mode='r') as f:
        reader = csv.reader(f)
        mydict = {rows[0]: rows[1] for rows in reader}

    return mydict


def list_from_csv(input_file):
    """same as the name"""
    with open(input_file, mode='r') as f:
        reader = csv.reader(f)
        mylist = [row[0] for row in reader]
    return mylist


class Quote_Wrangler:
    """
    The Quote_Wrangler class is designed to take in a "quotes" file as downloaded from the TAQ database - and extract
    a time series of National Best Bid/Offer adjustments during a trading day.
    """

    def __init__(self, quotes_file):
        '''

        Intitialization -----------------------------------------------------------------------------------------------

        The "quotes_file" input must include the file location - reccomendation is to use dynamic referencing as follows:
        "../Training_Files/<filenames>.csv"

        The exchange map is a a transcodification between exchanges/trading venues, and the exchange codes in the TAQ files

        the quotes_columns csv file defines what columns from the TAQ quote file we want to keep - as there are initially quite alot.
        To alter which columns we keep, simply edit the quotes_column file, found in the same location as the Quote_Wrangler class


        '''
        self.my_dir = my_dir
        self.quotes_df = pd.read_csv(quotes_file, low_memory=False)  # TAQ Quotes file
        self.exchange_map = dict_create(self.my_dir + '/exchange_code_dict.csv')  # copied from NYSE TAQ Documentation

        # time formatting
        self.quotes_df['Datetime'] = pd.to_datetime(self.quotes_df['DATE'].astype('str') + " " + self.quotes_df["TIME_M"], format="%Y%m%d %H:%M:%S.%f")
        self.quotes_df['Date'] = self.quotes_df['Datetime'].dt.date
        self.quotes_df['Time'] = self.quotes_df['Datetime'].dt.time

        # self.quotes_df['Time'] = self.quotes_df['Time'].apply(lambda x: str(x.time()))  # gets just the time

        # list of columns to be used - can edit in file
        self.quotes_cols = list_from_csv(self.my_dir + '/quotes_columns.csv')
        self.quotes_df = self.quotes_df[self.quotes_cols]
        self.NB_master = self.NB_combiner()

    # self.NBB  = self.NB_master[self.NB_master.Flag == 'NBB']
    # self.NBO = self.NB_master[self.NB_master.Flag == 'NBO']

    def BBO_series(self):
        """

        Returns a dataframe that contains ONLY BB0 eligible Quotes as outlined by QU_COND codes 'O','R','Y'.
        For more information see the TAQ Reference guide.  https://www.nyse.com/publicdocs/nyse/data/Daily_TAQ_Client_Spec_v3.0.pdf

        """
        BBO = self.quotes_df[(self.quotes_df['QU_COND'] == 'O') | (self.quotes_df['QU_COND'] == 'R') | (
            self.quotes_df['QU_COND'] == 'Y')]
        return BBO

    def NB_combiner(self, exchange_filter=''):
        """
        #Ready for Cythonification
        Core function that breaks out quotes that adjust something related to either the National Best Bid or the
        National Best Offer, whether that be the actual price of the NBB/NBO or the quantity at the current NBB/NBO.

        Using this function we can determine when exchanges join the NBB/NBO or create.

        Exchange filter should be list - allows you to filter this process for a specific exchange
        """
        if exchange_filter == '':
            filtered_df = self.BBO_series()
        else:
            temp = self.BBO_series()
            filtered_df = temp[temp.EX.isin(exchange_filter)]

        # Dictionary Initialization
        ex_bid_price = {k: 0 for k in self.exchange_map.keys()}  # bid should be more than 0
        ex_bid_size = ex_bid_price.copy()
        ex_ask_price = {k: 10e7 for k in self.exchange_map.keys()}  # ask should be less than 10e7 lol
        ex_ask_size = ex_bid_price.copy()

        master = []
        # cols = ['BID','BIDSIZ','ASK','ASKSIZ']
        prev_best_bid = 0
        prev_best_offer = 10e7

        prev_bid_total = 0.1  # initialize an amount
        prev_ask_total = 0.1

        filtered_df['ASKSIZ'] = filtered_df['ASKSIZ'].astype(np.float32)
        filtered_df['BIDSIZ'] = filtered_df['BIDSIZ'].astype(np.float32)

        for msg in filtered_df.itertuples():  # goingthrough line by line in quotes file
            ex = msg.EX
            # update dictionaries
            ex_bid_price[ex] = msg.BID  # update dict
            ex_bid_size[ex] = msg.BIDSIZ  # update dict
            ex_ask_price[ex] = msg.ASK  # update dict
            if msg.ASK == 0:
                ex_ask_price[ex] = 10e7
                # this is a little strange but sometimes we will see an ask price of 0 which technically would be the best
                # Ask price in all scenarios. To avoid this, i check if its 0, then replace it with 10e7 if so.
            ex_ask_size[ex] = msg.ASKSIZ  # update dict

            # Finding the Best Bid and Best Ask
            itemMaxBid = max(ex_bid_price.items(), key=lambda x: x[1])  # find new max (this is a cool peice of code)
            itemMaxOffer = min(ex_ask_price.items(), key=lambda x: x[1])  # find new min
            ex_at_nbb = list()
            ex_at_nbo = list()
            # Iterate over all the items in dictionary to find keys (exchanges) with max bid as there can be more than one
            for key, value in ex_bid_price.items():
                if value == itemMaxBid[1]:
                    ex_at_nbb.append(key)  # there should always be one max?
            for key, value in ex_ask_price.items():
                if value == itemMaxOffer[1]:
                    ex_at_nbo.append(key)  # there should always be one max?

            bid_vol_total = sum(
                ex_bid_size[ex] for ex in ex_at_nbb)  # total vol - sum over the vols per exchanges at the NBB and NBO
            ask_vol_total = sum(ex_ask_size[ex] for ex in ex_at_nbo)

            # Now check if there are any changes to either NBO/NBB or volumes at those prices
            if ((float(itemMaxBid[1]) != float(prev_best_bid)) | (float(itemMaxOffer[1]) != float(prev_best_offer))) | (
                    (bid_vol_total != prev_bid_total) | (
                    ask_vol_total != prev_ask_total)):  # need to check changes in vol as well?
                bid = itemMaxBid[1]
                exchanges_nbb = ex_at_nbb
                bid_vol_by_ex = [ex_bid_size[ex] for ex in ex_at_nbb]
                bid_vol_total = sum(ex_bid_size[ex] for ex in ex_at_nbb)

                ask = itemMaxOffer[1]  # not sure
                exhanges_nbo = ex_at_nbo
                ask_vol_by_ex = [ex_ask_size[ex] for ex in ex_at_nbo]
                ask_vol_total = sum(ex_ask_size[ex] for ex in ex_at_nbo)

                time = msg.Time

                if ((itemMaxBid[1] != prev_best_bid) | (bid_vol_total != prev_bid_total)):
                    flag = "NBB"  # either the change was in the NBB side by price or volume
                else:
                    flag = "NBO"  # or the change was in the NBO

                master.append(
                    [time, exchanges_nbb, bid_vol_by_ex, bid_vol_total, bid, ask, ask_vol_total, ask_vol_by_ex,
                     exhanges_nbo, flag])

                # reset the previous best errthing
                prev_best_bid = bid
                prev_best_offer = ask
                prev_bid_total = bid_vol_total
                prev_ask_total = ask_vol_total

        master_df = pd.DataFrame(master)

        master_df.columns = ['Time', 'B_Exchanges', 'B_Vol_Ex', 'B_Vol_Tot', 'Bid', 'Ask', 'A_Vol_Tot', 'A_Vol_Ex',
                             'A_Exchanges', 'Flag']
        master_df['Spread'] = master_df['Ask'] - master_df['Bid']
        master_df["Mid"] = 0.5 * (master_df["Ask"] + master_df['Bid'])
        master_df['Weighted Avg Mid'] = (master_df['Ask'] * master_df['A_Vol_Tot'] + master_df['Bid'] * master_df[
            'B_Vol_Tot']) / (master_df['A_Vol_Tot'] + master_df['B_Vol_Tot'])
        return master_df

    def exchange_analysis(self, exchange_BBO, NBBO):
        # first find the NBBO at each point in time of the exchange BBO
        nbb_list = []
        nbo_list = []
        for i in range(len(exchange_BBO)):
            time = exchange_BBO.Time.iloc[i]
            nbbo = NBBO[NBBO.Time <= time].tail(1)
            nbb = float(nbbo.Bid)
            nbo = float(nbbo.Ask)
            nbb_list.append(nbo)

        exchange_BBO['NBB'] = exchange_BBO.Time.apply(lambda x: NBBO[NBBO.Time <= x].Bid.tail(1))
        # BO = exchange_BBO[exchange_BBO.flag == 'NBB']
        # BB = exchange_BBO[exchange_BBO.flag == 'NBB']

        # for i in
        return exchange_BBO

    def cj_flagger(self, nbb_flag=True):
        """
        Function that identifies instances of creation/joining.
        nb_df - either the NBB only or NBO only dataframe
        nbb_flag - True if using NBB or False for NBO
        """
        create_master = ['']
        join_master = ['']
        cols = self.NB_master.columns
        if nbb_flag:
            nb_df = self.NB_master[self.NB_master.Flag == 'NBB']
            ex_side = 'B_Exchanges'
            side = 'Bid'
            vol = 'B_Vol_Tot'
            vol_ex = 'B_Vol_Ex'

        else:
            nb_df = self.NB_master[self.NB_master.Flag == 'NBO']
            ex_side = 'A_Exchanges'
            side = 'Ask'
            vol = 'A_Vol_Tot'
            vol_ex = 'A_Vol_Ex'

        ex_side_idx = np.where(cols == ex_side)[0][0]
        side_idx = np.where(cols == side)[0][0]
        vol_idx = np.where(cols == vol)[0][0]
        vol_ex_idx = np.where(cols == vol_ex)[0][0]

        start_price = nb_df.iloc[0][side]
        for i, cur_line in enumerate(nb_df.itertuples(index=False)):
            cur_price = cur_line[side_idx]
            if i == 0:
                prev_line = cur_line
                continue
            if cur_price != start_price:
                start_price = cur_price
                prev_line = cur_line
                create_master.append('')
                join_master.append('')
                continue
            prev_state = dict(zip(prev_line[ex_side_idx], prev_line[vol_ex_idx]))
            cur_state = dict(zip(cur_line[ex_side_idx], cur_line[vol_ex_idx]))

            creates = {i: v for i, v in cur_state.items() if i not in prev_state.keys()}
            joins = {i: (cur_state[i] - prev_state[i]) for i in cur_state.keys() if
                     (i in prev_state.keys()) and ((cur_state[i] - prev_state[i]) > 0)}

            if creates == {}:
                creates = ''
            if joins == {}:
                joins = ''

            create_master.append(creates)
            join_master.append(joins)
            prev_line = cur_line

        nb_df['Creates'] = create_master
        nb_df['Joins'] = join_master

        return nb_df

    def get_mid_quote(self, shift: int=0):
        """
        Get mid quote price given the consolideted order book.
        :param shift: int, time shift by seconds.
        :return res: series of mid quote price
        
        
        Kevin Notes: Good for getting current mid-price, but takes a while for getting shifted mid-price -> using another function that makes use of current mid-price. 
        """
        if shift == 0:
            res = pd.Series(data=((self.quotes_df['BID'] + self.quotes_df['ASK']) / 2).values, index=self.quotes_df['Datetime'], name='mid-quote')
        else:
            # find the first tick after n seconds
            res = pd.Series(index=self.quotes_df['Datetime'], name='mid-quote')
            dt = self.quotes_df['Datetime']
            # Loop is a bit slow, but still musch faster than apply. Feel free to optimize this part.
            for i, t in dt.items():
                ind = (((dt[i:(i + shift * 1000)] - t).dt.total_seconds() // shift) > 0).idxmax()  # assume the target is in the next 1000 * shift rows
                res.iloc[i] = self.quotes_df.loc[ind, ['ASK', 'BID']].sum() / 2
            res.iloc[1 - shift:] = np.nan
        return res

"""--------------------------Helper Functions for Probabilitiy----------------------"""
def get_next_mid(nb_df):
    """
    Takes in df with MID, and returns same df with MID_next
    Returns same df with column for next change in mid price 
    """
    original_cols = list(nb_df.columns)
    original_cols += ['Mid_Next']
    
    mid_changes = nb_df.loc[nb_df.Mid.shift(1) != nb_df.Mid][['Mid']]
    nb_df = pd.merge(nb_df, mid_changes, how='left',left_index = True, right_index = True)
    nb_df.Mid_y = nb_df.Mid_y.fillna(method = 'bfill').shift(-1)
    nb_df.columns = original_cols
    return nb_df 

def probability_master_func(QW,exch):
    """
    Takes in NBBO object db and spits out prob db
    Example input: <Quote_Wrangler>.NB_master
    
    exch should be list of exchange codes.
    For all exchanges - use list(<Quote_Wrangler>.exchange_map.keys())
    """
    NBB_all = QW.cj_flagger(nbb_flag = True)    
    NBO_all = QW.cj_flagger(nbb_flag = False)
    NBBO_cj_Master = NBB_all.append(NBO_all)
    NBBO_cj_Master = NBBO_cj_Master.sort_values(by = 'Time')
    NBX = get_next_mid(NBBO_cj_Master)
    
    NBX['Mid_Change'] = NBX.Mid_Next - NBX.Mid
    
    prob_matrix = np.empty([len(exch),4])
    NBX_BB = NBX[NBX.Flag == 'NBB']
    NBX_BO = NBX[NBX.Flag == 'NBO']
    for i in range(len(exch)):
        ex = exch[i]
        NBX_BB_ex = NBX_BB.copy()
        NBX_BO_ex = NBX_BO.copy()
        
        NBX_BB_ex['Ex_Flag'] =  NBX_BB_ex.B_Exchanges.apply(lambda x :ex in x)
        NBX_BB_ex = NBX_BB_ex[NBX_BB_ex.Ex_Flag == True]

        NBX_BO_ex['Ex_Flag'] =  NBX_BO_ex.A_Exchanges.apply(lambda x :ex in x)
        NBX_BO_ex = NBX_BO_ex[NBX_BO_ex.Ex_Flag == True]
            
        NBX_BB_ex_C = NBX_BB_ex[NBX_BB_ex.Creates != '']
        NBX_BB_ex_J = NBX_BB_ex[NBX_BB_ex.Joins != '']

        NBX_BO_ex_C = NBX_BO_ex[NBX_BO_ex.Creates != '']
        NBX_BO_ex_J = NBX_BO_ex[NBX_BO_ex.Joins != '']
        
        try:
            prob_BB_c = sum(np.array(NBX_BB_ex_C.Mid_Change > 0))/len(NBX_BB_ex_C)
        except:
            prob_BB_c = 0
        try:
            prob_BB_j = sum(np.array(NBX_BB_ex_J.Mid_Change > 0))/len(NBX_BB_ex_J)
        except:
            prob_BB_j = 0
        try:
            prob_BO_c = sum(np.array(NBX_BO_ex_C.Mid_Change > 0))/len(NBX_BO_ex_C)
        except:
            prob_BO_c = 0
        try:
            prob_BO_j = sum(np.array(NBX_BO_ex_J.Mid_Change > 0))/len(NBX_BO_ex_J)
        except:
            prob_BO_j = 0
            
            
        prob_matrix[i,0] = prob_BB_c
        prob_matrix[i,1] = prob_BB_j
        prob_matrix[i,2] = prob_BO_c
        prob_matrix[i,3] = prob_BO_j
    prob_df = pd.DataFrame(prob_matrix)
    prob_df.index = exch
    prob_df.columns = ['Best_Bid_Creates','Best_Bid_Joins','Best_Offer_Creates','Best_Offer_Joins']
    return prob_df
    
    

"""---------------------TRADE WRANGLER CLASS------------------------------------"""


class Trade_Wrangler():
    def __init__(self, trade_file):
        self.trades = pd.read_csv(trade_file)
        self.my_dir = my_dir
        self.exchange_map = dict_create(self.my_dir + '.\exchange_code_dict.csv')
        self.trades['DateTime'] = self.trades['DATE'].map(str)
        self.trades['DateTime'] = self.trades['DateTime'].apply(lambda x:
                                                                datetime.strptime(x[:], "%Y%m%d"))
        self.trades['Time'] = self.trades['TIME_M'].apply(lambda x:
                                                          datetime.strptime(x[:-3],
                                                                            "%H:%M:%S.%f").time().isoformat())
        self.columns = ['DateTime', 'Time', 'EX', 'SYM_ROOT', 'TR_SCOND', 'SIZE', 'PRICE', 'TR_CORR', 'TR_SOURCE',
                        'TR_RF']
        self.trades = self.trades[self.columns]

    def trade_finder(self, time, num_trades, time_after=0):
        """return db of trades executed after a specified time.
        Either take x num of trades after or grab all trades that executed up to a certain number
        of miutes after

        time - should be taken from Time column in quotes db
        num_trades - integer
        time_after - should be in second (note that like 1 minute will yeild thousands of results lol)

        """
        if time_after == 0:
            return self.trades[self.trades.Time > time].head(num_trades)
        else:
            new_time = (datetime.strptime(time[:-3], "%H:%M:%S.%f") + pd.Timedelta(
                seconds=time_after)).time().isoformat()
            return self.trades[(self.trades.Time > time) & (self.trades.Time < new_time)]

    def volume_finder(self, time, num_trades, time_after=0):
        """
        Returns volume of trades by either number of trades after, or time after a specified start time
        """
        trades = self.trade_finder(time, num_trades, time_after)
        return sum(trades['SIZE'])


# ------------Misc Helper Functions---------------------------------------

def cj_count(cj_count):
        # counts cases of creates and joins given a NBB/NBO CJ DB
    create_count_dict = defaultdict(int)
    join_count_dict = defaultdict(int)
    for i, line in enumerate(cj_count.itertuples(index=False)):
        try:
            ex = list(line.Creates.items())[0][0]
            create_count_dict[ex] += 1
        except:
            try:
                ex = list(line.Joins.items())[0][0]
                join_count_dict[ex] += 1
            except:
                continue
    return create_count_dict, join_count_dict

# -------------------------------------------------------

# ------------ Probability Analysis Functions-----------------








def main():
    # print(dict_create('./exchange_code_dict.csv'))
    # print(list_from_csv('./quotes_columns.csv'))
    quotes_file = '../Training_Files/AAPL.1.4.18_training.csv'
    q = Quote_Wrangler(quotes_file)
    NBBO = q.NB_combiner
    ARCA = q.NB_combiner(exchange_filter=['P'])
    test = q.exchange_analysis(ARCA, NBBO)
    return test


if __name__ == '__main__':
    outcome = main()
