###DataFrame Preperation for IEX Class

import pandas as pd
import numpy as np
import sklearn as sk
import sys, os
import csv 
from datetime import datetime
import os
'''
There are two main types of files, Stock QUotes File, and Trade File
The Goal of this file if to create a dataframe class that extracts generic
information from the input files.
'''
import os 
my_dir = os.path.dirname(__file__)

def dict_create(input_file):
	"""Creates dictionary from input file"""
	with open(input_file, mode = 'r') as f:
		reader = csv.reader(f)
		mydict = {rows[0]:rows[1] for rows in reader}

	return mydict

def list_from_csv(input_file):
	"""same as the name"""
	with open(input_file, mode = 'r') as f:
		reader = csv.reader(f)
		mylist= [row[0] for row in reader]
	return mylist



class Quote_Wrangler:
	"""
	The Quote_Wrangler class is designed to take in a "quotes" file as downloaded from the TAQ database - and extract
	a time series of National Best Bid/Offer adjustments during a trading day. 
	"""
	def __init__(self,quotes_file):
		'''

		Intitialization -----------------------------------------------------------------------------------------------
		
		The "quotes_file" input must include the file location - reccomendation is to use dynamic referencing as follows:
		"../Training_Files/<filenames>.csv"

		The exchange map is a a transcodification between exchanges/trading venues, and the exchange codes in the TAQ files

		the quotes_columns csv file defines what columns from the TAQ quote file we want to keep - as there are initially quite alot.
		To alter which columns we keep, simply edit the quotes_column file, found in the same location as the Quote_Wrangler class


		'''
		self.my_dir = os.path.abspath(__file__) #directory 
		print("CHECK HERE:",self.my_dir)
		self.quotes_df = pd.read_csv(quotes_file, low_memory = False) #TAQ Quotes file 
		self.exchange_map = dict_create(self.my_dir + '.\exchange_code_dict.csv') #copied from NYSE TAQ Documentation

		#time formatting 
		self.quotes_df['DateTime'] = self.quotes_df['DATE'].map(str) 
		self.quotes_df['DateTime'] = self.quotes_df['DateTime'].apply(lambda x: \
			datetime.strptime(x[:], "%Y%m%d"))
		self.quotes_df['Time'] = self.quotes_df['TIME_M'].apply(lambda x: \
			datetime.strptime(x[:-3], "%H:%M:%S.%f").time().isoformat())

		#list of columns to be used - can edit in file 
		self.quotes_cols = list_from_csv(self.my_dir + '.\quotes_columns.csv')
		self.quotes_df = self.quotes_df[self.quotes_cols]
		self.NB_master = self.NB_combiner()
		# self.NBB  = self.NB_master[self.NB_master.Flag == 'NBB']
		# self.NBO = self.NB_master[self.NB_master.Flag == 'NBO']

	def BBO_series(self):
		"""

		Returns a dataframe that contains ONLY BB0 eligible Quotes as outlined by QU_COND codes 'O','R','Y'.
		For more information see the TAQ Reference guide.  https://www.nyse.com/publicdocs/nyse/data/Daily_TAQ_Client_Spec_v3.0.pdf

		"""
		BBO = self.quotes_df[(self.quotes_df['QU_COND'] == 'O') | (self.quotes_df['QU_COND'] == 'R') | (self.quotes_df['QU_COND'] == 'Y')]
		return BBO 
	

	def NB_combiner(self, exchange_filter = ''):
		"""
	
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

		#Dictionary Initialization 
		ex_bid_price = {k:0 for k in self.exchange_map.keys()} #bid should be more than 0 
		ex_bid_size = ex_bid_price.copy()
		ex_ask_price = {k:10e7 for k in self.exchange_map.keys()} #ask should be less than 10e7 lol 
		ex_ask_size = ex_bid_price.copy()

		master = []
		# cols = ['BID','BIDSIZ','ASK','ASKSIZ']
		prev_best_bid = 0
		prev_best_offer = 10e7 

		prev_bid_total = 0.1 #initialize an amount 
		prev_ask_total = 0.1

		for msg in range(len(filtered_df)): #goingthrough line by line in quotes file 
			#update dictionaries
			ex_bid_price[filtered_df['EX'].iloc[msg]] = float(filtered_df['BID'].iloc[msg]) #update dict
			ex_bid_size[filtered_df['EX'].iloc[msg]] = float(filtered_df['BIDSIZ'].iloc[msg]) #update dict
			ex_ask_price[filtered_df['EX'].iloc[msg]] = float(filtered_df['ASK'].iloc[msg]) #update dict
			if float(filtered_df['ASK'].iloc[msg]) == 0:
				ex_ask_price[filtered_df['EX'].iloc[msg]] = 10e7			
				#this is a little strange but sometimes we will see an ask price of 0 which technically would be the best
				#Ask price in all scenarios. To avoid this, i check if its 0, then replace it with 10e7 if so. 	
			ex_ask_size[filtered_df['EX'].iloc[msg]] = float(filtered_df['ASKSIZ'].iloc[msg]) #update dict

			#Finding the Best Bid and Best Ask 
			itemMaxBid = max(ex_bid_price.items(), key=lambda x: x[1]) #find new max (this is a cool peice of code)
			itemMaxOffer = min(ex_ask_price.items(), key=lambda x: x[1]) #find new min 
			ex_at_nbb = list()
			ex_at_nbo = list()
			# Iterate over all the items in dictionary to find keys (exchanges) with max bid as there can be more than one 
			for key, value in ex_bid_price.items():
				if value == itemMaxBid[1]:
					ex_at_nbb.append(key) #there should always be one max?
			for key, value in ex_ask_price.items():
				if value == itemMaxOffer[1]:
					ex_at_nbo.append(key) #there should always be one max?
			
			bid_vol_total = sum(ex_bid_size[ex] for ex in ex_at_nbb) #total vol - sum over the vols per exchanges at the NBB and NBO 
			ask_vol_total = sum(ex_ask_size[ex] for ex in ex_at_nbo)

			#Now check if there are any changes to either NBO/NBB or volumes at those prices 
			if ((float(itemMaxBid[1]) != float(prev_best_bid)) | (float(itemMaxOffer[1]) != float(prev_best_offer)))| ((bid_vol_total != prev_bid_total) | (ask_vol_total != prev_ask_total)): #need to check changes in vol as well?
				bid = itemMaxBid[1] 
				exchanges_nbb = ex_at_nbb
				bid_vol_by_ex = [ex_bid_size[ex] for ex in ex_at_nbb]
				bid_vol_total = sum(ex_bid_size[ex] for ex in ex_at_nbb)
				
				ask = itemMaxOffer[1] #not sure 
				exhanges_nbo = ex_at_nbo
				ask_vol_by_ex = [ex_ask_size[ex] for ex in ex_at_nbo]
				ask_vol_total = sum(ex_ask_size[ex] for ex in ex_at_nbo)
				
				time = filtered_df['Time'].iloc[msg]

				if ((itemMaxBid[1] != prev_best_bid) | (bid_vol_total != prev_bid_total)):
					flag = "NBB" #either the change was in the NBB side by price or volume 
				else:
					flag = "NBO" #or the change was in the NBO 

				master.append([time,exchanges_nbb,bid_vol_by_ex,bid_vol_total,bid,ask,ask_vol_total,ask_vol_by_ex,exhanges_nbo,flag])
				
				#reset the previous best errthing
				prev_best_bid = bid
				prev_best_offer = ask
				prev_bid_total = bid_vol_total
				prev_ask_total = ask_vol_total

		master_df = pd.DataFrame(master)
		
		master_df.columns = ['Time','B_Exchanges','B_Vol_Ex','B_Vol_Tot','Bid','Ask','A_Vol_Tot','A_Vol_Ex','A_Exchanges','Flag']
		master_df['Spread'] = master_df['Ask'] - master_df['Bid']
		master_df["Mid"] = 0.5*(master_df["Ask"] + master_df['Bid'])
		master_df['Weighted Avg Mid'] = (master_df['Ask']*master_df['A_Vol_Tot'] + master_df['Bid']*master_df['B_Vol_Tot'])/(master_df['A_Vol_Tot'] + master_df['B_Vol_Tot'])
		return master_df

	def exchange_analysis(self,exchange_BBO,NBBO):
		#first find the NBBO at each point in time of the exchange BBO

		exchange_BBO['NBB'] = exchange_BBO.Time.apply(lambda x : NBBO[NBBO.Time <= x].Bid.tail(1))
		# BO = exchange_BBO[exchange_BBO.flag == 'NBB']
		# BB = exchange_BBO[exchange_BBO.flag == 'NBB']

		# for i in 
		return exchange_BBO 

	def create_join_flagger(self,nb_df,nbb_flag = True):
		###not correct - need to work with book by exchange####

		"""either feed in NBO or NBB only dataframes
		nb_df is either NBO or NBB df
		NBB_flag True when using NBB data, False for NBO"""
		temp = nb_df.copy()
		creates = ['']
		joins = ['']
		# fallback = ['']
		if nbb_flag:
			ex_side = 'B_Exchanges'
			side = 'Bid'
			vol = 'B_Vol_Tot'
			vol_ex = 'B_Vol_Ex'
			way = 1 #using this we can switch from < to > when comparing previous levels (< is for Bids, > is for Asks)
		else:
			ex_side = 'A_Exchanges'
			side = 'Ask'
			vol = 'A_Vol_Tot'
			vol_ex = 'A_Vol_Ex'
			way = -1

		for i in range(1,len(nb_df)):
			create_instance = ''
			join_instance = ''
			# fallback = ''

			if way*nb_df.iloc[i][side] > way*nb_df.iloc[i-1][side]:
				create_instance = dict(zip(nb_df.iloc[i][ex_side],nb_df.iloc[i][vol_ex]))
			elif (nb_df.iloc[i][side] == nb_df.iloc[i-1][side]):
				prev_status_dict = dict(zip(nb_df.iloc[i-1][ex_side],nb_df.iloc[i-1][vol_ex]))
				current_status_dict = dict(zip(nb_df.iloc[i][ex_side],nb_df.iloc[i][vol_ex]))
				join_instance = {k:v for k,v in current_status_dict.items() if k not in prev_status_dict.keys()}

				# joining_exchanges = [ex for ex in nb_df.iloc[i][ex_side] if ex not in nb_df.iloc[i-1][ex_side]]
				# joining_amount = float(nb_df.iloc[i][vol]) - float(nb_df.iloc[i-1][vol])
				# join_instance = [joining_exchanges,joining_amount]

			creates.append(create_instance)
			joins.append(join_instance)

		temp['Creates'] = creates
		temp['Joins'] = joins

		return temp



	# def NBO_combiner(self): #OLD VERSION DONT SCREW THIS ONE UP 
	# 	filtered_df = self.BBO_series()
	# 	#[bid,bid_size,ask,ask_size]
	# 	ex_bid_price = {k:0 for k in self.exchange_map.keys()}
	# 	ex_bid_size = ex_bid_price.copy()
	# 	ex_ask_price = ex_bid_price.copy()
	# 	ex_ask_size = ex_bid_price.copy()
	# 	master = []
	# 	# cols = ['BID','BIDSIZ','ASK','ASKSIZ']
	# 	cols = ['BID']
	# 	prev_best_bid = 0
	# 	for msg in range(len(filtered_df)):
	# 		#update dictionaries
	# 		ex_bid_price[filtered_df['EX'].iloc[msg]] = float(filtered_df['BID'].iloc[msg]) #update dict
	# 		ex_bid_size[filtered_df['EX'].iloc[msg]] = float(filtered_df['BIDSIZ'].iloc[msg]) #update dict
	# 		ex_ask_price[filtered_df['EX'].iloc[msg]] = float(filtered_df['ASK'].iloc[msg]) #update dict
	# 		ex_ask_size[filtered_df['EX'].iloc[msg]] = float(filtered_df['ASKSIZ'].iloc[msg]) #update dict
	# 		itemMaxBid = max(ex_bid_price.items(), key=lambda x: x[1]) #find new max 
	# 		ex_at_nbb = list()
	# 		# Iterate over all the items in dictionary to find keys with max value
	# 		for key, value in ex_bid_price.items():
	# 			if value == itemMaxBid[1]:
	# 				ex_at_nbb.append(key) #there should always be one max?
	# 		if itemMaxBid[1] != prev_best_bid:
	# 			best_bid = itemMaxValue[1]
	# 			bid_vol = sum(ex_bid_size[ex] for ex in ex_at_nbb)
	# 			best_ask = ex_ask_price[ex_at_nbb[0]] #not sure 
	# 			ask_vol = sum(ex_ask_size[ex] for ex in ex_at_nbb) #notsure 
	# 			exchanges = ex_at_nbb
	# 			time = filtered_df['Time'].iloc[msg]
	# 			master.append([time,exchanges,best_bid,bid_vol,best_ask,ask_vol])
	# 			prev_best_bid = best_bid
	# 	master_df = pd.DataFrame(master)
	# 	master_df.columns = ['Time','Exchanges','National Best Bid','Bid Size Total', 'Best Ask', 'Ask Vol']
	# 	return master_df


"""---------------------TRADE WRANGLER CLASS------------------------------------"""

class Trade_Wrangler():
	def __init__(self,trade_file):
		self.trades = pd.read_csv(trade_file)
		self.my_dir = my_dir
		self.exchange_map = dict_create(self.my_dir + '.\exchange_code_dict.csv')
		self.trades['DateTime'] = self.trades['DATE'].map(str)
		self.trades['DateTime'] = self.trades['DateTime'].apply(lambda x: \
			datetime.strptime(x[:], "%Y%m%d"))
		self.trades['Time'] = self.trades['TIME_M'].apply(lambda x: \
			datetime.strptime(x[:-3], "%H:%M:%S.%f").time().isoformat())
		self.columns = ['DateTime','Time','EX','SYM_ROOT','TR_SCOND','SIZE','PRICE','TR_CORR','TR_SOURCE','TR_RF']
		self.trades = self.trades[self.columns]


	def trade_finder(self,time,num_trades,time_after = 0):
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
			new_time = (datetime.strptime(time[:-3], "%H:%M:%S.%f") +  pd.Timedelta(seconds = time_after)).time().isoformat()
			return self.trades[(self.trades.Time > time) & (self.trades.Time < new_time)]

	def volume_finder(self,time,num_trades,time_after = 0):
		"""
		Returns volume of trades by either number of trades after, or time after a specified start time
		"""
		trades = self.trade_finder(time,num_trades,time_after)
		return sum(trades['SIZE']) 


def main():
	# print(dict_create('./exchange_code_dict.csv'))
	# print(list_from_csv('./quotes_columns.csv'))
	quotes_file = '../Training_Files/AAPL.1.4.18_training.csv'
	q = Quote_Wrangler(quotes_file)
	NBBO = q.NB_combiner
	ARCA = q.NB_combiner(exchange_filter = ['P'])
	test = q.exchange_analysis(ARCA,NBBO)
	return test

if __name__== '__main__':
	outcome = main()
	
