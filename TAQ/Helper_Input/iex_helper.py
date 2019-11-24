###DataFrame Preperation for IEX Class

import pandas as pd
import numpy as np
import sklearn as sk
import matplotlib.pyplot as plt
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
	with open(input_file, mode = 'r') as f:
		reader = csv.reader(f)
		mydict = {rows[0]:rows[1] for rows in reader}

	return mydict

def list_from_csv(input_file):
	with open(input_file, mode = 'r') as f:
		reader = csv.reader(f)
		mylist= [row[0] for row in reader]
	return mylist



class Quote_Wrangler:
	"""
	The Quote_Wrangler class is designed to take in a "quotes" file as downloaded from the TAQ database - and extract
	a time series of National Best Bid/Offer adjustments during a trading day. 
	"""
	def __init__(self,quotes_file,dir = my_dir):
		'''

		Intitialization -----------------------------------------------------------------------------------------------
		
		The "quotes_file" input must include the file location - reccomendation is to use dynamic referencing as follows:
		"../Training_Files/<filenames>.csv"

		The exchange map is a a transcodification between exchanges/trading venues, and the exchange codes in the TAQ files

		the quotes_columns csv file defines what columns from the TAQ quote file we want to keep - as there are initially quite alot.
		To alter which columns we keep, simply edit the quotes_column file, found in the same location as the Quote_Wrangler class


		'''
		self.my_dir = my_dir
		self.quotes_df = pd.read_csv(quotes_file, low_memory = False)
		self.exchange_map = dict_create(self.my_dir + '.\exchange_code_dict.csv')
		self.quotes_df['DateTime'] = self.quotes_df['DATE'].map(str)
		self.quotes_df['DateTime'] = self.quotes_df['DateTime'].apply(lambda x: \
			datetime.strptime(x[:], "%Y%m%d"))
		self.quotes_df['Time'] = self.quotes_df['TIME_M'].apply(lambda x: \
			datetime.strptime(x[:-3], "%H:%M:%S.%f").time().isoformat())
		self.quotes_cols = list_from_csv(self.my_dir + '.\quotes_columns.csv')
		self.quotes_df = self.quotes_df[self.quotes_cols]
		self.NB_master = self.NB_combiner()
		self.NBB  = self.NB_master[self.NB_master.Flag == 'NBB']
		self.NBO = self.NB_master[self.NB_master.Flag == 'NBO']

	def BBO_series(self):
		"""

		Returns a dataframe that contains ONLY BB0 eligible Quotes as outlined by QU_COND codes 'O','R','Y'.
		For more information see the TAQ Reference guide.  https://www.nyse.com/publicdocs/nyse/data/Daily_TAQ_Client_Spec_v3.0.pdf

		"""
		temp = self.quotes_df[(self.quotes_df['QU_COND'] == 'O') | (self.quotes_df['QU_COND'] == 'R') | (self.quotes_df['QU_COND'] == 'Y')]
		return temp 
	

	def NB_combiner(self):
		"""

		Core function that breaks out quotes that adjust something related to either the National Best Bid or the 
		National Best Offer, whether that be the actual price of the NBB/NBO or the quantity at the current NBB/NBO.

		Using this function we can determine when exchanges join the NBB/NBO or create.
		"""
		filtered_df = self.BBO_series()
		#[bid,bid_size,ask,ask_size]
		ex_bid_price = {k:0 for k in self.exchange_map.keys()}
		ex_bid_size = ex_bid_price.copy()
		ex_ask_price = {k:10e7 for k in self.exchange_map.keys()}
		ex_ask_size = ex_bid_price.copy()
		master = []
		# cols = ['BID','BIDSIZ','ASK','ASKSIZ']
		prev_best_bid = 0
		prev_best_offer = 10000000000

		prev_bid_total = 0
		prev_ask_total = 0
		for msg in range(len(filtered_df)):
			#update dictionaries
			ex_bid_price[filtered_df['EX'].iloc[msg]] = float(filtered_df['BID'].iloc[msg]) #update dict
			ex_bid_size[filtered_df['EX'].iloc[msg]] = float(filtered_df['BIDSIZ'].iloc[msg]) #update dict
			ex_ask_price[filtered_df['EX'].iloc[msg]] = float(filtered_df['ASK'].iloc[msg]) #update dict
			ex_ask_size[filtered_df['EX'].iloc[msg]] = float(filtered_df['ASKSIZ'].iloc[msg]) #update dict
			itemMaxBid = max(ex_bid_price.items(), key=lambda x: x[1]) #find new max 
			itemMaxOffer = min(ex_ask_price.items(), key=lambda x: x[1]) #find new min 
			ex_at_nbb = list()
			ex_at_nbo = list()
			# Iterate over all the items in dictionary to find keys with max bid
			for key, value in ex_bid_price.items():
				if value == itemMaxBid[1]:
					ex_at_nbb.append(key) #there should always be one max?
			for key, value in ex_ask_price.items():
				if value == itemMaxOffer[1]:
					ex_at_nbo.append(key) #there should always be one max?
			
			bid_vol_total = sum(ex_bid_size[ex] for ex in ex_at_nbb)
			ask_vol_total = sum(ex_ask_size[ex] for ex in ex_at_nbo)

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
				if (itemMaxBid[1] != prev_best_bid):
					flag = "NBB"
				else:
					flag = "NBO"
				master.append([time,exchanges_nbb,bid_vol_by_ex,bid_vol_total,bid,ask,ask_vol_total,ask_vol_by_ex,exhanges_nbo,flag])
				prev_best_bid = bid
				prev_best_offer = ask
				prev_bid_total = bid_vol_total
				prev_ask_total = ask_vol_total

		master_df = pd.DataFrame(master)
		master_df.columns = ['Time','B_Exchanges','B_Vol_Ex','B_Vol_Tot','Bid','Ask','A_Vol_Tot','A_Vol_Ex','A_Exchanges','Flag']
		return master_df

	def create_join_flagger(self,nb_df,nbb_flag = True):
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

			if way*nb_df.iloc[i][side] < way*nb_df.iloc[i-1][side]:
				create_instance = dict(zip(nb_df.iloc[i][ex_side],nb_df.iloc[i][vol_ex]))
			elif (nb_df.iloc[i][side] == nb_df.iloc[i-1][side]):
				prev_status_dict = dict(zip(nb_df.iloc[i-1][ex_side],nb_df.iloc[i-1][vol_ex]))
				current_status_dict = dict(zip(nb_df.iloc[i][ex_side],nb_df.iloc[i][vol_ex]))
				join_instance = {k:v for k in current_status_dict.keys() if k not in prev_status_dict.keys()}

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


def main():
	# print(dict_create('./exchange_code_dict.csv'))
	# print(list_from_csv('./quotes_columns.csv'))
	quotes_file = '../AAPL.1.4.18_training.csv'
	q = quote_wrangler(quotes_file)
	q2 = q.exchange_combiner()
	return q2

if __name__== '__main__':
	q = main()
