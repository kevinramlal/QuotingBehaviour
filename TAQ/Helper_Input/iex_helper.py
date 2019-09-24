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


class quote_wrangler:
	def __init__(self,quotes_file,dir = my_dir):
		'''
		quotes_file - csv file location
		trades_file - csv file location
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

	def BBO_series(self):
		"""
		Returns a dataframe that contains ONLY BBO eligible Quotes
		"""
		temp = self.quotes_df[(self.quotes_df['QU_COND'] == 'O') | (self.quotes_df['QU_COND'] == 'R') | (self.quotes_df['QU_COND'] == 'Y')]
		return temp 
	

	def NBB_combiner(self):
		filtered_df = self.BBO_series()
		#[bid,bid_size,ask,ask_size]
		ex_bid_price = {k:0 for k in self.exchange_map.keys()}
		ex_bid_size = ex_bid_price.copy()
		ex_ask_price = ex_bid_price.copy()
		ex_ask_size = ex_bid_price.copy()
		master = []
		# cols = ['BID','BIDSIZ','ASK','ASKSIZ']
		cols = ['BID']
		prev_best_bid = 0
		for msg in range(len(filtered_df)):
			#update dictionaries
			ex_bid_price[filtered_df['EX'].iloc[msg]] = float(filtered_df['BID'].iloc[msg]) #update dict
			ex_bid_size[filtered_df['EX'].iloc[msg]] = float(filtered_df['BIDSIZ'].iloc[msg]) #update dict
			ex_ask_price[filtered_df['EX'].iloc[msg]] = float(filtered_df['ASK'].iloc[msg]) #update dict
			ex_ask_size[filtered_df['EX'].iloc[msg]] = float(filtered_df['ASKSIZ'].iloc[msg]) #update dict
			itemMaxValue = max(ex_bid_price.items(), key=lambda x: x[1]) #find new max 
			ex_at_nbb = list()
			# Iterate over all the items in dictionary to find keys with max value
			for key, value in ex_bid_price.items():
				if value == itemMaxValue[1]:
					ex_at_nbb.append(key) #there should always be one max?
			if itemMaxValue[1] != prev_best_bid:
				best_bid = itemMaxValue[1]
				bid_vol = sum(ex_bid_size[ex] for ex in ex_at_nbb)
				best_ask = ex_ask_price[ex_at_nbb[0]] #not sure 
				ask_vol = sum(ex_ask_size[ex] for ex in ex_at_nbb) #notsure 
				exchanges = ex_at_nbb
				time = filtered_df['Time'].iloc[msg]
				master.append([time,exchanges,best_bid,bid_vol,best_ask,ask_vol])
				prev_best_bid = best_bid
		master_df = pd.DataFrame(master)
		master_df.columns = ['Time','Exchanges','NBB','Bid Size Total', 'Best Ask', 'Ask Vol']
		return master_df


def main():
	# print(dict_create('./exchange_code_dict.csv'))
	# print(list_from_csv('./quotes_columns.csv'))
	quotes_file = '../AAPL.1.4.18_training.csv'
	q = quote_wrangler(quotes_file)
	q2 = q.exchange_combiner()
	return q2

if __name__== '__main__':
	q = main()
