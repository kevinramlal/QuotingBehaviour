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
		Returns a dataframe that contains ONLY BBOs
		"""
		#FIRST filter only qu_conditions for O ,R, or Y
		# temp = self.quotes_df[self.quotes_df['QU_COND'] in ('O','R','Y')]
		return self.quotes_df[(self.quotes_df['NATBBO_IND'] ==4) and (self.quotes_df['NATBBO_IND'] ==2)]


def main():
	# print(dict_create('./exchange_code_dict.csv'))
	# print(list_from_csv('./quotes_columns.csv'))
	quotes_file = '../AAPL.1.5.18.csv'
	trades_file = '../Trades1.5.18.csv'
	return stock_data(quotes_file,trades_file)

if __name__== '__main__':
	main()