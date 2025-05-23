from datetime import datetime, timedelta
import pandas as pd
import yaml
import os.path

from .dataExtractor import DataExtractor
from binance.client import Client
from typing import Literal

class BinanceDataExtractor(DataExtractor):

	def __init__(self):
		with open('config.yml', 'r') as f:
			cfg = yaml.load(f, Loader = yaml.FullLoader)
			api_key = cfg['binance']['api_key']
			api_secret = cfg['binance']['api_secret']
		self.client = Client(api_key = api_key, api_secret = api_secret)
	
	def getClient(self):
		return self.client

	def extractSymbolBetweenDates(
			self, symbol: str, 
			startDate: datetime, 
			endDate: datetime, 
			interval: Literal[
				Client.KLINE_INTERVAL_1MINUTE,
				Client.KLINE_INTERVAL_1HOUR, 
				Client.KLINE_INTERVAL_1DAY
			]
		) -> pd.DataFrame:
		filename = f'data\\{symbol}_{startDate.strftime('%Y%m%d_%H%M%S')}_{endDate.strftime('%Y%m%d_%H%M%S')}_{interval}.csv'

		# Cache check
		if os.path.isfile(filename):
			return pd.read_csv(filename)
		
		if 'MINUTE' in interval:
			minutes = int(interval.split('MINUTE')[0])
			batch_size = timedelta(minutes=1000 * minutes)
		elif 'HOUR' in interval:
			hours = int(interval.split('HOUR')[0])
			batch_size = timedelta(hours=1000 * hours)
		elif 'DAY' in interval:
			days = int(interval.split('DAY')[0])
			batch_size = timedelta(days=1000 * days)
		else:
			batch_size = timedelta(days=1000)

		klines = []
		current_start = startDate

		while current_start < endDate:
			batch_end = min(
				current_start + batch_size,
				endDate
			)

			batch = self.client.get_historical_klines(
				symbol 		= symbol,
				interval 	= interval,
				start_str 	= current_start.strftime("%d %b %Y %H:%M:%S"),
				end_str 	= batch_end.strftime("%d %b %Y %H:%M:%S"),
				limit 		= 1000
			)
        
			if batch:
				klines.extend(batch)
				last_timestamp = batch[-1][0]
				current_start = datetime.fromtimestamp(last_timestamp/1000) + timedelta(minutes=1)
			else:
				break
		
		data = pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ])
		data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')

		data.set_index('timestamp', inplace=True)
		data.to_csv(filename)

		return data