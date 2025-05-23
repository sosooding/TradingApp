from datetime import datetime, timedelta
import pandas as pd
import yaml
import os.path

from .dataExtractor import DataExtractor
from binance.client import Client

class BinanceDataExtractor(DataExtractor):

	def __init__(self):
		with open('config.yml', 'r') as f:
			cfg = yaml.load(f, Loader = yaml.FullLoader)
			api_key = cfg['binance']['api_key']
			api_secret = cfg['binance']['api_secret']
		self.client = Client(api_key = api_key, api_secret = api_secret)

	def extractSymbolBetweenDates(self, symbol: str, startDate: datetime, endDate: datetime):
		filename = f'data\\{symbol}_{startDate.strftime('%Y%m%d_%H%M%S')}_{endDate.strftime('%Y%m%d_%H%M%S')}.csv'

		# Cache check
		if os.path.isfile(filename):
			return pd.read_csv(filename)

		klines = []
		current_start = startDate

		while current_start < endDate:
			batch_end = min(
				current_start + timedelta(minutes=1000),
				endDate
			)

			batch = self.client.get_historical_klines(
				symbol 		= symbol,
				interval 	= Client.KLINE_INTERVAL_1MINUTE,
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