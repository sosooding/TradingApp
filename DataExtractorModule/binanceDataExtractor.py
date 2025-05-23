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

	def extractSymbolBetweenDates(self, symbol, startDate, endDate):
		filename = f'data\\{symbol}_{startDate}_{endDate}.csv'

		# Cache check
		if os.path.isfile(filename):
			return pd.read_csv(filename)

		klines = self.client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1MINUTE, startDate.strftime("%d %b %Y %H:%M:%S"), endDate.strftime("%d %b %Y %H:%M:%S"), 1000)
		data = pd.DataFrame(klines, columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ])
		data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')

		data.set_index('timestamp', inplace=True)
		data.to_csv(filename)

		return data