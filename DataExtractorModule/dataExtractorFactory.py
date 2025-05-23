from .binanceDataExtractor import BinanceDataExtractor

class DataExtractorFactory:

	def getDataExtractor(self, data_extractor_type):

		if data_extractor_type == 'binance':
			return BinanceDataExtractor()