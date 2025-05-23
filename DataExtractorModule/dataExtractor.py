from abc import ABC, abstractmethod
from datetime import datetime

class DataExtractor(ABC):

	def __init__(self):
		pass

	@abstractmethod
	def extractSymbolBetweenDates(self, symbol, startDate: datetime, endDate: datetime, interval: str):
		pass