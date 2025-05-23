from abc import ABC, abstractmethod

class DataExtractor(ABC):

	def __init__(self):
		pass

	@abstractmethod
	def extractSymbolBetweenDates(self, symbol, startDate, endDate):
		pass