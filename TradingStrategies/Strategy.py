from abc import ABC, abstractmethod
import pandas as pd

class TradingStrategy(ABC):

    @abstractmethod
    def trade(self, data: pd.DataFrame):
        pass