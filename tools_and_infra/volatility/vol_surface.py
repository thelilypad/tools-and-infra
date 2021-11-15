import pandas as pd

from tools_and_infra.loaders.ORATS import ORATSLoader


class VolSurface:
    def __init__(self, ticker: str, date: pd.Timestamp):
        self.options = ORATSLoader().get_options_for_date_and_tickers(tickers=[ticker], date=date)
        print(self.options)

if __name__ == '__main__':
    VolSurface(ticker="AAPL", date=pd.to_datetime("2019-09-05"))