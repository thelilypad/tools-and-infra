from typing import List

import pandas as pd
from tools_and_infra.loaders.Loader import Loader, make_url
import requests

BASE_URL = "https://api.orats.io/datav2"


class ORATSLoader(Loader):
    def api_key_property(self) -> str:
        return 'ORATS_API_KEY'

    def service_name(self) -> str:
        return 'ORATS'

    def __init__(self):
        super().__init__()
        print(self.api_key.unwrap())
        self.create_url = lambda *res, **kwargs: make_url(BASE_URL, *res, **kwargs, token=self.api_key.unwrap())

    @Loader._raise_api_exception_if_missing_config
    def get_min_max_data_for_tickers(self, ticker: str = None) -> pd.DataFrame:
        url = None
        if ticker:
            url = self.create_url("tickers", ticker=ticker)
        else:
            url = self.create_url("tickers")
        req = requests.get(url)
        return pd.DataFrame(req.json()['data'])

    @Loader._raise_api_exception_if_missing_config
    def get_dividend_history(self, ticker: str) -> pd.DataFrame:
        url = self.create_url("hist/divs", ticker=ticker)
        return pd.DataFrame(requests.get(url).json()['data'])

    def get_options_for_date_and_tickers(self, date: pd.Timestamp, tickers: List[str]):
        url = self.create_url("hist/strikes", ticker=",".join(tickers), tradeDate=date.strftime('%Y-%m-%d'))
        return pd.DataFrame(requests.get(url).json()['data'])

if __name__ == '__main__':
    loader = ORATSLoader()
    print(loader.get_options_for_date_and_tickers(date=pd.to_datetime("2019-09-05"), tickers=['AAPL']))
