import logging
from time import sleep

from cache_decorator import Cache
from alpha_vantage.timeseries import TimeSeries
from tqdm import tqdm
from tools_and_infra.config import Config
from tools_and_infra.loaders.Loader import Loader
from tools_and_infra.utils import utils
import pandas as pd
import io
import requests

SLICES_INTRADAY_EXTENDED = ['year' + str(j + 1) + 'month' + str(i + 1) for j in range(2) for i in range(12)]
INTERVALS = [
    '1min', '5min', '15min', '30min', '60min'
]


class AlphaVantageLoader(Loader):
    def api_key_property(self) -> str:
        return "ALPHA_VANTAGE_API_KEY"

    def service_name(self) -> str:
        return "AlphaVantage"

    @Loader._raise_api_exception_if_missing_config
    def get_intraday_equity_data(self, symbol: str, interval: str, slice_i: int = 0):
        ts = TimeSeries(key=self.api_key.unwrap(), output_format='csv')
        csv_reader = ts.get_intraday_extended(symbol=symbol, interval=interval, slice=SLICES_INTRADAY_EXTENDED[slice_i])
        df = pd.DataFrame(csv_reader[0])
        # For some reason, the columns get put into the first row, so let's move the columns back and drop the first row
        df.columns = df.iloc[0]
        return df.iloc[1:]

    @Cache(cache_path="{cache_dir}/{symbol}_complete_1min_intraday.csv")
    def _fetch_complete_intraday_equity_data(self, symbol: str, is_premium=False):
        if not is_premium:
            logging.warning(msg="Due to rate limiting of AlphaVantage, adding 15 second sleep between API calls.")
        df = pd.DataFrame()
        for i in tqdm(range(len(SLICES_INTRADAY_EXTENDED))):
            df2 = self.get_intraday_equity_data(symbol, '1min', i)
            df = df.append(df2)
            if not is_premium:
                logging.log(level=20, msg="Waiting for 15 seconds between calls due to rate limit.")
                sleep(15)
        df = df.sort_values(by='time')
        df.reset_index(inplace=True)
        del df['index']
        return df

    def get_complete_intraday_equity_data(self, symbol: str, interval: str, is_premium=False):
        complete_intraday_data = self._fetch_complete_intraday_equity_data(symbol, is_premium)
        return utils.resample_ohlcv_time_series(complete_intraday_data, interval, coerce_datetime_column='time')
