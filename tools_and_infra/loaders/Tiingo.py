import math
import os
from datetime import date, timedelta
from tools_and_infra.loaders.Loader import Loader
import tiingo
import pandas
from tqdm import tqdm
from dateutil import parser
from tools_and_infra.utils import utils
from cache_decorator import Cache


class TiingoLoader(Loader):
    def __init__(self):
        super().__init__()
        self.client = self.api_key.map(lambda x: tiingo.TiingoClient({'api_key': x}))

    def api_key_property(self) -> str:
        return "TIINGO_API_KEY"

    def service_name(self) -> str:
        return "Tiingo"

    @Loader._raise_api_exception_if_missing_config
    def _fetch_crypto_intraday_data(self, crypto_asset: str, start_date: str, base_currency: str='USD') -> pandas.DataFrame:
        client: tiingo.TiingoClient = self.client
        full_str = crypto_asset.lower() + base_currency.lower()
        return pandas.DataFrame(client.get_crypto_price_history([full_str], baseCurrency=base_currency, startDate=start_date, resampleFreq='1min')[0]['priceData'])

    @Cache(cache_path="{cache_dir}/{crypto_asset}_{start_date}_complete_1min_intraday.csv")
    def _fetch_complete_intraday_crypto_data_with_cache(self, crypto_asset: str, start_date: pandas.Timestamp, end_date: pandas.Timestamp):
        return self._fetch_complete_intraday_crypto_data(crypto_asset, start_date, end_date)

    def _fetch_complete_intraday_crypto_data(self, crypto_asset: str, start_date: pandas.Timestamp, end_date: pandas.Timestamp):
        df = pandas.DataFrame()
        minutes_to_pull = (end_date - start_date).total_seconds() / 60
        requests_needed = math.ceil(abs(minutes_to_pull)/5000)
        current_start_time = start_date.strftime("%Y-%m-%dT%H:%M:%S")
        for _ in tqdm(range(requests_needed)):
            df2 = self._fetch_crypto_intraday_data(crypto_asset, current_start_time)
            current_start_time = pandas.to_datetime(df2.iloc[-1]['date']) + timedelta(minutes=1)
            df = df.append(df2)
        df = df.sort_values(by='date')
        df = df.reset_index(drop=True)
        return df

    def get_complete_intraday_crypto_data(self, crypto_asset: str, interval: str, start_date=pandas.Timestamp(parser.parse('2019-01-02')), end_date=pandas.Timestamp(date.today())):
        filename = "./cache/" + crypto_asset + "_" + start_date.strftime("%Y-%m-%d %H:%M:%S") + "_complete_1min_intraday.csv"
        start_time = start_date
        if os.path.isfile(filename):
            df = pandas.read_csv(filename, index_col=[0])
            start_time = pandas.to_datetime(df.iloc[-1]['date']) + timedelta(minutes=1)
        else:
            df = pandas.DataFrame()
        is_incremental_addition = start_time != start_date
        fetch = self._fetch_complete_intraday_crypto_data if is_incremental_addition else self._fetch_complete_intraday_crypto_data_with_cache
        complete_intraday_data = fetch(crypto_asset, start_date=start_time.replace(tzinfo=None), end_date=end_date)
        df = df.append(complete_intraday_data)
        df = df.sort_values(by='date')
        df = df.reset_index()
        del df['index']
        if is_incremental_addition:
            Cache.store(df, filename)
        return utils.resample_ohlcv_time_series(df, interval, coerce_datetime_column='date')
