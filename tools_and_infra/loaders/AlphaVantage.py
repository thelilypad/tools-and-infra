import logging
from time import sleep

from cache_decorator import Cache
from alpha_vantage.timeseries import TimeSeries
from tqdm import tqdm
from tools_and_infra.config import Config, ConfigException
from tools_and_infra.utils import utils
import pandas as pd

api_key = Config.get_property("ALPHA_VANTAGE_API_KEY")
enable_api_calls = len(api_key)

SLICES_INTRADAY_EXTENDED = ['year' + str(j + 1) + 'month' + str(i + 1) for j in range(2) for i in range(12)]
INTERVALS = [
    '1min', '5min', '15min', '30min', '60min'
]


def _check_if_api_key_present():
    if not enable_api_calls:
        raise ConfigException("Missing ALPHA_VANTAGE_API_KEY to use methods. Sign up for free at " +
                              "https://www.alphavantage.co/")


def get_intraday_equity_data(symbol: str, interval: str, slice: int = 0):
    _check_if_api_key_present()
    ts = TimeSeries(key=api_key[0], output_format='csv')
    csv_reader = ts.get_intraday_extended(symbol=symbol, interval=interval, slice=SLICES_INTRADAY_EXTENDED[slice])
    df = pd.DataFrame(csv_reader[0])
    # For some reason, the columns get put into the first row, so let's move the columns back and drop the first row
    df.columns = df.iloc[0]
    return df.iloc[1:]


@Cache(validity_duration='1d')
def _fetch_complete_intraday_equity_data(symbol: str, is_premium=False):
    if not is_premium:
        logging.warning(msg="Due to rate limiting of AlphaVantage, adding 15 second sleep between API calls.")
    df = pd.DataFrame()
    for i in tqdm(range(len(SLICES_INTRADAY_EXTENDED))):
        df2 = get_intraday_equity_data(symbol, '1min', i)
        df = df.append(df2)
        if not is_premium:
            logging.log(level=20, msg="Waiting for 15 seconds between calls due to rate limit.")
            sleep(15)
    df = df.sort_values(by='time')
    df.reset_index(inplace=True)
    del df['index']
    return df


def get_complete_intraday_equity_data(symbol: str, interval: str, is_premium=False):
    complete_intraday_data = _fetch_complete_intraday_equity_data(symbol, is_premium)
    return utils.resample_ohlcv_time_series(complete_intraday_data, interval, coerce_datetime_column='time')
