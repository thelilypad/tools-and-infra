import numpy as np
import pandas as pd
import math


def integrated_variance(frame: pd.DataFrame, period: int = 500, window_size: int = 20):
    frame = frame.iloc[::window_size, :]
    frame['X'] = np.log(frame['prices']).diff(1)
    frame['X-1'] = frame['X'].shift(1)
    frame['X+1'] = frame['X'].shift(-1)
    frame = frame.iloc[2:-1]
    frame['variance'] = frame['X'] ** 2 + frame['X'] * frame['X-1'] + frame['X'] * frame['X+1']
    frame['var'] = frame['variance'].rolling(window=int(period / window_size)).sum()
    return frame[['date', 'var']]


def close_close_volatility(frame: pd.DataFrame, window: int = 30):
    if 'close' not in frame.columns:
        raise RuntimeError("Missing the following field from passed in dataframe: Close")
    frame = frame.dropna()
    num_obs = len(frame.index)
    log_return = (frame['close'] / frame['close'].shift(1)).apply(np.log)

    result = log_return.rolling(
        window=window,
        center=False
    ).std() * math.sqrt(num_obs)
    return result


def rogers_satchell_volatility(frame: pd.DataFrame, window: int = 30):
    REQUIRED_ROGERS_SATCHELL = ['open', 'high', 'low', 'close']
    if REQUIRED_ROGERS_SATCHELL not in frame.columns:
        raise RuntimeError("Missing one or more of the following fields from passed in dataframe: " + ",".join(
            REQUIRED_ROGERS_SATCHELL))
    frame = frame.dropna()
    num_obs = len(frame.index)
    log_ho = (frame['high'] / frame['open']).apply(np.log)
    log_lo = (frame['low'] / frame['open']).apply(np.log)
    log_co = (frame['close'] / frame['open']).apply(np.log)
    rs = log_ho * (log_ho - log_co) + log_lo * (log_lo - log_co)

    def f(v):
        return (num_obs * v.mean()) ** 0.5

    result = rs.rolling(
        window=window,
        center=False
    ).apply(func=f)
    return result


def garman_klass_volatility(frame: pd.DataFrame, window: int = 30):
    REQUIRED_GARMAN_KLASS = ['open', 'high', 'low', 'close']
    if REQUIRED_GARMAN_KLASS not in frame.columns:
        raise RuntimeError(
            "Missing one or more of the following fields from passed in dataframe: " + ",".join(REQUIRED_GARMAN_KLASS))
    frame = frame.dropna()
    num_obs = len(frame.index)
    log_hl = (frame['high'] / frame['low']).apply(np.log)
    log_co = (frame['close'] / frame['open']).apply(np.log)
    rs = 0.5 * log_hl ** 2 - (2 * math.log(2) - 1) * log_co ** 2

    def f(v):
        return (num_obs * v.mean()) ** 0.5

    result = rs.rolling(window=window, center=False).apply(func=f)
    return result


def parkinson_volatility(frame: pd.DataFrame, window: int = 30):
    REQUIRED_PARKINSON = ['high', 'low']
    if REQUIRED_PARKINSON not in frame.columns:
        raise RuntimeError(
            "Missing one or more of the following fields from passed in dataframe: " + ",".join(REQUIRED_PARKINSON))

    frame = frame.dropna()
    num_obs = len(frame.index)
    rs = (1.0 / (4.0 * math.log(2.0))) * ((frame['high'] / frame['low']).apply(np.log)) ** 2.0

    def f(v):
        return (num_obs * v.mean()) ** 0.5

    result = rs.rolling(
        window=window,
        center=False
    ).apply(func=f)
    return result


def variance_ratio():
    pass
