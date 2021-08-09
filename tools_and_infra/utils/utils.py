import pandas as pd

REQUIRED_OHLCV_COLUMNS = [
    'open',
    'high',
    'low',
    'close',
    'volume'
]


class UtilException(Exception):
    pass


def remove_non_ohlcv_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop(columns=[col for col in df.columns if col not in REQUIRED_OHLCV_COLUMNS])


def rename_to_ohlcv(df: pd.DataFrame, *, volume_col: str, open_col: str, high_col: str, low_col: str, close_col: str):
    return df.rename(
        columns={volume_col: 'volume', open_col: 'open', high_col: 'high', low_col: 'low', close_col: 'close'})


def resample_ohlcv_time_series(df: pd.DataFrame, interval: str, coerce_datetime_column: str) -> pd.DataFrame:
    if not set(REQUIRED_OHLCV_COLUMNS) <= set(df.columns):
        raise UtilException('Missing one or more required columns: ' + ','.join(REQUIRED_OHLCV_COLUMNS))
    # Convert all columns to float if they aren't already for aggregation/sampling
    for column in REQUIRED_OHLCV_COLUMNS:
        df[column] = df[column].apply(float)
    # If the user provides a column containing the datetime
    if coerce_datetime_column:
        df[coerce_datetime_column] = pd.to_datetime(df[coerce_datetime_column])
        df = df.set_index(coerce_datetime_column)
    return df.resample(interval).agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'})
