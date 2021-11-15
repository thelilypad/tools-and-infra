from tools_and_infra.loaders.Loader import Loader, make_url
from fredapi import Fred
import pandas as pd
import requests
import io

LIBOR_TENORS = {'ovn': 'USDONTD156N', '1m': 'USD1MTD156N', '3m': 'USD3MTD156N', '6m': 'USD6MTD156N', '12m': 'USD12MD156N'}
CONSTANT_TREASURY_TENORS = {'1m': 'DGS1MO', '3m': 'DGS3MO', '6m': 'DGS6MO', '12m': 'DGS1', '2y': 'DGS2', '3y': 'DGS3', '5y': 'DGS5', '7y': 'DGS7', '10y': 'DGS10', '20y': 'DGS20', '30y': 'DGS30'}

BASE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?bgcolor=%23e1e9f0&chart_type=line&drp=0&fo=open%20sans&graph_bgcolor=%23ffffff&height=450&mode=fred&recession_bars=on&txtcolor=%23444444&ts=12&tts=12&width=1168&nt=0&thu=0&trc=0&show_legend=yes&show_axis_titles=yes&show_tooltip=yes&id={series_id}&scale=left&cosd=1986-01-02&coed=2021-10-15&line_color=%234572a7&link_values=false&line_style=solid&mark_type=none&mw=3&lw=2&ost=-99999&oet=99999&mma=0&fml=a&fq=Daily&fam=avg&fgst=lin&fgsnd=2020-02-01&line_index=1&transformation=lin&vintage_date=2021-10-23&revision_date=2021-10-23&nd=1986-01-02"

def construct_libor_csv_from_fred(tenor: str):
    return BASE_URL.replace("{series_id}", LIBOR_TENORS[tenor])

class FREDLoader(Loader):
    def __init__(self):
        super().__init__()
        self.client = Fred(api_key=self.api_key.unwrap())

    def api_key_property(self) -> str:
        return 'FRED_API_KEY'

    def service_name(self) -> str:
        return 'FRED'

    def get_libor_usd_for_date(self, date: pd.Timestamp, tenor: str) -> float:
        req = requests.get(construct_libor_csv_from_fred(tenor))
        df = pd.read_csv(io.StringIO(req.text), sep=",")
        df.dropna(inplace=True)
        df = df.set_index('DATE')
        return df.iloc[df.index.get_loc(date.strftime('%Y-%m-%d'), method='backfill')][LIBOR_TENORS[tenor]]

    def get_constant_treasury_yield_for_date(self, date: pd.Timestamp, tenor: str) -> float:
        df = self.client.get_series(series_id=CONSTANT_TREASURY_TENORS[tenor])
        df.dropna(inplace=True)
        return df.iloc[df.index.get_loc(date.strftime('%Y-%m-%d'), method='backfill')]