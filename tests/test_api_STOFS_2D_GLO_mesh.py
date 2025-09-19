import pathlib  
import sys
sys.path.append(str(pathlib.Path(__file__).parent.parent.absolute()))
import datetime
import pandas as pd
from seanode.api import get_surge_model_at_stations
import logging


logging.basicConfig(level=logging.INFO)


df_stations = pd.DataFrame(
    data={
        'station':['8720218', '8720357', '8447930'],
        'latitude':[30 + (23.9/60.0), 30 + (11.5/60.0), 41 + (31.4/60.0)],
        'longitude':[-81 - (25.7/60.0), -81 - (41.3/60.0), -70 - (40.3/60.0)]
    }
)

df_forecast = get_surge_model_at_stations(
    'STOFS_2D_GLO',
    ['cwl_raw'],
    df_stations,
    datetime.datetime(2024,12,1,12,0),
    None,
    'forecast',
    'mesh',
    'MLLW',
    'AWS'
)

assert df_forecast.shape == (543,3), "df_forecast should have shape (543, 3)."