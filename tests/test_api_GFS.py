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
    'GFS',
    ['u10', 'v10', 'ps'],
    df_stations,
    datetime.datetime(2024,12,1,18,0),
    None,
    'forecast',
    'grid',
    None,
    'AWS'
)

print(df_forecast)

df_nowcast = get_surge_model_at_stations(
    'GFS',
    ['u10', 'v10', 'ps'],
    df_stations, 
    datetime.datetime(2024,5,12,3,0),
    datetime.datetime(2024,5,13,21,0),
    'nowcast', 
    'grid', 
    None, 
    'AWS'
)

print(df_nowcast)

assert df_forecast.shape[0] == 423, "df_forecast should have 423 rows."
assert df_nowcast.shape[0] == 129, "df_nowcast should have 129 rows."