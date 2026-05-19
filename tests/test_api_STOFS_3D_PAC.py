import pathlib  
import sys
sys.path.append(str(pathlib.Path(__file__).parent.parent.absolute()))
import datetime
import pandas as pd
from seanode.api import get_surge_model_at_stations
import logging


logging.basicConfig(level=logging.INFO)


df_forecast = get_surge_model_at_stations(
    'STOFS_3D_PAC',
    ['cwl', 'salinity'],
    pd.Series(['1612340', '9447130', '9462620']),
    datetime.datetime(2026,2,1,12,0),
    None,
    'forecast',
    'points',
    'MLLW',
    'AWS'
)

df_nowcast = get_surge_model_at_stations(
    'STOFS_3D_PAC',
    ['cwl', 'u_vel'], 
    pd.Series(['1612340', '9447130', '9462620']), 
    datetime.datetime(2026,2,12,3,0),
    datetime.datetime(2026,2,13,21,0),
    'nowcast', 
    'points', 
    'MLLW', 
    'AWS'
)

assert df_forecast.shape == (1443, 5), "df_forecast should have shape (1443, 5)."
assert df_nowcast.shape == (1263, 5), "df_nowcast should have shape (1263, 5)."

print('---------- df_forecast ----------')
print(df_forecast)
print('---------- df_nowcast ----------')
print(df_nowcast)