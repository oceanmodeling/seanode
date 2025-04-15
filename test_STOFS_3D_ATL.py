import datetime
import pandas as pd
from seanode.api import get_surge_model_at_stations


df_forecast = get_surge_model_at_stations(
    'STOFS_3D_ATL',
    ['cwl', 'salinity'],
    pd.Series(['8720218', '8720357', '8447930']),
    datetime.datetime(2024,12,1,18,0),
    None,
    'forecast',
    'points',
    'MLLW',
    'AWS'
)

print(df_forecast)

df_nowcast = get_surge_model_at_stations(
    'STOFS_3D_ATL',
    ['cwl', 'u_vel'], 
    pd.Series(['8720218', '8720357', '8447930']), 
    datetime.datetime(2024,5,12,3,0),
    datetime.datetime(2024,5,15,21,0),
    'nowcast', 
    'points', 
    'MLLW', 
    'AWS'
)

print(df_nowcast)