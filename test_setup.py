import datetime
import pandas as pd
from seanode.api import get_surge_model_at_stations


df_out = get_surge_model_at_stations(
    'STOFS_2D_GLO',
    ['cwl_bias_corrected', 'u_vel', 'v_vel'],
    pd.Series(['8720218', '8720357', '8725114']),
    datetime.datetime(2024,12,1,12,0),
    None,
    'forecast',
    'points',
    'MLLW',
    'AWS'
)

print(df_out)